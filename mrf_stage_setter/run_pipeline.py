#!/usr/bin/env python3
"""
Simplified pipeline runner for MRF processing steps.

Each step is designed to be run separately in different executors/jobs.
Usage:
    python run_pipeline.py --step 1
    python run_pipeline.py --step 2
    python run_pipeline.py --step 3
    python run_pipeline.py --step 4
"""
import subprocess
import sys
import logging
import argparse
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.shared.config import configure_logging, load_config
from src.shared.path_helper import ensure_directories_from_config

# ----------------------------
# Configuration
# ----------------------------
CONFIG_PATH = Path("config.yaml")

# Step definitions with descriptive names
STEP1 = [sys.executable, "commands/01_download.py", "--config", "config.yaml"]  # Download MRF files from index URL
STEP2 = [sys.executable, "commands/02_ingest.py", "--config", "config.yaml"]  # Ingest MRF JSON files into PostgreSQL (with rare key detection)
STEP3 = [sys.executable, "commands/03_analyze_schema_gen.py", "--config", "config.yaml"]  # Analyze JSON structures and generate schemas
STEP4 = [sys.executable, "commands/05_split.py", "--config", "config.yaml"]  # Split large JSON.gz files into smaller parts
# Step descriptions
STEP_DESCRIPTIONS = {
    1: "Download MRF files from index URL",
    2: "Ingest MRF JSON files into PostgreSQL",
    3: "Analyze JSON structures and generate schemas",
    4: "Split large JSON.gz files into smaller parts",
}

# ----------------------------
# Logger setup
# ----------------------------
LOG = logging.getLogger("pipeline_runner")

# ----------------------------
# Helpers
# ----------------------------
@dataclass
class RunResult:
    returncode: int
    stdout: str
    stderr: str

def run_cmd(cmd: List[str], timeout: Optional[int] = None) -> RunResult:
    """Run a command and return the result."""
    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return RunResult(p.returncode, p.stdout, p.stderr)

def run_step(name: str, description: str, cmd: List[str]) -> None:
    """Run a single pipeline step."""
    LOG.info("Step %s: Starting %s", name, description)
    LOG.info("Step %s: Running command: %s", name, ' '.join(cmd))
    res = run_cmd(cmd)

    if res.stdout.strip():
        LOG.info("Step %s: STDOUT:\n%s", name, res.stdout)
    if res.stderr.strip():
        # Filter out Spark warnings that are not actual errors
        stderr_lines = res.stderr.strip().split('\n')
        important_lines = [
            line for line in stderr_lines 
            if not any(ignore in line for ignore in [
                'WARNING: Using incubator modules',
                'Using Spark\'s default log4j profile',
                'Setting default log level',
                'To adjust logging level',
                'Unable to load native-hadoop library'
            ])
        ]
        if important_lines:
            LOG.warning("Step %s: STDERR:\n%s", name, '\n'.join(important_lines))
        elif stderr_lines:
            LOG.debug("Step %s: STDERR (filtered Spark warnings):\n%s", name, res.stderr)

    if res.returncode == 0:
        LOG.info("Step %s: Completed successfully", name)
        # Show log file location if available
        try:
            from src.shared.config import get_log_file_path, load_config
            config = load_config(CONFIG_PATH)
            log_file = get_log_file_path(config, {
                1: "download",
                2: "ingest", 
                3: "analyze",  # Combined analyze and schema gen
                4: "split",
                6: "migrate"
            }.get(int(name), "unknown"))
            if log_file and log_file.exists():
                LOG.info("Step %s: Detailed logs available at: %s", name, log_file)
        except Exception:  # noqa: BLE001
            pass  # Don't fail if we can't get log file path
        return

    LOG.error("Step %s: Failed with exit code %d", name, res.returncode)
    if res.stdout.strip():
        LOG.error("Step %s: Last STDOUT output:\n%s", name, res.stdout[-2000:])  # Last 2000 chars
    raise RuntimeError(f"Step {name} ({description}) failed with exit code {res.returncode}")

def run_step_with_polling(
    name: str, 
    description: str, 
    cmd: List[str], 
    wait_minutes: int, 
    num_additional_attempts: int | str
) -> None:
    """
    Run a pipeline step with polling: after completion (success or failure), wait and run again.
    
    For polling steps, failures are expected and will be retried. The step will only raise
    an exception if it fails on the final attempt (for finite polling) or if interrupted.
    
    Args:
        name: Step name/number
        description: Step description
        cmd: Command to run
        wait_minutes: Minutes to wait between attempts
        num_additional_attempts: Number of additional attempts after the first run,
                                or "inf" for infinite polling until interrupted
    """
    wait_seconds = wait_minutes * 60
    is_infinite = (num_additional_attempts == "inf" or str(num_additional_attempts).lower() == "inf")
    
    # Helper function to run step and catch failures for polling
    def run_step_with_retry(attempt_label: str) -> bool:
        """
        Run step and return True if successful, False if failed.
        For polling, failures are expected and will be retried.
        """
        try:
            run_step(name, description, cmd)
            return True
        except RuntimeError as exc:
            # For polling steps, failures are expected (e.g., size limit exceeded)
            # Log the failure but don't raise - we'll retry
            LOG.warning("Step %s: %s failed (will retry): %s", name, attempt_label, exc)
            return False
    
    # First run
    LOG.info("Step %s: Initial run - %s", name, description)
    first_run_success = run_step_with_retry("Initial run")
    
    if is_infinite:
        # Infinite polling mode
        LOG.info("Step %s: Running in infinite polling mode (will continue until interrupted)", name)
        attempt_num = 1
        try:
            while True:
                LOG.info(
                    "Step %s: Waiting %d minutes before attempt %d (infinite mode - press Ctrl+C to stop)",
                    name, wait_minutes, attempt_num
                )
                time.sleep(wait_seconds)
                
                LOG.info("Step %s: Polling attempt %d (infinite mode) - %s", name, attempt_num, description)
                run_step_with_retry(f"Polling attempt {attempt_num}")
                attempt_num += 1
        except KeyboardInterrupt:
            LOG.info("Step %s: Interrupted by user after %d polling attempt(s)", name, attempt_num - 1)
            raise
    else:
        # Finite polling attempts
        # Additional polling attempts
        last_failure = None
        for attempt_num in range(1, num_additional_attempts + 1):
            LOG.info(
                "Step %s: Waiting %d minutes before attempt %d/%d",
                name, wait_minutes, attempt_num, num_additional_attempts
            )
            time.sleep(wait_seconds)
            
            LOG.info("Step %s: Polling attempt %d/%d - %s", name, attempt_num, num_additional_attempts, description)
            success = run_step_with_retry(f"Polling attempt {attempt_num}/{num_additional_attempts}")
            if not success:
                # Store the last failure to re-raise if this is the final attempt
                last_failure = RuntimeError(f"Step {name} ({description}) failed on polling attempt {attempt_num}/{num_additional_attempts}")
        
        # If all attempts failed, raise the last failure
        if not first_run_success and last_failure:
            LOG.error("Step %s: All polling attempts failed. Raising last failure.", name)
            raise last_failure
        
        LOG.info("Step %s: Completed all attempts (1 initial + %d additional)", name, num_additional_attempts)

# ----------------------------
# Main
# ----------------------------
def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run MRF pipeline steps. Each step is designed to be run separately.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --step 1     # Run step 1 only
  python run_pipeline.py --step 2     # Run step 2 only
  python run_pipeline.py --step 3     # Run step 3 only (analyze and generate schemas)
  python run_pipeline.py --step 4     # Run step 4 only (split files)
  python run_pipeline.py --step 1 2   # Run steps 1 and 2 sequentially
  
Note: Step 6 (JSON.gz to Parquet) is Databricks-only - run commands/05_jsongz_to_parquet_databricks.py directly in Databricks
        """
    )
    parser.add_argument(
        "--step",
        type=int,
        nargs="+",
        choices=[1, 2, 3, 4, 6],
        required=True,
        help="Step(s) to run (1-4, 6). Example: --step 1 or --step 1 2 3"
    )
    return parser.parse_args()

def main():
    """Main entry point."""
    # Parse command-line arguments
    args = parse_args()
    steps_to_run: Set[int] = set(args.step)
    
    # Load config and configure logging
    if not CONFIG_PATH.exists():
        print(f"ERROR: Config file not found: {CONFIG_PATH}", flush=True)
        sys.exit(1)
    
    config = load_config(CONFIG_PATH)
    configure_logging(config)
    
    LOG.info("Starting pipeline runner")
    LOG.info("Configuration loaded from: %s", CONFIG_PATH)
    LOG.info("Steps to run: %s", sorted(steps_to_run))
    
    # Create all necessary directories from config
    LOG.info("Ensuring all required directories exist...")
    ensure_directories_from_config(config)
    
    # Load polling configuration for steps 2, 3, and 4
    pipeline_runner_cfg = config.get("pipeline_runner", {})
    polling_wait_minutes = pipeline_runner_cfg.get("polling_wait_minutes", 20)
    polling_num_additional_attempts_raw = pipeline_runner_cfg.get("polling_num_additional_attempts", 10)
    # Support "inf" string for infinite polling
    if isinstance(polling_num_additional_attempts_raw, str) and polling_num_additional_attempts_raw.lower() == "inf":
        polling_num_additional_attempts = "inf"
    else:
        polling_num_additional_attempts = int(polling_num_additional_attempts_raw)
    
    # Map step numbers to commands
    step_commands = {
        1: STEP1,
        2: STEP2,
        3: STEP3,
        4: STEP4,
    }
    
    # Run selected steps sequentially
    for step_num in sorted(steps_to_run):
        if step_num == 6:
            LOG.warning("Step 6 (JSON.gz to Parquet) is Databricks-only and not run via run_pipeline.py")
            LOG.warning("To run step 6, use commands/05_jsongz_to_parquet_databricks.py directly in Databricks")
            continue
        
        if step_num not in step_commands:
            LOG.warning("Step %d is not implemented", step_num)
            continue
        
        description = STEP_DESCRIPTIONS.get(step_num, f"Step {step_num}")
        cmd = step_commands[step_num]
        
        # Steps 1, 2, 3, and 4 use polling (run, wait, run again)
        if step_num in (1, 2, 3, 4):
            if polling_num_additional_attempts == "inf":
                LOG.info(
                    "Step %s: Using infinite polling mode (wait %d minutes between attempts, runs until interrupted)",
                    step_num, polling_wait_minutes
                )
            else:
                LOG.info(
                    "Step %s: Using polling mode (wait %d minutes, %d additional attempts)",
                    step_num, polling_wait_minutes, polling_num_additional_attempts
                )
            run_step_with_polling(
                str(step_num), 
                description, 
                cmd, 
                polling_wait_minutes, 
                polling_num_additional_attempts
            )
        else:
            # Other steps run once
            run_step(str(step_num), description, cmd)
    
    LOG.info("Pipeline completed successfully. Selected steps finished.")

if __name__ == "__main__":
    main()
