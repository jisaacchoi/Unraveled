"""FastAPI application for Parquet conversion service."""
import asyncio
import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession

from app.api_config import load_config, get_config_value
from app.models import (
    ConvertRequest,
    ConvertResponse,
    JobStatusResponse,
    HealthResponse,
    ErrorResponse,
)
from app.job_manager import job_manager, JobStatus
from app.file_finder import (
    collect_structure_directories_for_files,
    find_all_file_parts_in_paths,
    find_group_dirs_with_files,
)
from app.processor import process_single_file
from app.utils import parse_filter_string, generate_run_id, ensure_directory
from app.db_utils import get_file_name_core_by_plan, get_npis_by_zip
from app.plan_files import download_plan_files, download_files_by_name, split_files_if_needed
from app.schema_grouping import group_schema_directories_in_paths, move_schema_files_to_directory

# Load configuration
CONFIG_PATH = Path("config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).parent / "config.yaml"

try:
    config = load_config(CONFIG_PATH)
except Exception as e:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    LOG = logging.getLogger("app.main")
    LOG.warning(f"Could not load config from {CONFIG_PATH}: {e}. Using defaults.")
    config = {}


def configure_app_logging(config: dict) -> None:
    """Configure application logging from config."""
    logger_cfg = get_config_value(config, "logger", {}) or {}

    level_name = str(logger_cfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = logger_cfg.get(
        "format",
        "%(asctime)s %(levelname)s %(name)s:%(lineno)d - %(message)s",
    )
    datefmt = logger_cfg.get("date_format", "%Y-%m-%d %H:%M:%S")
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    if logger_cfg.get("console_enabled", True):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    file_cfg = logger_cfg.get("file_logging", {}) or {}
    if file_cfg.get("enabled", False):
        file_path = file_cfg.get("path")
        if not file_path:
            directory = Path(file_cfg.get("directory", "logs"))
            filename = file_cfg.get("filename", "api.log")
            file_path = str(directory / filename)

        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler = RotatingFileHandler(
            filename=log_path,
            mode=file_cfg.get("mode", "a"),
            maxBytes=int(file_cfg.get("max_bytes", 50 * 1024 * 1024)),
            backupCount=int(file_cfg.get("backup_count", 5)),
            encoding="utf-8",
        )
        handler.setLevel(level)
        handler.setFormatter(formatter)
        root.addHandler(handler)


configure_app_logging(config)
LOG = logging.getLogger("app.main")

# Optional: Configure PySpark Python executable for Windows environments
spark_python = get_config_value(config, "spark.python_executable", None)
if spark_python:
    os.environ["PYSPARK_PYTHON"] = spark_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = spark_python

# Initialize FastAPI app
app = FastAPI(
    title="Parquet Conversion API",
    description="REST API for converting JSON.gz files to Parquet format",
    version="1.0.0",
)

# Global Spark session (initialized on startup)
spark: Optional[SparkSession] = None


@app.middleware("http")
async def log_http_requests(request, call_next):
    """Log request/response metadata and processing time."""
    start = time.perf_counter()
    client_host = request.client.host if request.client else "unknown"

    LOG.info(
        "HTTP request started method=%s path=%s client=%s",
        request.method,
        request.url.path,
        client_host,
    )

    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        LOG.exception(
            "HTTP request failed method=%s path=%s client=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            client_host,
            duration_ms,
        )
        raise

    duration_ms = (time.perf_counter() - start) * 1000
    LOG.info(
        "HTTP request completed method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


def get_spark_session() -> SparkSession:
    """Get or create Spark session."""
    global spark
    if spark is None:
        app_name = get_config_value(config, "spark.app_name", "ParquetConversionAPI")
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        LOG.info("Spark session created")
    return spark


@app.on_event("startup")
async def startup_event():
    """Initialize Spark session on startup."""
    try:
        enable_parquet_conversion = get_config_value(
            config, "processing.enable_parquet_conversion", True
        )
        if enable_parquet_conversion:
            get_spark_session()
            LOG.info("API service started")
        else:
            LOG.info(
                "API service started with enable_parquet_conversion=false; "
                "Spark initialization skipped."
            )
    except Exception as e:
        LOG.error(f"Failed to initialize Spark: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global spark
    if spark is not None:
        spark.stop()
        LOG.info("Spark session stopped")


def process_job(
    job_id: str,
    plan_name: Optional[str],
    file_name: Optional[str],
    cpt_code: Optional[str],
    zipcode: Optional[str],
):
    """Background task to process a conversion job."""
    try:
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=10, message="Starting processing"
        )
        
        # Get configuration
        input_directory_cfg = get_config_value(config, "paths.input_directory", None)
        input_directory = Path(input_directory_cfg) if input_directory_cfg else None
        final_stage_directory = Path(get_config_value(config, "paths.final_stage_directory", "/tmp/final_stage"))
        temp_directory = Path(get_config_value(config, "paths.temp_directory", "/tmp/temp"))
        
        processing_config = get_config_value(config, "processing", {})
        explode_service_codes = processing_config.get("explode_service_codes", False)
        is_serverless = processing_config.get("is_serverless", True)
        enable_url_expansion = processing_config.get("enable_url_expansion", True)
        provider_download_dir = processing_config.get("download_directory")
        skip_url_download = processing_config.get("skip_url_download", True)
        output_format = processing_config.get("output_format", "parquet")
        enable_parquet_conversion = processing_config.get("enable_parquet_conversion", True)

        if provider_download_dir:
            provider_download_dir = Path(provider_download_dir)

        plan_download_dir = get_config_value(config, "paths.plan_download_directory", None)
        if plan_download_dir:
            plan_download_dir = Path(plan_download_dir)
        
        use_local_input = input_directory is not None and input_directory.exists()

        # Download files if not using local input and enable_plan_download is true
        downloaded_files = []
        if not use_local_input:
            enable_plan_download = get_config_value(config, "processing.enable_plan_download", True)
            if enable_plan_download:
                job_manager.update_job_status(
                    job_id, JobStatus.PROCESSING, progress=15, message="Downloading plan files"
                )
                if plan_name:
                    downloaded_files = download_plan_files(config, plan_name)
                elif file_name:
                    # Download files by file_name
                    downloaded_files = download_files_by_name(config, file_name)
                
                # Verify all downloads completed successfully before proceeding
                # Note: download_file() creates a .part file during download, then renames it when complete
                # We need to wait for .part files to disappear before proceeding
                if downloaded_files:
                    LOG.info(f"Downloaded {len(downloaded_files)} file(s). Verifying downloads are complete...")
                    import time
                    max_wait_time = 300  # Maximum 5 minutes to wait for downloads
                    check_interval = 0.5  # Check every 0.5 seconds
                    
                    for downloaded_file in downloaded_files:
                        part_file = downloaded_file.with_suffix(downloaded_file.suffix + '.part')
                        start_time = time.time()
                        wait_count = 0
                        
                        # Wait for .part file to be renamed (download complete)
                        while part_file.exists():
                            elapsed = time.time() - start_time
                            if elapsed > max_wait_time:
                                raise TimeoutError(
                                    f"Download timeout: {downloaded_file.name} still has .part file after {max_wait_time}s. "
                                    f"Download may have failed or is taking too long."
                                )
                            if wait_count % 10 == 0:  # Log every 5 seconds
                                LOG.info(f"Waiting for download to complete: {downloaded_file.name} (waited {elapsed:.1f}s)")
                            time.sleep(check_interval)
                            wait_count += 1
                        
                        # Verify final file exists and is not empty
                        if not downloaded_file.exists():
                            raise FileNotFoundError(f"Downloaded file does not exist: {downloaded_file}")
                        
                        file_size = downloaded_file.stat().st_size
                        if file_size == 0:
                            raise ValueError(f"Downloaded file is empty: {downloaded_file}")
                        
                        LOG.debug(f"Verified download: {downloaded_file.name} ({file_size:,} bytes)")
                    LOG.info("All downloads verified and complete")

        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=20, message="Finding source file"
        )
        
        # Resolve file_name_core: either from direct file_name or by resolving plan_name
        if file_name:
            # Use file_name directly (strip extension if present for matching)
            file_name_core = file_name
            if file_name_core.endswith('.json.gz'):
                file_name_core = file_name_core[:-8]  # Remove .json.gz
            elif file_name_core.endswith('.json'):
                file_name_core = file_name_core[:-5]  # Remove .json
            LOG.info(f"Using direct file_name: {file_name} (file_name_core: {file_name_core})")
        elif plan_name:
            # Resolve plan_name to file_name_core
            file_name_core = get_file_name_core_by_plan(config, plan_name)
            if not file_name_core:
                raise FileNotFoundError(f"Could not find file_name_core for plan_name: {plan_name}")
            LOG.info(f"Resolved plan_name '{plan_name}' to file_name_core: {file_name_core}")
        else:
            raise ValueError("Either plan_name or file_name must be provided")

        # Build search paths from structure.json locations and input directory
        structure_roots = get_config_value(config, "paths.structure_json_directories", []) or []
        if isinstance(structure_roots, str):
            structure_roots = [structure_roots]
        target_names = [p.name for p in downloaded_files]
        # If downloads are .json.gz, structure files may be .json with same base name
        for name in list(target_names):
            if name.endswith(".json.gz"):
                target_names.append(name[:-3])
        # Add schema name variants so schema files can be moved alongside downloads
        # Schema files can be named: {filename}.schema.json or {filename}_schema.json
        for name in list(target_names):
            if name.endswith(".json.gz"):
                # Try both naming conventions: .schema.json and _schema.json
                target_names.append(f"{name}.schema.json")  # file.json.gz.schema.json
                target_names.append(f"{name}_schema.json")   # file.json.gz_schema.json
                target_names.append(f"{name[:-3]}.schema.json")  # file.json.schema.json
                target_names.append(f"{name[:-3]}_schema.json")  # file.json_schema.json
            elif name.endswith(".json"):
                target_names.append(f"{name}.schema.json")  # file.json.schema.json
                target_names.append(f"{name}_schema.json")  # file.json_schema.json
        if not use_local_input and plan_download_dir:
            # Move matching schema files into plan_download_directory, then group there
            # Only try to move schema files if structure roots exist
            valid_structure_roots = [Path(p) for p in structure_roots if Path(p).exists() and Path(p).is_dir()]
            if valid_structure_roots:
                move_schema_files_to_directory(valid_structure_roots, plan_download_dir, target_names)
            else:
                LOG.warning("No valid structure directories found. Schema files must already exist in download directory or will be searched there.")
            group_schema_directories_in_paths([plan_download_dir])
        elif use_local_input and input_directory:
            # Local input flow: group only within input_directory
            group_schema_directories_in_paths([input_directory])

        search_paths = []
        if use_local_input and input_directory:
            search_paths.append(input_directory)
        else:
            if plan_download_dir:
                search_paths.append(plan_download_dir)

        # Find source file(s) and schema based on file_name_core
        all_file_parts = find_all_file_parts_in_paths(search_paths, file_name_core)
        group_dirs = find_group_dirs_with_files(search_paths, file_name_core)
        if not group_dirs:
            raise FileNotFoundError(
                f"Source file not found: {file_name_core} in any search path"
            )
        source_group = ",".join([d.name for d in group_dirs])
        
        LOG.info(f"Found file(s) in {source_group}: {file_name_core}")
        LOG.info(f"Found {len(all_file_parts)} file part(s)")
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=30, message="Preparing output directory"
        )
        
        # Create output directory
        run_id = generate_run_id()
        output_dir = final_stage_directory / run_id
        ensure_directory(output_dir)
        
        # Split large files if configured (downloads are already verified complete above)
        split_files_if_needed(config, all_file_parts, [Path(p) for p in structure_roots])
        all_file_parts = find_all_file_parts_in_paths(search_paths, file_name_core)
        group_dirs = find_group_dirs_with_files(search_paths, file_name_core)

        # Fast path: if parquet conversion is disabled, skip Spark processing entirely.
        if not enable_parquet_conversion:
            LOG.info(
                "enable_parquet_conversion=false; skipping Spark processing and "
                "finishing job after split/grouping."
            )

            used_schema_paths = []
            for group_dir in group_dirs:
                schema_path = group_dir / "main_schema.json"
                if not schema_path.exists():
                    candidates = list(group_dir.glob("*schema.json"))
                    if candidates:
                        schema_path = candidates[0]
                if schema_path.exists():
                    used_schema_paths.append(str(schema_path))

            billing_code_list = parse_filter_string(cpt_code)
            manifest = {
                "plan_name": plan_name,
                "file_name": file_name,
                "file_name_core": file_name_core,
                "source_group": source_group,
                "schema_path": used_schema_paths[0] if used_schema_paths else None,
                "schema_paths": used_schema_paths,
                "filters": {
                    "npi": [],
                    "cpt_code": billing_code_list if billing_code_list else [],
                    "zipcode": zipcode,
                },
                "processing_timestamp": datetime.utcnow().isoformat() + "Z",
                "total_rows": 0,
                "files_written": 0,
                "output_directory": str(output_dir),
                "url_expansion_status": "skipped_parquet_conversion_disabled",
                "skip_reason": "enable_parquet_conversion=false",
                "split_file_count": len(all_file_parts),
            }

            manifest_path = output_dir / "manifest.json"
            import json
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=2)

            LOG.info(f"Created manifest: {manifest_path}")

            job_manager.update_job_status(
                job_id,
                JobStatus.COMPLETED,
                progress=100,
                message="Processing skipped (enable_parquet_conversion=false)",
                result={
                    "output_directory": str(output_dir),
                    "total_rows": 0,
                    "files_written": 0,
                },
            )
            LOG.info(f"Job {job_id} completed successfully (Spark skipped)")
            return

        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=40, message="Processing file with Spark"
        )
        
        # Parse filters
        billing_code_list = parse_filter_string(cpt_code)

        # Resolve zipcode to NPI list (optional)
        npi_list = get_npis_by_zip(config, zipcode) if zipcode else []
        
        # Process file(s) by schema group; append results sequentially
        spark = get_spark_session()
        total_rows = 0
        files_written = 0
        used_schema_paths = []
        for idx, group_dir in enumerate(group_dirs, 1):
            group_files = [p for p in all_file_parts if p.parent == group_dir]
            if not group_files:
                continue

            schema_path = group_dir / "main_schema.json"
            if not schema_path.exists():
                # Fallback to any schema.json in group_dir
                candidates = list(group_dir.glob("*schema.json"))
                if not candidates:
                    raise FileNotFoundError(f"Schema file not found in {group_dir}")
                schema_path = candidates[0]
            used_schema_paths.append(str(schema_path))

            LOG.info(f"Processing group {idx}/{len(group_dirs)}: {group_dir.name} with {len(group_files)} file(s)")
            result = process_single_file(
                spark=spark,
                file_paths=group_files,
                schema_path=schema_path,
                output_dir=output_dir,
                npi_filter=npi_list,
                billing_code_filter=billing_code_list,
                explode_service_codes=explode_service_codes,
                download_dir=provider_download_dir,
                skip_url_download=skip_url_download,
                is_serverless=is_serverless,
                enable_url_expansion=enable_url_expansion,
                output_format=output_format,
                append_output=(idx > 1),
                enable_parquet_conversion=enable_parquet_conversion,
            )
            total_rows += result.get("total_rows", 0)
            files_written += result.get("files_written", 0)
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=90, message="Creating manifest"
        )
        
        # Create manifest.json
        manifest = {
            "plan_name": plan_name,
            "file_name": file_name,
            "file_name_core": file_name_core,
            "source_group": source_group,
            "schema_path": used_schema_paths[0] if used_schema_paths else None,
            "schema_paths": used_schema_paths,
            "filters": {
                "npi": npi_list if npi_list else [],
                "cpt_code": billing_code_list if billing_code_list else [],
                "zipcode": zipcode,
            },
            "processing_timestamp": datetime.utcnow().isoformat() + "Z",
            "total_rows": total_rows,
            "output_directory": str(output_dir),
            "url_expansion_status": result.get("url_expansion_status", "unknown"),
        }
        
        manifest_path = output_dir / "manifest.json"
        import json
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        
        LOG.info(f"Created manifest: {manifest_path}")
        
        job_manager.update_job_status(
            job_id,
            JobStatus.COMPLETED,
            progress=100,
            message="Processing complete",
            result={
                "output_directory": str(output_dir),
                "total_rows": total_rows,
                "files_written": files_written,
            },
        )
        
        LOG.info(f"Job {job_id} completed successfully")
        
    except FileNotFoundError as e:
        error_msg = str(e)
        LOG.error(f"Job {job_id} failed: {error_msg}")
        job_manager.update_job_status(
            job_id,
            JobStatus.FAILED,
            progress=0,
            message="Processing failed",
            error=error_msg,
        )
    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        LOG.error(f"Job {job_id} failed: {error_msg}", exc_info=True)
        job_manager.update_job_status(
            job_id,
            JobStatus.FAILED,
            progress=0,
            message="Processing failed",
            error=error_msg,
        )


@app.post("/api/v1/convert", response_model=ConvertResponse, status_code=202)
async def convert_file(request: ConvertRequest, background_tasks: BackgroundTasks):
    """
    Convert a JSON.gz file to Parquet format.
    
    This endpoint accepts either a plan_name (to resolve file_name_core) or a direct file_name,
    along with optional filters, then queues the job for asynchronous processing.
    """
    if not request.plan_name and not request.file_name:
        raise HTTPException(status_code=400, detail="Either plan_name or file_name is required")
    
    if request.plan_name and request.file_name:
        raise HTTPException(status_code=400, detail="Cannot specify both plan_name and file_name - use one or the other")
    
    # Create job
    job_id = job_manager.create_job()
    
    # Queue background task
    background_tasks.add_task(
        process_job,
        job_id=job_id,
        plan_name=request.plan_name,
        file_name=request.file_name,
        cpt_code=request.cpt_code,
        zipcode=request.zipcode,
    )
    
    return ConvertResponse(
        job_id=job_id,
        status="accepted",
        message="Job queued for processing",
        status_url=f"/api/v1/status/{job_id}",
    )


@app.get("/api/v1/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get the status of a conversion job."""
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status.value,
        progress=job.progress,
        message=job.message,
        result=job.result,
        error=job.error,
    )


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    enable_parquet_conversion = get_config_value(
        config, "processing.enable_parquet_conversion", True
    )
    if not enable_parquet_conversion:
        return HealthResponse(
            status="healthy",
            version="1.0.0",
            spark_available=False,
        )

    spark_available = False
    try:
        spark = get_spark_session()
        spark_available = spark is not None
    except Exception:
        pass
    
    return HealthResponse(
        status="healthy",
        version="1.0.0",
        spark_available=spark_available,
    )


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    LOG.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "details": str(exc)},
    )


if __name__ == "__main__":
    import uvicorn
    
    host = get_config_value(config, "api.host", "0.0.0.0")
    port = get_config_value(config, "api.port", 8000)
    
    uvicorn.run(app, host=host, port=port)
