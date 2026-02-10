"""FastAPI application for Parquet conversion service."""
import asyncio
import logging
import os
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
from app.plan_files import download_plan_files, split_files_if_needed
from app.schema_grouping import group_schema_directories_in_paths, move_schema_files_to_directory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
LOG = logging.getLogger("app.main")

# Load configuration
CONFIG_PATH = Path("config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).parent / "config.yaml"

try:
    config = load_config(CONFIG_PATH)
except Exception as e:
    LOG.warning(f"Could not load config from {CONFIG_PATH}: {e}. Using defaults.")
    config = {}

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
        get_spark_session()
        LOG.info("API service started")
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
    plan_name: str,
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

        if provider_download_dir:
            provider_download_dir = Path(provider_download_dir)

        plan_download_dir = get_config_value(config, "paths.plan_download_directory", None)
        if plan_download_dir:
            plan_download_dir = Path(plan_download_dir)
        
        use_local_input = input_directory is not None and input_directory.exists()

        if not use_local_input:
            job_manager.update_job_status(
                job_id, JobStatus.PROCESSING, progress=15, message="Downloading plan files"
            )
            downloaded_files = download_plan_files(config, plan_name)
        else:
            downloaded_files = []

        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=20, message="Finding source file"
        )
        
        # Resolve plan_name to file_name_core
        file_name_core = get_file_name_core_by_plan(config, plan_name)
        if not file_name_core:
            raise FileNotFoundError(f"Could not find file_name_core for plan_name: {plan_name}")

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
        for name in list(target_names):
            if name.endswith(".json.gz"):
                target_names.append(f"{name}_schema.json")
                target_names.append(f"{name[:-3]}_schema.json")
            elif name.endswith(".json"):
                target_names.append(f"{name}_schema.json")
        if not use_local_input and plan_download_dir:
            # Move matching schema files into plan_download_directory, then group there
            move_schema_files_to_directory([Path(p) for p in structure_roots], plan_download_dir, target_names)
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
        
        # Split large files if configured
        split_files_if_needed(config, all_file_parts, [Path(p) for p in structure_roots])
        all_file_parts = find_all_file_parts_in_paths(search_paths, file_name_core)
        group_dirs = find_group_dirs_with_files(search_paths, file_name_core)

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
            )
            total_rows += result.get("total_rows", 0)
            files_written += result.get("files_written", 0)
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=90, message="Creating manifest"
        )
        
        # Create manifest.json
        from datetime import datetime
        manifest = {
            "plan_name": plan_name,
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
    
    This endpoint accepts a source file name and optional filters, then queues
    the job for asynchronous processing.
    """
    if not request.plan_name:
        raise HTTPException(status_code=400, detail="plan_name is required")
    
    # Create job
    job_id = job_manager.create_job()
    
    # Queue background task
    background_tasks.add_task(
        process_job,
        job_id=job_id,
        plan_name=request.plan_name,
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
