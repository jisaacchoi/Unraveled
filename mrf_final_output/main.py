"""FastAPI application for Parquet conversion service."""
import asyncio
import logging
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
from app.file_finder import find_source_file, find_all_file_parts
from app.processor import process_single_file
from app.utils import parse_filter_string, generate_run_id, ensure_directory

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
    source_file: str,
    npi_filter: Optional[str],
    billing_code_filter: Optional[str],
):
    """Background task to process a conversion job."""
    try:
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=10, message="Starting processing"
        )
        
        # Get configuration
        input_directory = Path(get_config_value(config, "paths.input_directory", "/tmp/input"))
        final_stage_directory = Path(get_config_value(config, "paths.final_stage_directory", "/tmp/final_stage"))
        temp_directory = Path(get_config_value(config, "paths.temp_directory", "/tmp/temp"))
        
        processing_config = get_config_value(config, "processing", {})
        explode_service_codes = processing_config.get("explode_service_codes", False)
        is_serverless = processing_config.get("is_serverless", True)
        enable_url_expansion = processing_config.get("enable_url_expansion", True)
        download_dir = processing_config.get("download_directory")
        skip_url_download = processing_config.get("skip_url_download", True)
        
        if download_dir:
            download_dir = Path(download_dir)
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=20, message="Finding source file"
        )
        
        # Find source file and schema
        file_path, schema_path = find_source_file(input_directory, source_file)
        all_file_parts = find_all_file_parts(input_directory, source_file)
        source_group = file_path.parent.name
        
        LOG.info(f"Found file in {source_group}: {file_path.name}")
        LOG.info(f"Found {len(all_file_parts)} file part(s)")
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=30, message="Preparing output directory"
        )
        
        # Create output directory
        run_id = generate_run_id()
        output_dir = final_stage_directory / run_id
        ensure_directory(output_dir)
        
        # Copy schema.json to output directory
        import shutil
        output_schema_path = output_dir / "schema.json"
        shutil.copy2(schema_path, output_schema_path)
        LOG.info(f"Copied schema.json to {output_schema_path}")
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=40, message="Processing file with Spark"
        )
        
        # Parse filters
        npi_list = parse_filter_string(npi_filter)
        billing_code_list = parse_filter_string(billing_code_filter)
        
        # Process file
        spark = get_spark_session()
        result = process_single_file(
            spark=spark,
            file_paths=all_file_parts,
            schema_path=schema_path,
            output_dir=output_dir,
            npi_filter=npi_list,
            billing_code_filter=billing_code_list,
            explode_service_codes=explode_service_codes,
            download_dir=download_dir,
            skip_url_download=skip_url_download,
            is_serverless=is_serverless,
            enable_url_expansion=enable_url_expansion,
        )
        
        job_manager.update_job_status(
            job_id, JobStatus.PROCESSING, progress=90, message="Creating manifest"
        )
        
        # Create manifest.json
        from datetime import datetime
        manifest = {
            "source_file": source_file,
            "source_group": source_group,
            "schema_path": str(schema_path),
            "filters": {
                "npi": npi_list if npi_list else [],
                "billing_code": billing_code_list if billing_code_list else [],
            },
            "processing_timestamp": datetime.utcnow().isoformat() + "Z",
            "total_rows": result["total_rows"],
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
                "total_rows": result["total_rows"],
                "files_written": result["files_written"],
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
    if not request.source_file:
        raise HTTPException(status_code=400, detail="source_file is required")
    
    # Create job
    job_id = job_manager.create_job()
    
    # Queue background task
    background_tasks.add_task(
        process_job,
        job_id=job_id,
        source_file=request.source_file,
        npi_filter=request.npi_filter,
        billing_code_filter=request.billing_code_filter,
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
