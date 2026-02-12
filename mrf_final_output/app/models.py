"""Pydantic models for API request/response."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ConvertRequest(BaseModel):
    """Request model for convert endpoint."""
    plan_name: Optional[str] = Field(None, description="Plan name to resolve file_name_core (required if file_name not provided)")
    file_name: Optional[str] = Field(None, description="Direct filename to use instead of resolving from plan_name (required if plan_name not provided)")
    cpt_code: Optional[str] = Field(None, description="Comma-separated list of CPT codes")
    zipcode: Optional[str] = Field(
        None, description="Zipcode to resolve nearby NPIs (optional)"
    )


class ConvertResponse(BaseModel):
    """Response model for convert endpoint."""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Job status: accepted, pending, processing, completed, failed")
    message: str = Field(..., description="Status message")
    status_url: str = Field(..., description="URL to check job status")


class JobResult(BaseModel):
    """Result information for completed job."""
    output_directory: str = Field(..., description="Path to output directory")
    total_rows: int = Field(..., description="Total number of rows processed")
    files_written: int = Field(..., description="Number of Parquet files written")


class JobStatusResponse(BaseModel):
    """Response model for job status endpoint."""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Job status: pending, processing, completed, failed")
    progress: int = Field(..., description="Progress percentage (0-100)")
    message: str = Field(..., description="Status message")
    result: Optional[JobResult] = Field(None, description="Result information (only when completed)")
    error: Optional[str] = Field(None, description="Error message (only when failed)")


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    spark_available: bool = Field(..., description="Whether Spark is available")


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error message")
    details: Optional[str] = Field(None, description="Additional error details")
