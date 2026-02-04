"""Job management for async processing and status tracking."""
import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

LOG = logging.getLogger("app.job_manager")


class JobStatus(str, Enum):
    """Job status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Job:
    """Job representation."""
    
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.status = JobStatus.PENDING
        self.progress = 0
        self.message = "Job queued"
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.result: Optional[Dict] = None
        self.error: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert job to dictionary for API response."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "result": self.result,
            "error": self.error,
        }


class JobManager:
    """Simple in-memory job manager."""
    
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
    
    def create_job(self) -> str:
        """Create a new job and return its ID."""
        job_id = f"job_{uuid.uuid4().hex[:8]}"
        job = Job(job_id)
        self._jobs[job_id] = job
        LOG.info(f"Created job: {job_id}")
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return self._jobs.get(job_id)
    
    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        progress: int = None,
        message: str = None,
        result: Dict = None,
        error: str = None,
    ):
        """Update job status."""
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        job.status = status
        if progress is not None:
            job.progress = progress
        if message is not None:
            job.message = message
        if result is not None:
            job.result = result
        if error is not None:
            job.error = error
        
        if status == JobStatus.PROCESSING and not job.started_at:
            job.started_at = datetime.utcnow()
        elif status in (JobStatus.COMPLETED, JobStatus.FAILED):
            job.completed_at = datetime.utcnow()
        
        LOG.info(f"Updated job {job_id}: status={status.value}, progress={progress}, message={message}")
    
    def list_jobs(self) -> Dict[str, Job]:
        """List all jobs."""
        return self._jobs.copy()


# Global job manager instance
job_manager = JobManager()
