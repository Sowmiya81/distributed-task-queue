"""
FastAPI REST API for task submission and queue monitoring.
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
import logging
from queue_manager import QueueManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Distributed Task Queue API",
    description="REST API for submitting tasks and monitoring queue status",
    version="1.0.0"
)

queue_manager = QueueManager()


class TaskRequest(BaseModel):
    """Request model for task submission."""
    task_type: str = Field(..., description="Type of task to execute")
    payload: Dict[str, Any] = Field(..., description="Task payload/data")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")
    priority: int = Field(default=0, ge=0, le=100, description="Task priority (0-100)")


class TaskResponse(BaseModel):
    """Response model for task submission."""
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    """Response model for task status."""
    task_id: str
    status: str
    task_type: str
    created_at: str
    retry_count: int
    max_retries: int
    last_error: Optional[str] = None
    completed_at: Optional[str] = None


class QueueStatsResponse(BaseModel):
    """Response model for queue statistics."""
    pending: int
    processing: int
    retrying: int
    dead_letter: int
    total_tasks: int
    completed_tasks: int
    completion_rate: float


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Distributed Task Queue API",
        "version": "1.0.0",
        "endpoints": {
            "submit_task": "POST /tasks",
            "get_task_status": "GET /tasks/{task_id}",
            "get_queue_stats": "GET /stats",
            "health": "GET /health"
        }
    }


@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def submit_task(task_request: TaskRequest):
    """
    Submit a new task to the queue.
    
    Args:
        task_request: Task submission request
        
    Returns:
        Task ID and status
    """
    try:
        task_id = queue_manager.enqueue_task(
            task_type=task_request.task_type,
            payload=task_request.payload,
            max_retries=task_request.max_retries,
            priority=task_request.priority
        )
        
        logger.info(f"Task submitted: {task_id} (type: {task_request.task_type})")
        
        return TaskResponse(
            task_id=task_id,
            status="pending",
            message="Task submitted successfully"
        )
    except Exception as e:
        logger.error(f"Error submitting task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to submit task: {str(e)}")


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """
    Get the status of a specific task.
    
    Args:
        task_id: Task ID
        
    Returns:
        Task status information
    """
    try:
        task = queue_manager.get_task_status(task_id)
        
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        return TaskStatusResponse(
            task_id=task["id"],
            status=task["status"],
            task_type=task["type"],
            created_at=task["created_at"],
            retry_count=task.get("retry_count", 0),
            max_retries=task.get("max_retries", 3),
            last_error=task.get("last_error"),
            completed_at=task.get("completed_at")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@app.get("/stats", response_model=QueueStatsResponse)
async def get_queue_stats():
    """
    Get queue statistics including completion rate.
    
    Returns:
        Queue statistics
    """
    try:
        stats = queue_manager.get_queue_stats()
        
        return QueueStatsResponse(**stats)
    except Exception as e:
        logger.error(f"Error getting queue stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get queue stats: {str(e)}")


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Health status
    """
    try:
        is_healthy = queue_manager.health_check()
        
        if is_healthy:
            return {
                "status": "healthy",
                "redis": "connected"
            }
        else:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "unhealthy",
                    "redis": "disconnected"
                }
            )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

