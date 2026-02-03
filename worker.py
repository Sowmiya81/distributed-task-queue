"""
Worker node for processing tasks from the distributed queue.
Implements consumer pattern with retry logic and error handling.
"""

import time
import logging
import signal
import sys
from typing import Callable, Optional, Dict
from queue_manager import QueueManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Worker:
    """Worker node that processes tasks from the queue."""
    
    def __init__(self, queue_manager: QueueManager, worker_id: Optional[str] = None):
        """
        Initialize worker.
        
        Args:
            queue_manager: QueueManager instance
            worker_id: Unique identifier for this worker
        """
        self.queue_manager = queue_manager
        self.worker_id = worker_id or f"worker-{time.time()}"
        self.running = False
        self.task_handlers: Dict[str, Callable] = {}
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down worker {self.worker_id}...")
        self.stop()
    
    def register_handler(self, task_type: str, handler: Callable):
        """
        Register a handler function for a specific task type.
        
        Args:
            task_type: Type of task to handle
            handler: Function that processes the task
                    Should accept (payload: dict) and return success (bool)
        """
        self.task_handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")
    
    def process_task(self, task: dict) -> bool:
        """
        Process a single task.
        
        Args:
            task: Task dictionary
            
        Returns:
            True if task completed successfully, False otherwise
        """
        task_id = task["id"]
        task_type = task["type"]
        payload = task["payload"]
        
        logger.info(f"Processing task {task_id} of type {task_type}")
        
        if task_type not in self.task_handlers:
            error_msg = f"No handler registered for task type: {task_type}"
            logger.error(error_msg)
            self.queue_manager.fail_task(task_id, error_msg, retry=False)
            return False
        
        try:
            handler = self.task_handlers[task_type]
            success = handler(payload)
            
            if success:
                self.queue_manager.complete_task(task_id)
                logger.info(f"Task {task_id} completed successfully")
                return True
            else:
                error_msg = "Task handler returned False"
                logger.warning(f"Task {task_id} failed: {error_msg}")
                self.queue_manager.fail_task(task_id, error_msg, retry=True)
                return False
                
        except Exception as e:
            error_msg = f"Exception while processing task: {str(e)}"
            logger.error(f"Task {task_id} failed with exception: {error_msg}", exc_info=True)
            self.queue_manager.fail_task(task_id, error_msg, retry=True)
            return False
    
    def start(self, poll_interval: float = 1.0):
        """
        Start the worker to continuously process tasks.
        
        Args:
            poll_interval: Seconds to wait between polling for tasks
        """
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        while self.running:
            try:
                task = self.queue_manager.dequeue_task(timeout=1)
                
                if task:
                    self.process_task(task)
                else:
                    time.sleep(poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in worker loop: {str(e)}", exc_info=True)
                time.sleep(poll_interval)
        
        logger.info(f"Worker {self.worker_id} stopped")
    
    def stop(self):
        """Stop the worker gracefully."""
        self.running = False


# Example task handlers
def example_email_handler(payload: dict) -> bool:
    """Handler for email tasks."""
    logger.info(f"Sending email: {payload}")
    time.sleep(0.5)
    return True


def example_data_processing_handler(payload: dict) -> bool:
    """Handler for data processing tasks."""
    logger.info(f"Processing data: {payload}")
    time.sleep(1.0)
    return True


if __name__ == "__main__":
    queue_mgr = QueueManager()
    
    if not queue_mgr.health_check():
        logger.error("Cannot connect to Redis. Please ensure Redis is running.")
        sys.exit(1)
    
    worker = Worker(queue_mgr)
    
    worker.register_handler("email", example_email_handler)
    worker.register_handler("data_processing", example_data_processing_handler)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()

