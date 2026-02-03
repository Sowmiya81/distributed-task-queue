"""
Redis-based queue manager for distributed task queue system.
Handles task enqueueing, dequeueing, retry logic, and dead-letter queue management.
"""

import json
import time
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
import redis
from redis.exceptions import RedisError


class QueueManager:
    """Manages task queues using Redis with retry and dead-letter queue support."""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, 
                 redis_db: int = 0, queue_name: str = "task_queue"):
        """
        Initialize queue manager.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            queue_name: Base name for the task queue
        """
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True
        )
        self.queue_name = queue_name
        self.pending_queue = f"{queue_name}:pending"
        self.processing_queue = f"{queue_name}:processing"
        self.dead_letter_queue = f"{queue_name}:dead_letter"
        self.retry_queue = f"{queue_name}:retry"
        self.metadata_key = f"{queue_name}:metadata"
        
    def _generate_task_id(self) -> str:
        """Generate unique task ID."""
        return str(uuid.uuid4())
    
    def enqueue_task(self, task_type: str, payload: Dict[str, Any], 
                    max_retries: int = 3, priority: int = 0) -> str:
        """
        Enqueue a new task.
        
        Args:
            task_type: Type of task (e.g., 'email', 'data_processing')
            payload: Task data/payload
            max_retries: Maximum number of retry attempts
            priority: Task priority (higher = more priority)
            
        Returns:
            Task ID
        """
        task_id = self._generate_task_id()
        task = {
            "id": task_id,
            "type": task_type,
            "payload": payload,
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "max_retries": max_retries,
            "retry_count": 0,
            "priority": priority
        }
        
        try:
            self.redis_client.hset(
                self.metadata_key,
                task_id,
                json.dumps(task)
            )
            
            score = float(priority) + time.time() / 1000000
            self.redis_client.zadd(self.pending_queue, {task_id: score})
            
            return task_id
        except RedisError as e:
            raise Exception(f"Failed to enqueue task: {str(e)}")
    
    def dequeue_task(self, timeout: int = 1) -> Optional[Dict[str, Any]]:
        """
        Dequeue a task from the pending queue.
        
        Args:
            timeout: Blocking timeout in seconds
            
        Returns:
            Task dictionary or None if no task available
        """
        try:
            result = self.redis_client.zrevrange(
                self.pending_queue, 0, 0, withscores=True
            )
            
            if not result:
                result = self.redis_client.zrevrange(
                    self.retry_queue, 0, 0, withscores=True
                )
            
            if result:
                task_id = result[0][0]
                
                self.redis_client.zrem(self.pending_queue, task_id)
                self.redis_client.zrem(self.retry_queue, task_id)
                
                self.redis_client.zadd(
                    self.processing_queue,
                    {task_id: time.time()}
                )
                
                task_json = self.redis_client.hget(self.metadata_key, task_id)
                if task_json:
                    task = json.loads(task_json)
                    task["status"] = "processing"
                    task["started_at"] = datetime.utcnow().isoformat()
                    
                    self.redis_client.hset(
                        self.metadata_key,
                        task_id,
                        json.dumps(task)
                    )
                    
                    return task
            
            return None
        except RedisError as e:
            raise Exception(f"Failed to dequeue task: {str(e)}")
    
    def complete_task(self, task_id: str) -> bool:
        """
        Mark a task as completed.
        
        Args:
            task_id: Task ID to complete
            
        Returns:
            True if successful
        """
        try:
            self.redis_client.zrem(self.processing_queue, task_id)
            
            task_json = self.redis_client.hget(self.metadata_key, task_id)
            if task_json:
                task = json.loads(task_json)
                task["status"] = "completed"
                task["completed_at"] = datetime.utcnow().isoformat()
                
                self.redis_client.hset(
                    self.metadata_key,
                    task_id,
                    json.dumps(task)
                )
                return True
            return False
        except RedisError as e:
            raise Exception(f"Failed to complete task: {str(e)}")
    
    def fail_task(self, task_id: str, error: str, retry: bool = True) -> bool:
        """
        Handle task failure with retry logic.
        
        Args:
            task_id: Task ID that failed
            error: Error message
            retry: Whether to retry the task
            
        Returns:
            True if successful
        """
        try:
            self.redis_client.zrem(self.processing_queue, task_id)
            
            task_json = self.redis_client.hget(self.metadata_key, task_id)
            if not task_json:
                return False
            
            task = json.loads(task_json)
            task["retry_count"] = task.get("retry_count", 0) + 1
            task["last_error"] = error
            task["last_failed_at"] = datetime.utcnow().isoformat()
            
            max_retries = task.get("max_retries", 3)
            
            if retry and task["retry_count"] <= max_retries:
                backoff_delay = min(2 ** task["retry_count"], 300)
                retry_time = time.time() + backoff_delay
                
                self.redis_client.zadd(
                    self.retry_queue,
                    {task_id: retry_time}
                )
                
                task["status"] = "retrying"
            else:
                self.redis_client.lpush(self.dead_letter_queue, task_id)
                task["status"] = "failed"
                task["moved_to_dlq_at"] = datetime.utcnow().isoformat()
            
            self.redis_client.hset(
                self.metadata_key,
                task_id,
                json.dumps(task)
            )
            
            return True
        except RedisError as e:
            raise Exception(f"Failed to handle task failure: {str(e)}")
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a specific task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task dictionary or None if not found
        """
        try:
            task_json = self.redis_client.hget(self.metadata_key, task_id)
            if task_json:
                return json.loads(task_json)
            return None
        except RedisError as e:
            raise Exception(f"Failed to get task status: {str(e)}")
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the queue.
        
        Returns:
            Dictionary with queue statistics
        """
        try:
            pending_count = self.redis_client.zcard(self.pending_queue)
            processing_count = self.redis_client.zcard(self.processing_queue)
            retry_count = self.redis_client.zcard(self.retry_queue)
            dlq_count = self.redis_client.llen(self.dead_letter_queue)
            
            all_tasks = self.redis_client.hgetall(self.metadata_key)
            total_tasks = len(all_tasks)
            completed_tasks = sum(
                1 for task_json in all_tasks.values()
                if json.loads(task_json).get("status") == "completed"
            )
            
            completion_rate = (
                (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
            )
            
            return {
                "pending": pending_count,
                "processing": processing_count,
                "retrying": retry_count,
                "dead_letter": dlq_count,
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "completion_rate": round(completion_rate, 2)
            }
        except RedisError as e:
            raise Exception(f"Failed to get queue stats: {str(e)}")
    
    def health_check(self) -> bool:
        """Check if Redis connection is healthy."""
        try:
            return self.redis_client.ping()
        except RedisError:
            return False

