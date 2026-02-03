# Distributed Task Queue System

A scalable, fault-tolerant distributed task queue system built with Python and Redis. This system implements a producer-consumer pattern with retry logic and dead-letter queues to ensure reliable job processing with a target of 95% job completion rate.

## Features

- **Asynchronous Job Processing**: Submit tasks and process them asynchronously across multiple worker nodes
- **Producer-Consumer Pattern**: Decoupled task submission and processing
- **Retry Logic**: Automatic retry with exponential backoff for failed tasks
- **Dead-Letter Queue**: Failed tasks after max retries are moved to DLQ for manual inspection
- **REST API**: FastAPI-based API for task submission and monitoring
- **Horizontal Scaling**: Run multiple worker instances to handle concurrent job execution
- **Priority Queue**: Support for task prioritization
- **Queue Monitoring**: Real-time statistics and task status tracking

## Architecture

The system follows a producer-consumer architecture where the FastAPI server acts as the producer, Redis serves as the message broker, and worker nodes consume and process tasks.

## Prerequisites

- Python 3.8+
- Redis server (running on localhost:6379 by default)

## Installation

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On macOS/Linux
   venv\Scripts\activate     # On Windows
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start Redis server:
   ```bash
   brew services start redis  # macOS
   sudo systemctl start redis # Linux
   redis-server               # Direct execution
   ```

## Usage

Start the API server:
```bash
python api.py
```

Start worker nodes in separate terminals:
```bash
python worker.py
```

The API is available at `http://localhost:8000`. Interactive API documentation is available at `http://localhost:8000/docs`.

Submit tasks via the REST API:
```bash
curl -X POST "http://localhost:8000/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "task_type": "email",
    "payload": {"to": "user@example.com", "subject": "Hello"},
    "max_retries": 3,
    "priority": 10
  }'
```

Query task status and queue statistics:
```bash
curl "http://localhost:8000/tasks/{task_id}"
curl "http://localhost:8000/stats"
```

## Configuration

The queue manager can be configured with custom Redis connection parameters:

```python
queue_manager = QueueManager(
    redis_host="localhost",
    redis_port=6379,
    redis_db=0,
    queue_name="task_queue"
)
```

Workers can be configured with custom identifiers and polling intervals:

```python
worker = Worker(queue_manager, worker_id="custom-worker-1")
worker.start(poll_interval=1.0)
```

## Task Handlers

Register custom task handlers in `worker.py`:

```python
def my_custom_handler(payload: dict) -> bool:
    """Process custom task type."""
    # Your processing logic here
    # Return True on success, False on failure
    return True

worker.register_handler("my_task_type", my_custom_handler)
```

## Queue Structure

The system uses the following Redis data structures:

- **Pending Queue**: `task_queue:pending` (Sorted Set) - Tasks waiting to be processed
- **Processing Queue**: `task_queue:processing` (Sorted Set) - Tasks currently being processed
- **Retry Queue**: `task_queue:retry` (Sorted Set) - Tasks scheduled for retry
- **Dead-Letter Queue**: `task_queue:dead_letter` (List) - Failed tasks after max retries
- **Metadata**: `task_queue:metadata` (Hash) - Task metadata and status

## Retry Logic

Tasks are automatically retried on failure using exponential backoff. The delay between retries is calculated as `2^retry_count` seconds, with a maximum delay of 5 minutes. After exhausting the maximum number of retry attempts, tasks are moved to the dead-letter queue for manual inspection.

## Monitoring

The `/stats` endpoint provides real-time queue statistics including pending, processing, and retrying task counts, dead-letter queue size, and overall completion rate.

## Horizontal Scaling

The system supports horizontal scaling by running multiple worker instances across different machines or containers. All workers connect to the same Redis instance and automatically compete for tasks. No additional configuration is required.

## Error Handling

Task failures are automatically retried with exponential backoff. Tasks that exceed the maximum retry count are moved to the dead-letter queue. Redis connection issues are handled gracefully with error logging and automatic reconnection attempts. Handler exceptions are caught and logged, with tasks marked as failed for retry processing.

## Example Use Cases

- Email sending and notifications
- Data processing pipelines
- Image and video processing
- Report generation
- Web scraping and data collection
- Background job processing

## Project Structure

```
.
├── api.py              # FastAPI REST API
├── queue_manager.py    # Redis queue management
├── worker.py           # Worker node implementation
├── requirements.txt    # Python dependencies
└── README.md           # Documentation
```

