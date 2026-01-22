from typing import Any, List, Optional, Union
from enum import IntEnum

class JobStatus(IntEnum):
    """Represents the status of a job in the queue (integer-backed)."""
    Pending = 0
    InProgress = 1
    Done = 2
    Failed = 3
    def __repr__(self) -> str: ...

class LeaseKey:
    """A token required to perform operations on a leased job."""
    @property
    def job_id(self) -> int: ...
    @property
    def lease_id(self) -> str: ...

class Job:
    """A job record from the queue."""
    @property
    def id(self) -> int: ...
    @property
    def idempotency_key(self) -> str: ...
    @property
    def status(self) -> JobStatus: ...
    @property
    def payload(self) -> Any: ...
    @property
    def visible_at(self) -> str: ... # Note: Returns RFC3339 string from Rust
    @property
    def attempt_count(self) -> int: ...
    @property
    def lease_timeout_seconds(self) -> int: ...

    def lease_key(self) -> Optional[LeaseKey]:
        """Returns the LeaseKey required to acknowledge or modify this job, if it is currently leased."""
        ...

class PgQueue:
    """A PostgreSQL-backed queue client."""
    @staticmethod
    async def connect(pg_uri: str) -> 'PgQueue':
        """Connects to a PostgreSQL database using the provided URI."""
        ...

    async def enqueue(
        self,
        idempotency_key: str,
        payload: Any,
        lease_timeout_seconds: int
    ) -> Optional[Job]:
        """Enqueues a new job into the queue."""
        ...

    async def dequeue(
        self,
        worker_id: str,
        batch_size: int,
        max_attempts: int
    ) -> List[Job]:
        """Dequeues up to `batch_size` jobs, atomically leasing them to `worker_id`."""
        ...

    async def ack(self, lease: LeaseKey) -> Optional[Job]:
        """Acknowledges a job as successfully completed."""
        ...

    async def ack_batch(self, leases: List[LeaseKey]) -> List[Job]:
        """Acknowledges a batch of jobs as successfully completed."""
        ...

    async def nack(
        self,
        lease: LeaseKey,
        max_attempts: int,
        delay_seconds: Optional[float] = None
    ) -> Optional[Job]:
        """Negatively acknowledges a job, returning it to the pending state for retry."""
        ...

    async def touch(self, lease: LeaseKey, lease_seconds: int) -> Optional[Job]:
        """Extends the lease of an InProgress job."""
        ...
