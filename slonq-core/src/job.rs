use chrono::{DateTime, Utc};
use postgres_types::{FromSql, ToSql};
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

/// Represents the status of a job in the queue.
///
/// Mirrors the PostgreSQL enum `job_status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToSql, FromSql)]
#[postgres(name = "job_status")]
pub enum JobStatus {
    /// The job is waiting to be processed.
    #[postgres(name = "pending")]
    Pending,
    /// The job is currently being processed by a worker.
    #[postgres(name = "in_progress")]
    InProgress,
    /// The job has been successfully completed.
    #[postgres(name = "done")]
    Done,
    /// The job has failed after the maximum number of attempts.
    #[postgres(name = "failed")]
    Failed,
}

/// A job record from the `jobs` table.
///
/// It contains the payload and metadata about the job's lifecycle, including
/// its current status, attempt count, and lease information.
#[derive(Debug, Clone)]
pub struct Job {
    /// Unique identifier for the job.
    pub id: i64,
    /// Unique idempotency key for the job.
    pub idempotency_key: String,
    /// Current status of the job.
    pub status: JobStatus,
    /// The data payload associated with the job.
    pub payload: Value,
    /// When the job becomes visible to workers.
    ///
    /// For `Pending` jobs, this is when they can be dequeued.
    /// For `InProgress` jobs, this is when their lease expires.
    pub visible_at: DateTime<Utc>,
    /// Number of times this job has been dequeued.
    pub attempt_count: i32,
    /// Duration (in seconds) that a worker has to process the job before it becomes visible again.
    pub lease_timeout_seconds: i32,
    /// Unique identifier for the current lease.
    pub lease_id: Option<Uuid>,
    /// Identifier of the worker currently holding the lease.
    pub leased_by: Option<String>,
    /// When the job was created.
    pub created_at: DateTime<Utc>,
    /// When the job was last updated.
    pub updated_at: DateTime<Utc>,
}

impl From<&Row> for Job {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("id"),
            idempotency_key: row.get("idempotency_key"),
            status: row.get("status"),
            payload: row.get("payload"),
            visible_at: row.get("visible_at"),
            attempt_count: row.get("attempt_count"),
            lease_timeout_seconds: row.get("lease_timeout_seconds"),
            lease_id: row.get("lease_id"),
            leased_by: row.get("leased_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl Job {
    /// Returns the [`LeaseKey`] required to acknowledge or modify this job,
    /// if it is currently leased.
    pub fn lease_key(&self) -> Option<LeaseKey> {
        self.lease_id.map(|lease_id| LeaseKey {
            job_id: self.id,
            lease_id,
        })
    }
}

/// A token required to perform operations on a leased job.
///
/// It ensures that only the current lease holder can ACK, NACK, or TOUCH a job,
/// preventing "stale" updates if a job is reassigned after a timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    /// The ID of the job.
    pub job_id: i64,
    /// The unique ID of the specific lease.
    pub lease_id: Uuid,
}
