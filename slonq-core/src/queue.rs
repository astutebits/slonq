use crate::error::QueueError;
use crate::job::{Job, LeaseKey};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// A PostgreSQL-backed queue client.
///
/// This implementation is lightweight and uses a `tokio-postgres` client internally.
/// It can be safely cloned and shared across tasks.
#[derive(Clone)]
pub struct PgQueue {
    client: Arc<Client>,
}

impl PgQueue {
    // ---------------------------
    // SQL
    // ---------------------------

    const ENQUEUE_SQL: &'static str = r#"
        INSERT INTO jobs (idempotency_key, payload, lease_timeout_seconds, visible_at)
        VALUES ($1, $2, $3, COALESCE($4, now()))
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *;
    "#;

    // Includes inline reap of exhausted timed-out in_progress jobs.
    const DEQUEUE_SQL: &'static str = r#"
        WITH exhausted AS (
            SELECT id
            FROM jobs
            WHERE status = 'in_progress'
              AND visible_at <= now()
              AND attempt_count >= $1
            FOR UPDATE SKIP LOCKED
        ),
        mark_failed AS (
            UPDATE jobs
            SET status     = 'failed',
                updated_at = now(),
                lease_id   = NULL,
                leased_by  = NULL
            FROM exhausted
            WHERE jobs.id = exhausted.id
            RETURNING jobs.id
        ),
        next_job AS (
            SELECT id
            FROM jobs
            WHERE status IN ('pending', 'in_progress')
              AND visible_at <= now()
              AND attempt_count < $1
            ORDER BY visible_at, created_at, id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET status        = 'in_progress',
            updated_at    = now(),
            visible_at    = now() + make_interval(secs => lease_timeout_seconds::float8),
            attempt_count = attempt_count + 1,
            lease_id      = gen_random_uuid(),
            leased_by     = $3
        FROM next_job
        WHERE jobs.id = next_job.id
        RETURNING jobs.*;
    "#;

    const ACK_SQL: &'static str = r#"
        UPDATE jobs
        SET status     = 'done',
            updated_at = now(),
            lease_id   = NULL,
            leased_by  = NULL
        WHERE id = $1
          AND lease_id = $2
          AND status = 'in_progress'
        RETURNING *;
    "#;

    // Pair job_ids + lease_ids *safely* using multi-arg UNNEST.
    const ACK_BATCH_SQL: &'static str = r#"
        UPDATE jobs j
        SET status     = 'done',
            updated_at = now(),
            lease_id   = NULL,
            leased_by  = NULL
        FROM unnest($1::bigint[], $2::uuid[]) AS x(id, lease_id)
        WHERE j.id = x.id
          AND j.lease_id = x.lease_id
          AND j.status = 'in_progress'
        RETURNING j.*;
    "#;

    // Immediate retry until max_attempts; else terminal failed.
    const NACK_SQL: &'static str = r#"
        UPDATE jobs
        SET status = CASE
                WHEN attempt_count >= $3 THEN 'failed'::job_status
                ELSE 'pending'::job_status
            END,
            updated_at = now(),
            visible_at = CASE
                WHEN attempt_count >= $3 THEN now()
                ELSE now() + make_interval(secs => $4)
            END,
            lease_id = NULL,
            leased_by = NULL
        WHERE id = $1
          AND lease_id = $2
          AND status = 'in_progress'
        RETURNING *;
    "#;

    const TOUCH_SQL: &'static str = r#"
        UPDATE jobs
        SET visible_at = now() + make_interval(secs => $3),
            updated_at = now()
        WHERE id = $1
          AND lease_id = $2
          AND status = 'in_progress'
        RETURNING *;
    "#;

    // ---------------------------
    // Construction
    // ---------------------------

    /// Connects to a PostgreSQL database using the provided URI.
    ///
    /// This method also spawns a background task to handle the connection.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect(pg_uri: &str) -> Result<Self, QueueError> {
        let (client, connection) = tokio_postgres::connect(pg_uri, NoTls).await?;
        tokio::spawn(async move {
            // If this ends, all queries will error; surface via logs later if you want.
            let _ = connection.await;
        });
        Ok(Self::from_client(client))
    }

    /// Creates a new `PgQueue` from an existing `tokio_postgres::Client`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    /// use tokio_postgres::NoTls;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let (client, connection) = tokio_postgres::connect("postgres://postgres@localhost:5432", NoTls).await?;
    ///     tokio::spawn(async move { let _ = connection.await; });
    ///
    ///     let queue = PgQueue::from_client(client);
    ///     Ok(())
    /// }
    /// ```
    pub fn from_client(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    // ---------------------------
    // API
    // ---------------------------

    /// Enqueues a new job into the queue.
    ///
    /// * `idempotency_key`: A unique key to prevent duplicate jobs.
    /// * `payload`: The data payload for the job (as a JSON value).
    /// * `lease_timeout_seconds`: How long (in seconds) the job should be leased for when dequeued.
    /// * `visible_at`: Optional time when the job should first become visible. Defaults to `now()`.
    ///
    /// Returns `Ok(Some(job))` if the job was successfully enqueued, or `Ok(None)` if
    /// a job with the same idempotency key already exists.
    ///
    /// Note: This only guarantees at-least-once delivery. A worker can perform a side effect
    /// and crash before acknowledging the job, causing it to be retried.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    /// use serde_json::json;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     queue.enqueue("unique-key", json!({"task": "send_email"}), 300, None).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn enqueue(
        &self,
        idempotency_key: &str,
        payload: Value,
        lease_timeout_seconds: i32,
        visible_at: Option<DateTime<Utc>>,
    ) -> Result<Option<Job>, QueueError> {
        if lease_timeout_seconds <= 0 {
            return Err(QueueError::InvalidArgument(
                "lease_timeout_seconds must be > 0".to_string(),
            ));
        }

        let row = self
            .client
            .query_opt(
                Self::ENQUEUE_SQL,
                &[
                    &idempotency_key,
                    &payload,
                    &lease_timeout_seconds,
                    &visible_at,
                ],
            )
            .await?;

        Ok(row.as_ref().map(Job::from))
    }

    /// Dequeues up to `batch_size` jobs, atomically leasing them to `worker_id`.
    ///
    /// The jobs will be marked as `InProgress` and their `visible_at` will be updated
    /// to `now() + lease_timeout_seconds`.
    ///
    /// * `worker_id`: A unique identifier for the worker performing the dequeue.
    /// * `batch_size`: Maximum number of jobs to dequeue at once.
    /// * `max_attempts`: Maximum number of times a job can be dequeued before it is marked as `failed`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     let jobs = queue.dequeue("worker-1", 5, 3).await?;
    ///     for job in jobs {
    ///         println!("Processing job: {}", job.id);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn dequeue(
        &self,
        worker_id: &str,
        batch_size: i64,
        max_attempts: i32,
    ) -> Result<Vec<Job>, QueueError> {
        if batch_size <= 0 {
            return Err(QueueError::InvalidArgument(
                "batch_size must be > 0".to_string(),
            ));
        }
        if max_attempts <= 0 {
            return Err(QueueError::InvalidArgument(
                "max_attempts must be > 0".to_string(),
            ));
        }

        let rows = self
            .client
            .query(Self::DEQUEUE_SQL, &[&max_attempts, &batch_size, &worker_id])
            .await?;

        Ok(rows.iter().map(Job::from).collect())
    }

    /// Acknowledges a job as successfully completed.
    ///
    /// This succeeds only if the provided `lease` is still valid (i.e., the job is
    /// `InProgress` and the `lease_id` matches).
    ///
    /// Returns `Ok(Some(job))` if the job was successfully updated, or `Ok(None)` if
    /// the lease was lost or the job was not in progress.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     let jobs = queue.dequeue("worker-1", 1, 3).await?;
    ///     if let Some(job) = jobs.first() {
    ///         let lease = job.lease_key().unwrap();
    ///         queue.ack(lease).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn ack(&self, lease: LeaseKey) -> Result<Option<Job>, QueueError> {
        let rows = self
            .client
            .query(Self::ACK_SQL, &[&lease.job_id, &lease.lease_id])
            .await?;

        Ok(rows.first().map(Job::from))
    }

    /// Acknowledges a batch of jobs as successfully completed.
    ///
    /// Returns the list of jobs that were successfully updated.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     let jobs = queue.dequeue("worker-1", 10, 3).await?;
    ///     let leases: Vec<_> = jobs.iter().filter_map(|j| j.lease_key()).collect();
    ///     queue.ack_batch(&leases).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn ack_batch(&self, leases: &[LeaseKey]) -> Result<Vec<Job>, QueueError> {
        if leases.is_empty() {
            return Ok(vec![]);
        }

        let mut job_ids: Vec<i64> = Vec::with_capacity(leases.len());
        let mut lease_ids: Vec<Uuid> = Vec::with_capacity(leases.len());
        for l in leases {
            job_ids.push(l.job_id);
            lease_ids.push(l.lease_id);
        }

        let rows = self
            .client
            .query(Self::ACK_BATCH_SQL, &[&job_ids, &lease_ids])
            .await?;

        Ok(rows.iter().map(Job::from).collect())
    }

    /// Negatively acknowledges a job, returning it to the `pending` state for retry.
    ///
    /// If the job has reached `max_attempts`, it will be marked as `failed` instead.
    /// This succeeds only if the provided `lease` is still valid.
    ///
    /// * `lease`: The lease key for the job.
    /// * `max_attempts`: The maximum number of attempts for this job.
    /// * `delay`: Optional duration to wait before the job becomes visible again.
    ///
    /// Returns `Ok(Some(job))` if the job was successfully updated, or `Ok(None)` if
    /// the lease was lost or the job was not in progress.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     let jobs = queue.dequeue("worker-1", 1, 3).await?;
    ///     if let Some(job) = jobs.first() {
    ///         let lease = job.lease_key().unwrap();
    ///         // Nack with a 10-second retry delay
    ///         queue.nack(lease, 3, Some(Duration::from_secs(10))).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn nack(
        &self,
        lease: LeaseKey,
        max_attempts: i32,
        delay: Option<Duration>,
    ) -> Result<Option<Job>, QueueError> {
        if max_attempts <= 0 {
            return Err(QueueError::InvalidArgument(
                "max_attempts must be > 0".to_string(),
            ));
        }

        let delay_secs = delay.map(|d| d.as_secs_f64()).unwrap_or(0.0);

        let rows = self
            .client
            .query(
                Self::NACK_SQL,
                &[&lease.job_id, &lease.lease_id, &max_attempts, &delay_secs],
            )
            .await?;

        Ok(rows.first().map(Job::from))
    }

    /// Extends the lease of an `InProgress` job.
    ///
    /// This updates the job's `visible_at` to `now() + lease_seconds`.
    /// This succeeds only if the provided `lease` is still valid.
    ///
    /// Returns `Ok(Some(job))` if the job was successfully updated, or `Ok(None)` if
    /// the lease was lost or the job was not in progress.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use slonq::PgQueue;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
    ///     let jobs = queue.dequeue("worker-1", 1, 3).await?;
    ///     if let Some(job) = jobs.first() {
    ///         let lease = job.lease_key().unwrap();
    ///         // Extend lease by another 60 seconds
    ///         queue.touch(lease, 60).await?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn touch(
        &self,
        lease: LeaseKey,
        lease_seconds: i32,
    ) -> Result<Option<Job>, QueueError> {
        if lease_seconds <= 0 {
            return Err(QueueError::InvalidArgument(
                "lease_seconds must be > 0".to_string(),
            ));
        }

        let rows = self
            .client
            .query(
                Self::TOUCH_SQL,
                &[&lease.job_id, &lease.lease_id, &(lease_seconds as f64)],
            )
            .await?;

        Ok(rows.first().map(Job::from))
    }
}
