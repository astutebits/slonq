//! `slonq` is a PostgreSQL-backed queue implementation.
//!
//! It provides a reliable way to enqueue, dequeue, and manage jobs using PostgreSQL
//! as the storage and coordination engine. It supports job visibility timeouts,
//! retries, and batch acknowledgements.
//!
//! # Example
//!
//! ```rust,no_run
//! use slonq::{PgQueue, JobStatus};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let queue = PgQueue::connect("postgres://postgres@localhost:5432").await?;
//!
//!     // Enqueue a job
//!     queue.enqueue("unique-key", json!({"data": 123}), 60, None).await?;
//!
//!     // Dequeue a job
//!     let jobs = queue.dequeue("worker-1", 1, 3).await?;
//!     if let Some(job) = jobs.first() {
//!         let lease = job.lease_key().unwrap();
//!         // Do work...
//!         queue.ack(lease).await?;
//!     }
//!
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod job;
pub mod queue;

pub use error::QueueError;
pub use job::{Job, JobStatus, LeaseKey};
pub use queue::PgQueue;
