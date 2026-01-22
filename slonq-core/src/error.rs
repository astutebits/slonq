use tokio_postgres;

/// Errors that can occur during queue operations.
#[derive(Debug)]
pub enum QueueError {
    /// An error occurred in the underlying PostgreSQL database.
    ///
    /// This could be due to connection issues, syntax errors in SQL,
    /// or other database-level failures.
    Postgres(tokio_postgres::Error),

    /// An invalid argument was provided to a queue method.
    ///
    /// For example, providing a non-positive `batch_size` or `max_attempts`.
    InvalidArgument(String),
}

impl std::fmt::Display for QueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueError::Postgres(e) => write!(f, "postgres error: {}", e),
            QueueError::InvalidArgument(msg) => write!(f, "invalid argument: {}", msg),
        }
    }
}

impl std::error::Error for QueueError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            QueueError::Postgres(e) => Some(e),
            QueueError::InvalidArgument(_) => None,
        }
    }
}

impl From<tokio_postgres::Error> for QueueError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Postgres(e)
    }
}
