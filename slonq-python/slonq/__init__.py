try:
    from .slonq import PgQueue, Job, JobStatus, LeaseKey
except ImportError:
    # This might happen during build or if things are not where we expect
    from slonq import PgQueue, Job, JobStatus, LeaseKey

__all__ = ["PgQueue", "Job", "JobStatus", "LeaseKey"]
