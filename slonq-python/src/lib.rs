use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use slonq::{Job, JobStatus, LeaseKey, PgQueue};
use std::time::Duration;

#[pyclass(name = "JobStatus", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyJobStatus {
    Pending,
    InProgress,
    Done,
    Failed,
}

#[pymethods]
impl PyJobStatus {
    fn __repr__(&self) -> String {
        match self {
            PyJobStatus::Pending => "JobStatus.Pending".to_string(),
            PyJobStatus::InProgress => "JobStatus.InProgress".to_string(),
            PyJobStatus::Done => "JobStatus.Done".to_string(),
            PyJobStatus::Failed => "JobStatus.Failed".to_string(),
        }
    }
}

impl From<JobStatus> for PyJobStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Pending => PyJobStatus::Pending,
            JobStatus::InProgress => PyJobStatus::InProgress,
            JobStatus::Done => PyJobStatus::Done,
            JobStatus::Failed => PyJobStatus::Failed,
        }
    }
}

#[pyclass(name = "LeaseKey")]
#[derive(Clone)]
pub struct PyLeaseKey {
    inner: LeaseKey,
}

#[pymethods]
impl PyLeaseKey {
    #[getter]
    fn job_id(&self) -> i64 {
        self.inner.job_id
    }

    #[getter]
    fn lease_id(&self) -> String {
        self.inner.lease_id.to_string()
    }
}

#[pyclass(name = "Job")]
pub struct PyJob {
    inner: Job,
}

#[pymethods]
impl PyJob {
    #[getter]
    fn id(&self) -> i64 {
        self.inner.id
    }

    #[getter]
    fn idempotency_key(&self) -> String {
        self.inner.idempotency_key.clone()
    }

    #[getter]
    fn status(&self) -> PyJobStatus {
        PyJobStatus::from(self.inner.status)
    }

    #[getter]
    fn payload(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let json_str = serde_json::to_string(&self.inner.payload)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let json_module = py.import("json")?;
        let json_val = json_module.call_method1("loads", (json_str,))?;
        Ok(json_val.into())
    }

    #[getter]
    fn visible_at(&self) -> String {
        self.inner.visible_at.to_rfc3339()
    }

    #[getter]
    fn attempt_count(&self) -> i32 {
        self.inner.attempt_count
    }

    #[getter]
    fn lease_timeout_seconds(&self) -> i32 {
        self.inner.lease_timeout_seconds
    }

    fn lease_key(&self) -> Option<PyLeaseKey> {
        self.inner.lease_key().map(|inner| PyLeaseKey { inner })
    }
}

#[pyclass(name = "PgQueue")]
pub struct PyPgQueue {
    inner: PgQueue,
}

#[pymethods]
impl PyPgQueue {
    #[staticmethod]
    fn connect(py: Python<'_>, pg_uri: String) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let queue = PgQueue::connect(&pg_uri)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(PyPgQueue { inner: queue })
        })
    }

    fn enqueue<'py>(
        &self,
        py: Python<'py>,
        idempotency_key: String,
        payload: Py<PyAny>,
        lease_timeout_seconds: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload_json: serde_json::Value = {
            let json_module = py.import("json")?;
            let json_str: String = json_module.call_method1("dumps", (payload,))?.extract()?;
            serde_json::from_str(&json_str)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?
        };

        let inner = self.inner.clone();
        future_into_py(py, async move {
            let res = inner
                .enqueue(&idempotency_key, payload_json, lease_timeout_seconds, None)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(res.map(|inner| PyJob { inner }))
        })
    }

    fn dequeue<'py>(
        &self,
        py: Python<'py>,
        worker_id: String,
        batch_size: i64,
        max_attempts: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let jobs = inner
                .dequeue(&worker_id, batch_size, max_attempts)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let py_jobs: Vec<PyJob> = jobs.into_iter().map(|j| PyJob { inner: j }).collect();
            Ok(py_jobs)
        })
    }

    fn ack<'py>(&self, py: Python<'py>, lease: PyLeaseKey) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let res = inner
                .ack(lease.inner)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(res.map(|inner| PyJob { inner }))
        })
    }

    fn ack_batch<'py>(
        &self,
        py: Python<'py>,
        leases: Vec<PyLeaseKey>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let leases_inner: Vec<LeaseKey> = leases.into_iter().map(|l| l.inner).collect();
        future_into_py(py, async move {
            let jobs = inner
                .ack_batch(&leases_inner)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let py_jobs: Vec<PyJob> = jobs.into_iter().map(|j| PyJob { inner: j }).collect();
            Ok(py_jobs)
        })
    }

    #[pyo3(signature = (lease, max_attempts, delay_seconds=None))]
    fn nack<'py>(
        &self,
        py: Python<'py>,
        lease: PyLeaseKey,
        max_attempts: i32,
        delay_seconds: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let delay = delay_seconds.map(Duration::from_secs_f64);
        future_into_py(py, async move {
            let res = inner
                .nack(lease.inner, max_attempts, delay)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(res.map(|inner| PyJob { inner }))
        })
    }

    fn touch<'py>(
        &self,
        py: Python<'py>,
        lease: PyLeaseKey,
        lease_seconds: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let res = inner
                .touch(lease.inner, lease_seconds)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(res.map(|inner| PyJob { inner }))
        })
    }
}

#[pymodule(name = "slonq")]
fn slonq_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPgQueue>()?;
    m.add_class::<PyJob>()?;
    m.add_class::<PyJobStatus>()?;
    m.add_class::<PyLeaseKey>()?;
    Ok(())
}
