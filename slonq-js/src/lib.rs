use neon::prelude::*;
use neon::types::JsBox;
use slonq::{Job, JobStatus, LeaseKey, PgQueue, QueueError};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to start tokio runtime"))
}

struct QueueHandle(Arc<PgQueue>);

impl Finalize for QueueHandle {}

type QueueBox = JsBox<QueueHandle>;

fn status_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::InProgress => "in_progress",
        JobStatus::Done => "done",
        JobStatus::Failed => "failed",
    }
}

fn job_to_object<'cx, C: Context<'cx>>(cx: &mut C, job: &Job) -> JsResult<'cx, JsObject> {
    let obj = cx.empty_object();

    let id = cx.number(job.id as f64);
    obj.set(cx, "id", id)?;

    let idem = cx.string(&job.idempotency_key);
    obj.set(cx, "idempotencyKey", idem)?;

    let status = cx.string(status_str(job.status));
    obj.set(cx, "status", status)?;

    let payload_str =
        serde_json::to_string(&job.payload).or_else(|e| cx.throw_error(e.to_string()))?;
    let payload = cx.string(&payload_str);
    obj.set(cx, "payloadJson", payload)?;

    let visible = cx.string(job.visible_at.to_rfc3339());
    obj.set(cx, "visibleAt", visible)?;

    let attempts = cx.number(job.attempt_count as f64);
    obj.set(cx, "attemptCount", attempts)?;

    let lts = cx.number(job.lease_timeout_seconds as f64);
    obj.set(cx, "leaseTimeoutSeconds", lts)?;

    if let Some(lease) = job.lease_key() {
        let lid = cx.string(lease.lease_id.to_string());
        obj.set(cx, "_leaseId", lid)?;
    }

    Ok(obj)
}

fn jobs_to_array<'cx, C: Context<'cx>>(cx: &mut C, jobs: &[Job]) -> JsResult<'cx, JsArray> {
    let arr = cx.empty_array();
    for (i, job) in jobs.iter().enumerate() {
        let obj = job_to_object(cx, job)?;
        arr.set(cx, i as u32, obj)?;
    }
    Ok(arr)
}

/// Turns `Result<Option<Job>, QueueError>` into a settled JS value:
/// some -> Job object, none -> null, err -> thrown JS error.
fn settle_optional_job<'cx, C: Context<'cx>>(
    cx: &mut C,
    res: Result<Option<Job>, QueueError>,
) -> JsResult<'cx, JsValue> {
    match res {
        Ok(Some(job)) => Ok(job_to_object(cx, &job)?.upcast()),
        Ok(None) => Ok(cx.null().upcast()),
        Err(e) => cx.throw_error(e.to_string()),
    }
}

/// Turns `Result<Vec<Job>, QueueError>` into a settled JS value:
/// ok -> Job[] array, err -> thrown JS error.
fn settle_jobs<'cx, C: Context<'cx>>(
    cx: &mut C,
    res: Result<Vec<Job>, QueueError>,
) -> JsResult<'cx, JsValue> {
    match res {
        Ok(jobs) => Ok(jobs_to_array(cx, &jobs)?.upcast()),
        Err(e) => cx.throw_error(e.to_string()),
    }
}

fn extract_lease<'cx, C: Context<'cx>>(
    cx: &mut C,
    value: Handle<'cx, JsValue>,
) -> NeonResult<LeaseKey> {
    let obj = value.downcast_or_throw::<JsObject, _>(cx)?;
    let job_id_h: Handle<JsNumber> = obj.get(cx, "jobId")?;
    let lease_id_h: Handle<JsString> = obj.get(cx, "leaseId")?;
    let job_id = job_id_h.value(cx) as i64;
    let lease_id_str = lease_id_h.value(cx);
    let lease_id = Uuid::parse_str(&lease_id_str)
        .or_else(|e| cx.throw_error(format!("invalid leaseId '{}': {}", lease_id_str, e)))?;
    Ok(LeaseKey { job_id, lease_id })
}

fn pgqueue_connect(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let uri = cx.argument::<JsString>(0)?.value(&mut cx);

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = PgQueue::connect(&uri).await;
        deferred.settle_with(&channel, move |mut cx| match res {
            Ok(q) => {
                let boxed = cx.boxed(QueueHandle(Arc::new(q)));
                Ok(boxed.upcast::<JsValue>())
            }
            Err(e) => cx.throw_error(e.to_string()),
        });
    });
    Ok(promise)
}

fn pgqueue_enqueue(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let idem = cx.argument::<JsString>(1)?.value(&mut cx);
    let payload_str = cx.argument::<JsString>(2)?.value(&mut cx);
    let lease_ts = cx.argument::<JsNumber>(3)?.value(&mut cx) as i32;
    let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
        Ok(v) => v,
        Err(e) => return cx.throw_error(format!("invalid payload JSON: {}", e)),
    };

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.enqueue(&idem, payload, lease_ts, None).await;
        deferred.settle_with(&channel, move |mut cx| settle_optional_job(&mut cx, res));
    });
    Ok(promise)
}

fn pgqueue_dequeue(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let worker_id = cx.argument::<JsString>(1)?.value(&mut cx);
    let batch_size = cx.argument::<JsNumber>(2)?.value(&mut cx) as i64;
    let max_attempts = cx.argument::<JsNumber>(3)?.value(&mut cx) as i32;

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.dequeue(&worker_id, batch_size, max_attempts).await;
        deferred.settle_with(&channel, move |mut cx| settle_jobs(&mut cx, res));
    });
    Ok(promise)
}

fn pgqueue_ack(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let lease_value = cx.argument::<JsValue>(1)?;
    let lease = extract_lease(&mut cx, lease_value)?;

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.ack(lease).await;
        deferred.settle_with(&channel, move |mut cx| settle_optional_job(&mut cx, res));
    });
    Ok(promise)
}

fn pgqueue_ack_batch(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let arr = cx.argument::<JsArray>(1)?;
    let len = arr.len(&mut cx);
    let mut leases: Vec<LeaseKey> = Vec::with_capacity(len as usize);
    for i in 0..len {
        let v: Handle<JsValue> = arr.get(&mut cx, i)?;
        leases.push(extract_lease(&mut cx, v)?);
    }

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.ack_batch(&leases).await;
        deferred.settle_with(&channel, move |mut cx| settle_jobs(&mut cx, res));
    });
    Ok(promise)
}

fn pgqueue_nack(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let lease_value = cx.argument::<JsValue>(1)?;
    let lease = extract_lease(&mut cx, lease_value)?;
    let max_attempts = cx.argument::<JsNumber>(2)?.value(&mut cx) as i32;
    let delay_arg = cx.argument::<JsValue>(3)?;
    let delay = if delay_arg.is_a::<JsNull, _>(&mut cx) || delay_arg.is_a::<JsUndefined, _>(&mut cx)
    {
        None
    } else {
        let secs = delay_arg
            .downcast_or_throw::<JsNumber, _>(&mut cx)?
            .value(&mut cx);
        Some(Duration::from_secs_f64(secs))
    };

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.nack(lease, max_attempts, delay).await;
        deferred.settle_with(&channel, move |mut cx| settle_optional_job(&mut cx, res));
    });
    Ok(promise)
}

fn pgqueue_touch(mut cx: FunctionContext) -> JsResult<JsPromise> {
    let queue: Arc<PgQueue> = Arc::clone(&cx.argument::<QueueBox>(0)?.0);
    let lease_value = cx.argument::<JsValue>(1)?;
    let lease = extract_lease(&mut cx, lease_value)?;
    let lease_seconds = cx.argument::<JsNumber>(2)?.value(&mut cx) as i32;

    let channel = cx.channel();
    let (deferred, promise) = cx.promise();
    runtime().spawn(async move {
        let res = queue.touch(lease, lease_seconds).await;
        deferred.settle_with(&channel, move |mut cx| settle_optional_job(&mut cx, res));
    });
    Ok(promise)
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("pgqueueConnect", pgqueue_connect)?;
    cx.export_function("pgqueueEnqueue", pgqueue_enqueue)?;
    cx.export_function("pgqueueDequeue", pgqueue_dequeue)?;
    cx.export_function("pgqueueAck", pgqueue_ack)?;
    cx.export_function("pgqueueAckBatch", pgqueue_ack_batch)?;
    cx.export_function("pgqueueNack", pgqueue_nack)?;
    cx.export_function("pgqueueTouch", pgqueue_touch)?;
    Ok(())
}
