# slonq

`slonq` (from the Slavic _slon_, meaning elephant, + _q_ for queue) is a reliable, lightweight PostgreSQL job queue.

This is the JavaScript / TypeScript package for `slonq`, providing high-performance bindings to the Rust core via [neon](https://neon-rs.dev/). It is shipped as a Node-API (`.node`) module and works with Node.js and Bun out of the box; Deno's Node-API support is experimental at the time of writing.

Built on top of `FOR UPDATE SKIP LOCKED`, it provides atomic, concurrent task distribution without the need for an external message broker. It is designed for small to medium-scale projects that already use PostgreSQL and require an ‘at-least-once’ delivery guarantee without adding infrastructure complexity.

## Features

- **Atomic concurrency:** Utilises Postgres `SKIP LOCKED` to ensure multiple workers can dequeue tasks simultaneously without collisions.
- **Idempotency support:** Built-in support for idempotency keys to prevent duplicate job insertion.
- **Delayed jobs:** Schedule tasks to become visible at a specific time in the future.
- **Lease mechanism:** Jobs are ‘leased’ to workers; if a worker crashes, the lease expires and the job becomes visible for retry.
- **High performance:** Core logic implemented in Rust with JavaScript bindings via neon.
- **Prebuilt binaries:** Distributed via per-platform optional-dependency packages (`@chmodas/slonq-darwin-arm64`, `@chmodas/slonq-linux-x64-gnu`, etc.). No Rust toolchain required to install.

## Installation

```bash
npm install @chmodas/slonq
```

Requires Node.js ≥ 18.17 (or Bun ≥ 1.x; or any other runtime with Node-API support). Prebuilt binaries are published for macOS (arm64, x64), Linux (x64, arm64; both glibc and musl), and Windows (x64, arm64). Other targets must build from source via `cargo` + `cargo-zigbuild`.

## Quick start

The following example demonstrates the full lifecycle of a job, including enqueuing, dequeuing, heartbeat (touching), and final acknowledgement.

```ts
import { PgQueue } from 'slonq';

async function main() {
  // 1. Initialise the connection to Postgres
  const queue = await PgQueue.connect('postgres://postgres@localhost:5432/db');

  // 2. Enqueue a job with an idempotency key and a 5-minute (300s) lease
  await queue.enqueue(
    'unique-request-id-123',
    { type: 'process_video', path: '/uploads/vid.mp4' },
    300,
  );

  // 3. Dequeue a batch of jobs for 'worker-01'
  // This atomically leases up to 5 jobs for 3 attempts each
  const jobs = await queue.dequeue('worker-01', 5, 3);

  for (const job of jobs) {
    const lease = job.leaseKey();
    if (!lease) continue;

    // 4. Extend the lease (heartbeat)
    // If the task is taking longer than expected, 'touch' it to prevent others from picking it up
    await queue.touch(lease, 60);

    // 5. Success vs failure logic
    const success = true; // Replace with actual processing logic

    if (success) {
      // 6. Acknowledge (mark as done)
      await queue.ack(lease);
    } else {
      // 7. Negatively acknowledge (return to queue with a 10s delay)
      await queue.nack(lease, 3, 10);
    }
  }

  // 8. Batch acknowledgement
  // If you have a list of processed jobs, you can ack them all at once
  // await queue.ackBatch(listOfLeases);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

## API

```ts
import { PgQueue, JobStatus, type Job, type LeaseKey } from 'slonq';

// JobStatus is a const object whose values match the Postgres enum.
JobStatus.Pending;     // 'pending'
JobStatus.InProgress;  // 'in_progress'
JobStatus.Done;        // 'done'
JobStatus.Failed;      // 'failed'

// PgQueue method signatures (all return Promises):
PgQueue.connect(pgUri: string): Promise<PgQueue>;
queue.enqueue(idempotencyKey: string, payload: unknown, leaseTimeoutSeconds: number): Promise<Job | null>;
queue.dequeue(workerId: string, batchSize: number, maxAttempts: number): Promise<Job[]>;
queue.ack(lease: LeaseKey): Promise<Job | null>;
queue.ackBatch(leases: LeaseKey[]): Promise<Job[]>;
queue.nack(lease: LeaseKey, maxAttempts: number, delaySeconds?: number | null): Promise<Job | null>;
queue.touch(lease: LeaseKey, leaseSeconds: number): Promise<Job | null>;

// Job shape:
interface Job {
  id: number;
  idempotencyKey: string;
  status: JobStatus;
  payload: unknown;
  visibleAt: string;       // RFC3339
  attemptCount: number;
  leaseTimeoutSeconds: number;
  leaseKey(): LeaseKey | null;   // null when status !== 'in_progress'
}

interface LeaseKey { jobId: number; leaseId: string }
```

`enqueue` returns `null` if a job with the same `idempotencyKey` already exists. `ack`, `nack`, and `touch` return `null` if the lease has been lost (e.g. the job already timed out and was reaped by another worker).

## How it works

`slonq` manages job states through a visibility-based lease system:

1. **Enqueue:** A job is inserted with a `visible_at` timestamp.
2. **Dequeue:** A worker selects a batch of jobs where `visible_at <= now()`. Using `FOR UPDATE SKIP LOCKED`, Postgres ensures no two workers grab the same job. The `visible_at` is then moved forward by the `lease_timeout`, effectively ‘locking’ the job for that worker.
3. **Heartbeat:** If a job is long-running, the worker can call `touch()` to extend the lease.
4. **Ack/Nack:**
    - **Ack:** Successfully processed jobs are marked as `done`.
    - **Nack:** If a worker fails, it can negatively acknowledge the job to make it visible for retry immediately (or with a delay).
5. **Recovery:** If a worker crashes, the `visible_at` time eventually passes, and the job naturally becomes available for another worker to attempt.

## Operational considerations

- **Database schema:** You must run the provided migration SQL to create the necessary table and indices.
- **Visibility:** Because slonq relies on `now()`, ensure your application servers and database server have synchronised clocks.
- **At-least-once delivery:** slonq guarantees that a job will be delivered to at least one worker. Ensure your worker logic is idempotent.

## Database schema

`slonq` requires a specific table structure and a custom ENUM type to manage job states. You can apply the following migration to your PostgreSQL instance:

```sql
-- Required ONLY for PostgreSQL versions prior to 13 to support gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Define the job lifecycle states
CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'done', 'failed');

CREATE TABLE jobs
(
   id                    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
   idempotency_key       TEXT UNIQUE NOT NULL,
   status                job_status  NOT NULL DEFAULT 'pending',
   payload               JSONB       NOT NULL,

   -- 'Next eligible time' logic:
   -- Pending: When the job becomes available for its first attempt.
   -- In_progress: When the current lease is set to expire.
   visible_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

   -- Tracks attempts to manage retry logic and dead-lettering
   attempt_count         INT         NOT NULL DEFAULT 0,

   -- Per-job lease duration (in seconds)
   lease_timeout_seconds INT         NOT NULL DEFAULT 60 CHECK (lease_timeout_seconds > 0),

   -- Unique lease identifier (regenerated on each dequeue)
   lease_id              UUID NULL,
   leased_by             TEXT NULL,

   created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
   updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for the dequeue operation (FOR UPDATE SKIP LOCKED)
CREATE INDEX idx_jobs_dequeue
   ON jobs (visible_at) WHERE status IN ('pending', 'in_progress');
```

## License

This project is licensed under the MIT License.
