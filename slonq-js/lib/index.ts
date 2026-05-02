import { native, type RawJob } from './load.js';

export const JobStatus = {
  Pending: 'pending',
  InProgress: 'in_progress',
  Done: 'done',
  Failed: 'failed',
} as const;
export type JobStatus = (typeof JobStatus)[keyof typeof JobStatus];

export interface LeaseKey {
  jobId: number;
  leaseId: string;
}

export interface Job {
  id: number;
  idempotencyKey: string;
  status: JobStatus;
  payload: unknown;
  visibleAt: string;
  attemptCount: number;
  leaseTimeoutSeconds: number;
  leaseKey(): LeaseKey | null;
}

function wrapJob(raw: RawJob): Job {
  const lease: LeaseKey | null = raw._leaseId != null
    ? { jobId: raw.id, leaseId: raw._leaseId }
    : null;
  return {
    id: raw.id,
    idempotencyKey: raw.idempotencyKey,
    status: raw.status,
    payload: JSON.parse(raw.payloadJson),
    visibleAt: raw.visibleAt,
    attemptCount: raw.attemptCount,
    leaseTimeoutSeconds: raw.leaseTimeoutSeconds,
    leaseKey() {
      return lease;
    },
  };
}

function payloadToJson(payload: unknown): string {
  // Mirror the Python binding's behaviour: undefined ⇒ JSON null.
  return JSON.stringify(payload === undefined ? null : payload);
}

export class PgQueue {
  private constructor(private readonly handle: unknown) {}

  static async connect(pgUri: string): Promise<PgQueue> {
    const handle = await native.pgqueueConnect(pgUri);
    return new PgQueue(handle);
  }

  async enqueue(
    idempotencyKey: string,
    payload: unknown,
    leaseTimeoutSeconds: number,
  ): Promise<Job | null> {
    const raw = await native.pgqueueEnqueue(
      this.handle,
      idempotencyKey,
      payloadToJson(payload),
      leaseTimeoutSeconds,
    );
    return raw ? wrapJob(raw) : null;
  }

  async dequeue(
    workerId: string,
    batchSize: number,
    maxAttempts: number,
  ): Promise<Job[]> {
    const raws = await native.pgqueueDequeue(this.handle, workerId, batchSize, maxAttempts);
    return raws.map(wrapJob);
  }

  async ack(lease: LeaseKey): Promise<Job | null> {
    const raw = await native.pgqueueAck(this.handle, lease);
    return raw ? wrapJob(raw) : null;
  }

  async ackBatch(leases: LeaseKey[]): Promise<Job[]> {
    const raws = await native.pgqueueAckBatch(this.handle, leases);
    return raws.map(wrapJob);
  }

  async nack(
    lease: LeaseKey,
    maxAttempts: number,
    delaySeconds?: number | null,
  ): Promise<Job | null> {
    const raw = await native.pgqueueNack(
      this.handle,
      lease,
      maxAttempts,
      delaySeconds ?? null,
    );
    return raw ? wrapJob(raw) : null;
  }

  async touch(lease: LeaseKey, leaseSeconds: number): Promise<Job | null> {
    const raw = await native.pgqueueTouch(this.handle, lease, leaseSeconds);
    return raw ? wrapJob(raw) : null;
  }
}
