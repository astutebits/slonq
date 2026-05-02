import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { familySync, GLIBC, MUSL } from 'detect-libc';

export interface NativeBindings {
  pgqueueConnect(uri: string): Promise<unknown>;
  pgqueueEnqueue(handle: unknown, idempotencyKey: string, payloadJson: string, leaseTimeoutSeconds: number): Promise<RawJob | null>;
  pgqueueDequeue(handle: unknown, workerId: string, batchSize: number, maxAttempts: number): Promise<RawJob[]>;
  pgqueueAck(handle: unknown, lease: { jobId: number; leaseId: string }): Promise<RawJob | null>;
  pgqueueAckBatch(handle: unknown, leases: Array<{ jobId: number; leaseId: string }>): Promise<RawJob[]>;
  pgqueueNack(handle: unknown, lease: { jobId: number; leaseId: string }, maxAttempts: number, delaySeconds: number | null): Promise<RawJob | null>;
  pgqueueTouch(handle: unknown, lease: { jobId: number; leaseId: string }, leaseSeconds: number): Promise<RawJob | null>;
}

export interface RawJob {
  id: number;
  idempotencyKey: string;
  status: 'pending' | 'in_progress' | 'done' | 'failed';
  payloadJson: string;
  visibleAt: string;
  attemptCount: number;
  leaseTimeoutSeconds: number;
  _leaseId?: string;
}

function computeTriple(): string {
  const { platform, arch } = process;

  if (platform === 'darwin') {
    if (arch === 'arm64') return 'darwin-arm64';
    if (arch === 'x64') return 'darwin-x64';
  }
  if (platform === 'linux') {
    const libc = familySync() === MUSL ? 'musl' : 'gnu';
    if (arch === 'x64') return `linux-x64-${libc}`;
    if (arch === 'arm64') return `linux-arm64-${libc}`;
  }
  if (platform === 'win32') {
    if (arch === 'x64') return 'win32-x64-msvc';
    if (arch === 'arm64') return 'win32-arm64-msvc';
  }
  throw new Error(
    `slonq: no prebuilt binary for ${platform}-${arch}` +
      (platform === 'linux' ? ` (libc=${familySync() ?? 'unknown'})` : '') +
      `. Build from source via cargo-zigbuild.`,
  );
}

const require_ = createRequire(import.meta.url);

function loadNative(): NativeBindings {
  const triple = computeTriple();
  const subpackage = `@chmodas/slonq-${triple}`;

  // Try the optional-deps subpackage first (production install).
  try {
    return require_(subpackage) as NativeBindings;
  } catch (e: unknown) {
    if (!isModuleNotFound(e, subpackage)) throw e;
  }

  // Developer-mode fallback: index.node sitting next to the package root.
  const here = fileURLToPath(import.meta.url);
  const local = resolve(here, '..', '..', 'index.node');
  if (existsSync(local)) {
    return require_(local) as NativeBindings;
  }

  throw new Error(
    `slonq: native module not found for ${triple}. ` +
      `Tried '${subpackage}' (optional dependency) and '${local}' (developer fallback). ` +
      `If you are developing slonq, run \`npm run build\`. Otherwise, please file an issue.`,
  );
}

function isModuleNotFound(err: unknown, name: string): boolean {
  if (!err || typeof err !== 'object') return false;
  const code = (err as { code?: string }).code;
  if (code !== 'MODULE_NOT_FOUND' && code !== 'ERR_MODULE_NOT_FOUND') return false;
  const msg = (err as Error).message ?? '';
  return msg.includes(name);
}

// detect-libc: GLIBC and MUSL are also exported, but we only use MUSL/familySync above.
void GLIBC;

export const native: NativeBindings = loadNative();
