#!/usr/bin/env node
// Copies the freshly-built cdylib from target/release into ./index.node so the
// developer-mode loader can pick it up. Used by `npm run build`.

import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { platform } from 'node:process';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..');
const TARGET_DIR = resolve(ROOT, '..', 'target', 'release');

const filename =
  platform === 'darwin' ? 'libslonq_js.dylib'
  : platform === 'win32' ? 'slonq_js.dll'
  : 'libslonq_js.so';

const src = join(TARGET_DIR, filename);
const dst = join(ROOT, 'index.node');

if (!existsSync(src)) {
  console.error(`build artifact not found: ${src}`);
  console.error('did `cargo build --release -p slonq-js` succeed?');
  process.exit(1);
}

mkdirSync(dirname(dst), { recursive: true });
copyFileSync(src, dst);
console.log(`copied ${src} -> ${dst}`);
