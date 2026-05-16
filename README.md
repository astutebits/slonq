# slonq

`slonq` is a reliable, lightweight job queue that runs entirely on PostgreSQL. No Redis, no RabbitMQ, no separate broker to operate. Under the hood it uses `FOR UPDATE SKIP LOCKED` for atomic, concurrent leasing. The queue protocol – visibility timeouts, lease safety, retry limits, and the races between concurrent workers – is model-checked with [stateright](https://github.com/stateright/stateright).

## Motivation

PostgreSQL is a wonderful engine. A great many systems already running on it can gain a queue without taking on yet another piece of infrastructure. `slonq` still follows the practices you would expect from a dedicated broker: leases, visibility timeouts, idempotency keys, retries. The semantics map cleanly onto a purpose-built engine the day the workload outgrows Postgres. The aim is to favour ease of maintenance over peak performance, until you actually need the performance.

The core is written in Rust using only safe code. That rules out an entire class of memory-safety bugs in `slonq` itself. We cannot speak for every line of every dependency or binding crate – some `unsafe` lives in any large ecosystem. Still, this places the library on the faster and safer end of the spectrum compared with equivalents written directly in Python or Node.js. The Rust ecosystem also makes it fairly straightforward to expose the core through native bindings. That is a win for anyone who, like the author, juggles all three languages on a daily basis.

## Packages

- [`slonq-core`](slonq-core/README.md) – the Rust crate, usable directly from Rust projects.
- [`slonq-python`](slonq-python/README.md) – Python bindings, published to PyPI.
- [`slonq-js`](slonq-js/README.md) – Node.js bindings, published to npm.
