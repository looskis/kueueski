<p align="center">
  <img src="assets/kueueski-icon.png" alt="Kueueski logo" width="128" height="128">
</p>

# kueueski

`kueueski` is a small, script-friendly CLI for inspecting and operating
[BullMQ](https://bullmq.io/) queues. It is written in Rust and uses the official
native `bullmq-official` crate.

## Install

Build from source:

```sh
cargo install --path .
```

Install the latest tagged release from the project tap:

```sh
brew install lookevink/tap/kueueski
```

Before the first tagged release, contributors can install directly from `HEAD`
with `brew install --HEAD ./Formula/kueueski.rb`. See
[RELEASING.md](RELEASING.md) for the one-time tap setup and release process.

## Usage

The Redis URL can be passed with `--redis-url` or stored in
`KUEUESKI_REDIS_URL`. Both `redis://` and TLS-enabled `rediss://` URLs are
supported. The default is `redis://127.0.0.1:6379`.

```sh
# Inspect a queue
kueueski --redis-url redis://localhost:6379 status emails

# Emit JSON for scripts
kueueski --json status emails

# Add a job with inline JSON
kueueski add emails send-email --data '{"to":"user@example.com"}'

# Read job data from a file or stdin
kueueski add emails send-email --data-file job.json
printf '{"to":"user@example.com"}' | kueueski add emails send-email --data -

# Configure BullMQ attempts and exponential backoff
kueueski add emails send-email --attempts 5 \
  --backoff exponential --backoff-delay 1s \
  --data '{"to":"user@example.com"}'

# Claim and process exactly one job
kueueski dequeue emails -- ./scripts/send-email

# Wait up to 30 seconds for one job
kueueski dequeue emails --wait 30s -- ./scripts/send-email

# Run a long-lived worker with four concurrent handlers
kueueski worker emails --concurrency 4 -- ./scripts/send-email

# Globally pause or resume processing
kueueski pause emails
kueueski resume emails
```

If your queues use a non-default BullMQ key prefix, pass `--prefix` or set
`KUEUESKI_PREFIX`.

### Handler contract

`dequeue` and `worker` use BullMQ workers rather than directly popping Redis
keys. This preserves job locks, lock renewal, attempts, fixed and exponential
backoff, stalled-job recovery, rate limits, parent/dependency behavior, and
completed/failed state transitions.

The command after `--` is started once per job:

- The job's JSON data is written to the handler's standard input.
- `KUEUESKI_JOB_ID`, `KUEUESKI_JOB_NAME`, `KUEUESKI_QUEUE`,
  `KUEUESKI_ATTEMPTS_MADE`, and `KUEUESKI_ATTEMPTS_STARTED` are set in its
  environment.
- Standard error is streamed live for logs.
- On exit code `0`, standard output becomes the BullMQ return value. Valid JSON
  remains structured; other text is stored as a string; empty output becomes
  `null`. Output is capped at 1 MiB by default; change the bound with
  `--max-result-bytes`.
- A nonzero exit fails the attempt and lets BullMQ apply the job's retry and
  backoff options. Use repeatable `--unrecoverable-exit-code CODE` flags for
  failures that must skip retries.

`dequeue` pauses its worker before acknowledging the first claim, so it cannot
prefetch a second job. With an empty queue it exits immediately unless `--wait`
is provided. `worker` runs until `SIGINT` or `SIGTERM`, then stops taking new
jobs and allows active handlers to finish for `--shutdown-timeout`. If that
deadline expires, the CLI kills the handler's full process group, exits with an
error, and leaves BullMQ to recover the interrupted job through stalled-job
handling.

Use `--json` for newline-delimited lifecycle events. Worker tuning flags mirror
the official Rust SDK where they make sense for a process-oriented CLI; run
`kueueski worker --help` for concurrency, locking, stalled checks, retention,
metrics, rate limiting, result bounds, and version-check options.

## Development

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

Integration tests that exercise Redis can be added against any disposable
Redis 7 instance; the current test suite does not require a running server.
