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

# Globally pause or resume processing
kueueski pause emails
kueueski resume emails
```

If your queues use a non-default BullMQ key prefix, pass `--prefix` or set
`KUEUESKI_PREFIX`.

## Development

```sh
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

Integration tests that exercise Redis can be added against any disposable
Redis 7 instance; the current test suite does not require a running server.
