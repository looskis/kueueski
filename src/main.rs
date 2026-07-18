use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bullmq::options::RedisConnectionOptions;
use bullmq::types::BackoffStrategy;
use bullmq::types::{KeepJobs, RemoveOnFinish};
use bullmq::{
    Error as BullError, Job, JobOptions, MetricsOptions, Queue, QueueOptions, RateLimiterOptions,
    Worker as BullWorker, WorkerOptions, worker::CancellationToken,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    /// Redis connection URL. Supports redis:// and rediss://.
    #[arg(
        long,
        env = "KUEUESKI_REDIS_URL",
        default_value = "redis://127.0.0.1:6379",
        global = true
    )]
    redis_url: RedisUrl,

    /// BullMQ Redis key prefix.
    #[arg(long, env = "KUEUESKI_PREFIX", default_value = "bull", global = true)]
    prefix: String,

    /// Emit machine-readable JSON.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Show queue state and job counts.
    Status { queue: String },

    /// Add a job to a queue.
    Add {
        queue: String,
        name: String,

        /// Job data as JSON. Use '-' to read from stdin.
        #[arg(long, value_name = "JSON", conflicts_with = "data_file")]
        data: Option<String>,

        /// Read job data as JSON from a file.
        #[arg(long, value_name = "PATH", conflicts_with = "data")]
        data_file: Option<PathBuf>,

        /// Delay before the job becomes available.
        #[arg(long, value_parser = humantime::parse_duration)]
        delay: Option<Duration>,

        /// Job priority. Lower nonzero values run first.
        #[arg(long)]
        priority: Option<u32>,

        /// Total processing attempts before permanent failure.
        #[arg(long, value_parser = parse_positive_u32)]
        attempts: Option<u32>,

        /// Retry backoff strategy.
        #[arg(long, value_enum)]
        backoff: Option<BackoffKind>,

        /// Base delay for the retry backoff strategy.
        #[arg(long, default_value = "0s", requires = "backoff", value_parser = humantime::parse_duration)]
        backoff_delay: Duration,

        /// Process this job before older jobs of the same priority.
        #[arg(long)]
        lifo: bool,

        /// Custom unique job ID.
        #[arg(long)]
        job_id: Option<String>,

        /// Keep at most this many completed jobs.
        #[arg(long)]
        remove_on_complete: Option<usize>,

        /// Keep at most this many failed jobs.
        #[arg(long)]
        remove_on_fail: Option<usize>,

        /// Maximum number of job log entries to retain.
        #[arg(long)]
        keep_logs: Option<u32>,

        /// Reject serialized job data larger than this many bytes.
        #[arg(long)]
        size_limit: Option<usize>,
    },

    /// Claim and process one job with an external command.
    Dequeue(DequeueArgs),

    /// Continuously process jobs with an external command.
    Worker(WorkerArgs),

    /// Pause a queue globally.
    Pause { queue: String },

    /// Resume a paused queue.
    Resume { queue: String },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BackoffKind {
    Fixed,
    Exponential,
}

#[derive(Debug, Args)]
struct DequeueArgs {
    /// Queue to process.
    queue: String,

    /// How long to wait for a job after finding the queue empty.
    #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
    wait: Duration,

    #[command(flatten)]
    tuning: WorkerTuning,

    /// Handler command and arguments. Job JSON is written to stdin.
    #[arg(
        required = true,
        value_name = "COMMAND",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    command: Vec<OsString>,
}

#[derive(Debug, Args)]
struct WorkerArgs {
    /// Queue to process.
    queue: String,

    /// Maximum number of jobs processed concurrently.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    concurrency: usize,

    #[command(flatten)]
    tuning: WorkerTuning,

    /// Handler command and arguments. Job JSON is written to stdin.
    #[arg(
        required = true,
        value_name = "COMMAND",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    command: Vec<OsString>,
}

#[derive(Clone, Debug, Args)]
struct WorkerTuning {
    /// Worker name recorded on processed jobs.
    #[arg(long)]
    name: Option<String>,

    /// Duration of each BullMQ job lock.
    #[arg(long, default_value = "30s", value_parser = parse_positive_duration)]
    lock_duration: Duration,

    /// Interval between automatic job-lock renewals.
    #[arg(long, value_parser = parse_positive_duration)]
    lock_renew_time: Option<Duration>,

    /// Time allowed for active handlers to finish during shutdown.
    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    shutdown_timeout: Duration,

    /// Interval between stalled-job checks.
    #[arg(long, default_value = "30s", value_parser = parse_positive_duration)]
    stalled_interval: Duration,

    /// Delay before retrying a transient worker error.
    #[arg(long, default_value = "15s", value_parser = parse_positive_duration)]
    run_retry_delay: Duration,

    /// Seconds between empty-queue checks.
    #[arg(long, default_value_t = 5, value_parser = parse_positive_u64)]
    drain_delay: u64,

    /// Number of stalls allowed before a job permanently fails.
    #[arg(long, default_value_t = 1)]
    max_stalled_count: u32,

    /// Maximum times a job may start before it permanently fails.
    #[arg(long)]
    max_started_attempts: Option<u32>,

    /// Keep at most this many completed jobs.
    #[arg(long)]
    remove_on_complete: Option<usize>,

    /// Keep completed jobs no older than this duration.
    #[arg(long, value_parser = humantime::parse_duration)]
    remove_on_complete_age: Option<Duration>,

    /// Maximum aged completed jobs removed per cleanup pass.
    #[arg(long)]
    remove_on_complete_limit: Option<usize>,

    /// Keep at most this many failed jobs.
    #[arg(long)]
    remove_on_fail: Option<usize>,

    /// Keep failed jobs no older than this duration.
    #[arg(long, value_parser = humantime::parse_duration)]
    remove_on_fail_age: Option<Duration>,

    /// Maximum aged failed jobs removed per cleanup pass.
    #[arg(long)]
    remove_on_fail_limit: Option<usize>,

    /// Maximum number of worker metric data points to retain.
    #[arg(long)]
    metrics_max_data_points: Option<usize>,

    /// Maximum jobs allowed in each rate-limit window.
    #[arg(long, requires = "rate_limit_duration", value_parser = parse_positive_u64)]
    rate_limit_max: Option<u64>,

    /// Duration of each rate-limit window.
    #[arg(long, requires = "rate_limit_max", value_parser = parse_positive_duration)]
    rate_limit_duration: Option<Duration>,

    /// Longest delay accepted from the rate limiter.
    #[arg(long, default_value = "30s", value_parser = parse_positive_duration)]
    maximum_rate_limit_delay: Duration,

    /// Maximum handler stdout stored as the job return value.
    #[arg(long, default_value_t = 1_048_576, value_parser = parse_positive_usize)]
    max_result_bytes: usize,

    /// Treat this handler exit code as unrecoverable. May be repeated.
    #[arg(long, value_name = "CODE", action = clap::ArgAction::Append)]
    unrecoverable_exit_code: Vec<i32>,

    /// Skip the Redis server version compatibility check.
    #[arg(long)]
    skip_version_check: bool,

    /// Disable stalled-job checks for this worker.
    #[arg(long)]
    skip_stalled_check: bool,

    /// Disable automatic job-lock renewal.
    #[arg(long)]
    skip_lock_renewal: bool,
}

#[derive(Clone, Debug)]
struct HandlerSpec {
    queue: String,
    command: Vec<OsString>,
    unrecoverable_exit_codes: Vec<i32>,
    max_result_bytes: usize,
}

struct ClaimGate {
    acknowledge: oneshot::Sender<()>,
}

#[derive(Debug, Serialize)]
struct Status {
    queue: String,
    paused: bool,
    workers: usize,
    counts: std::collections::HashMap<String, u64>,
}

#[derive(Clone, Debug)]
struct RedisUrl(String);

impl RedisUrl {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for RedisUrl {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.starts_with("redis://") || value.starts_with("rediss://") {
            Ok(Self(value.to_owned()))
        } else {
            Err("Redis URL must begin with redis:// or rediss://".to_owned())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Status { queue: queue_name } => {
            let queue = connect_queue(&queue_name, &cli.redis_url, &cli.prefix).await?;
            let counts = queue
                .get_job_counts_by_types(&[])
                .await
                .context("failed to read queue job counts")?;
            let paused = queue
                .is_paused()
                .await
                .context("failed to read queue state")?;
            let workers = queue
                .get_workers_count()
                .await
                .context("failed to count queue workers")?;
            let status = Status {
                queue: queue_name,
                paused,
                workers,
                counts,
            };
            if cli.json {
                println!("{}", serde_json::to_string(&status)?);
            } else {
                print_status(&status);
            }
            queue.close().await;
        }
        Command::Add {
            queue: queue_name,
            name,
            data,
            data_file,
            delay,
            priority,
            attempts,
            backoff,
            backoff_delay,
            lifo,
            job_id,
            remove_on_complete,
            remove_on_fail,
            keep_logs,
            size_limit,
        } => {
            let queue = connect_queue(&queue_name, &cli.redis_url, &cli.prefix).await?;
            let data = read_data(data, data_file)?;
            let backoff = backoff
                .map(|kind| -> Result<BackoffStrategy> {
                    let delay = duration_millis(backoff_delay, "backoff delay")?;
                    Ok(match kind {
                        BackoffKind::Fixed => BackoffStrategy::Fixed(delay),
                        BackoffKind::Exponential => BackoffStrategy::Exponential(delay),
                    })
                })
                .transpose()?;
            let options = JobOptions {
                delay: delay
                    .map(|duration| duration_millis(duration, "job delay"))
                    .transpose()?,
                priority,
                attempts,
                backoff,
                lifo: lifo.then_some(true),
                remove_on_complete: remove_on_complete.map(RemoveOnFinish::Count),
                remove_on_fail: remove_on_fail.map(RemoveOnFinish::Count),
                keep_logs,
                job_id,
                size_limit,
                ..Default::default()
            };
            let job = queue
                .add(&name, data)
                .options(options)
                .await
                .with_context(|| format!("failed to add job to queue '{queue_name}'"))?;
            if cli.json {
                println!(
                    "{}",
                    json!({ "queue": queue_name, "id": job.id(), "name": name })
                );
            } else {
                println!("Added job {} ({name}) to {queue_name}", job.id());
            }
            queue.close().await;
        }
        Command::Dequeue(args) => {
            run_handler_worker(
                &cli.redis_url,
                &cli.prefix,
                cli.json,
                args.queue,
                args.command,
                args.tuning,
                WorkerMode::Once { wait: args.wait },
            )
            .await?;
        }
        Command::Worker(args) => {
            run_handler_worker(
                &cli.redis_url,
                &cli.prefix,
                cli.json,
                args.queue,
                args.command,
                args.tuning,
                WorkerMode::Continuous {
                    concurrency: args.concurrency,
                },
            )
            .await?;
        }
        Command::Pause { queue: queue_name } => {
            let queue = connect_queue(&queue_name, &cli.redis_url, &cli.prefix).await?;
            queue
                .pause()
                .await
                .with_context(|| format!("failed to pause queue '{queue_name}'"))?;
            print_action(cli.json, &queue_name, "paused");
            queue.close().await;
        }
        Command::Resume { queue: queue_name } => {
            let queue = connect_queue(&queue_name, &cli.redis_url, &cli.prefix).await?;
            queue
                .resume()
                .await
                .with_context(|| format!("failed to resume queue '{queue_name}'"))?;
            print_action(cli.json, &queue_name, "resumed");
            queue.close().await;
        }
    }

    Ok(())
}

async fn connect_queue(name: &str, redis_url: &RedisUrl, prefix: &str) -> Result<Queue> {
    let options = QueueOptions {
        connection: RedisConnectionOptions {
            url: redis_url.as_str().to_owned(),
            ..Default::default()
        },
        prefix: prefix.to_owned(),
        ..Default::default()
    };
    Queue::with_options(name, options)
        .await
        .with_context(|| format!("failed to connect to Redis for queue '{name}'"))
}

#[derive(Clone, Copy, Debug)]
enum WorkerMode {
    Once { wait: Duration },
    Continuous { concurrency: usize },
}

async fn run_handler_worker(
    redis_url: &RedisUrl,
    prefix: &str,
    as_json: bool,
    queue: String,
    command: Vec<OsString>,
    tuning: WorkerTuning,
    mode: WorkerMode,
) -> Result<()> {
    validate_handler_command(&command)?;
    let concurrency = match mode {
        WorkerMode::Once { .. } => 1,
        WorkerMode::Continuous { concurrency } => concurrency,
    };
    let options = build_worker_options(redis_url, prefix, concurrency, &tuning)?;
    let spec = Arc::new(HandlerSpec {
        queue: queue.clone(),
        command,
        unrecoverable_exit_codes: tuning.unrecoverable_exit_code.clone(),
        max_result_bytes: tuning.max_result_bytes,
    });

    let (claim_tx, mut claim_rx) = mpsc::unbounded_channel::<ClaimGate>();
    let use_claim_gate = matches!(mode, WorkerMode::Once { .. });
    let processor = move |job: Job, token: CancellationToken| {
        let spec = Arc::clone(&spec);
        let claim_tx = claim_tx.clone();
        async move {
            if use_claim_gate {
                let (acknowledge, acknowledged) = oneshot::channel();
                claim_tx
                    .send(ClaimGate { acknowledge })
                    .map_err(|_| BullError::WorkerClosed)?;
                acknowledged.await.map_err(|_| BullError::WorkerClosed)?;
            }
            execute_handler(job, token, spec).await
        }
    };

    let worker = BullWorker::with_options(&queue, processor, options)
        .await
        .with_context(|| format!("failed to start worker for queue '{queue}'"))?;
    emit_worker_event(as_json, &queue, "ready", None, None);

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let wait_duration = match mode {
        WorkerMode::Once { wait } => wait,
        WorkerMode::Continuous { .. } => Duration::ZERO,
    };
    let wait_deadline = tokio::time::Instant::now() + wait_duration;
    let wait_timeout = tokio::time::sleep_until(wait_deadline);
    tokio::pin!(wait_timeout);
    let mut waiting_after_drain = false;
    let mut claimed = false;

    loop {
        tokio::select! {
            Some(gate) = claim_rx.recv(), if use_claim_gate && !claimed => {
                claimed = true;
                worker.pause();
                let _ = gate.acknowledge.send(());
            }
            event = worker.next_event() => {
                let Some(event) = event else {
                    close_worker(&worker, tuning.shutdown_timeout).await?;
                    bail!("worker event stream closed unexpectedly");
                };
                match event {
                    bullmq::worker::WorkerEvent::Ready => {}
                    bullmq::worker::WorkerEvent::Active { job_id } => {
                        emit_worker_event(as_json, &queue, "active", Some(&job_id), None);
                    }
                    bullmq::worker::WorkerEvent::Completed { job_id, result } => {
                        emit_worker_event(as_json, &queue, "completed", Some(&job_id), Some(&result));
                        if use_claim_gate {
                            close_worker(&worker, tuning.shutdown_timeout).await?;
                            return Ok(());
                        }
                    }
                    bullmq::worker::WorkerEvent::Failed { job_id, error } => {
                        emit_worker_event(as_json, &queue, "failed", Some(&job_id), Some(&Value::String(error.clone())));
                        if use_claim_gate {
                            close_worker(&worker, tuning.shutdown_timeout).await?;
                            bail!("job {job_id} failed: {error}");
                        }
                    }
                    bullmq::worker::WorkerEvent::Error(error) => {
                        emit_worker_event(as_json, &queue, "error", None, Some(&Value::String(error.clone())));
                        if use_claim_gate {
                            close_worker(&worker, tuning.shutdown_timeout).await?;
                            bail!("worker error: {error}");
                        }
                    }
                    bullmq::worker::WorkerEvent::Drained => {
                        if use_claim_gate && !claimed {
                            if wait_duration.is_zero() {
                                close_worker(&worker, tuning.shutdown_timeout).await?;
                                bail!("no job available in queue '{queue}'");
                            }
                            waiting_after_drain = true;
                        }
                    }
                    bullmq::worker::WorkerEvent::Closed
                    | bullmq::worker::WorkerEvent::Paused
                    | bullmq::worker::WorkerEvent::Resumed
                    | bullmq::worker::WorkerEvent::Stalled { .. }
                    | bullmq::worker::WorkerEvent::Progress { .. } => {}
                }
            }
            _ = &mut wait_timeout, if use_claim_gate && waiting_after_drain && !claimed => {
                close_worker(&worker, tuning.shutdown_timeout).await?;
                bail!("timed out waiting for a job in queue '{queue}'");
            }
            _ = &mut shutdown => {
                emit_worker_event(as_json, &queue, "shutting-down", None, None);
                close_worker(&worker, tuning.shutdown_timeout).await?;
                return Ok(());
            }
        }
    }
}

fn validate_handler_command(command: &[OsString]) -> Result<()> {
    let executable = command
        .first()
        .ok_or_else(|| anyhow!("handler command is empty"))?;
    let path = std::path::Path::new(executable);
    let found = if path.components().count() > 1 {
        is_executable_file(path)
    } else {
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
            .map(|directory| directory.join(path))
            .any(|candidate| is_executable_file(&candidate))
    };
    if found {
        Ok(())
    } else {
        bail!(
            "handler executable was not found or is not executable: {}",
            path.display()
        )
    }
}

fn is_executable_file(path: &std::path::Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn build_worker_options(
    redis_url: &RedisUrl,
    prefix: &str,
    concurrency: usize,
    tuning: &WorkerTuning,
) -> Result<WorkerOptions> {
    if tuning
        .lock_renew_time
        .is_some_and(|renew_time| renew_time >= tuning.lock_duration)
    {
        bail!("lock renewal interval must be shorter than the lock duration");
    }
    let limiter = match (tuning.rate_limit_max, tuning.rate_limit_duration) {
        (Some(max), Some(duration)) => Some(RateLimiterOptions::new(max, duration)),
        (None, None) => None,
        _ => bail!("--rate-limit-max and --rate-limit-duration must be used together"),
    };
    Ok(WorkerOptions {
        connection: RedisConnectionOptions {
            url: redis_url.as_str().to_owned(),
            ..Default::default()
        },
        prefix: prefix.to_owned(),
        name: tuning.name.clone(),
        concurrency,
        lock_duration: duration_millis(tuning.lock_duration, "lock duration")?,
        lock_renew_time: tuning
            .lock_renew_time
            .map(|duration| duration_millis(duration, "lock renewal interval"))
            .transpose()?,
        max_stalled_count: tuning.max_stalled_count,
        stalled_interval: duration_millis(tuning.stalled_interval, "stalled interval")?,
        drain_delay: tuning.drain_delay,
        skip_version_check: tuning.skip_version_check,
        remove_on_complete: retention_policy(
            tuning.remove_on_complete,
            tuning.remove_on_complete_age,
            tuning.remove_on_complete_limit,
            "completed-job retention age",
        )?,
        remove_on_fail: retention_policy(
            tuning.remove_on_fail,
            tuning.remove_on_fail_age,
            tuning.remove_on_fail_limit,
            "failed-job retention age",
        )?,
        run_retry_delay: duration_millis(tuning.run_retry_delay, "retry delay")?,
        limiter,
        maximum_rate_limit_delay: duration_millis(
            tuning.maximum_rate_limit_delay,
            "maximum rate-limit delay",
        )?,
        max_started_attempts: tuning.max_started_attempts,
        skip_stalled_check: tuning.skip_stalled_check,
        skip_lock_renewal: tuning.skip_lock_renewal,
        metrics: tuning
            .metrics_max_data_points
            .map(|max_data_points| MetricsOptions { max_data_points }),
        ..Default::default()
    })
}

fn retention_policy(
    count: Option<usize>,
    age: Option<Duration>,
    limit: Option<usize>,
    age_name: &str,
) -> Result<Option<RemoveOnFinish>> {
    if age.is_none() && limit.is_none() {
        return Ok(count.map(RemoveOnFinish::Count));
    }
    Ok(Some(RemoveOnFinish::Options(KeepJobs {
        age: age
            .map(|duration| duration_millis(duration, age_name))
            .transpose()?,
        count,
        limit,
    })))
}

async fn execute_handler(
    job: Job,
    token: CancellationToken,
    spec: Arc<HandlerSpec>,
) -> std::result::Result<Value, BullError> {
    let executable = spec
        .command
        .first()
        .ok_or_else(|| BullError::InvalidConfig("handler command is empty".to_owned()))?;
    let mut command = tokio::process::Command::new(executable);
    command
        .args(&spec.command[1..])
        .env("KUEUESKI_JOB_ID", job.id())
        .env("KUEUESKI_JOB_NAME", job.name())
        .env("KUEUESKI_QUEUE", &spec.queue)
        .env("KUEUESKI_ATTEMPTS_MADE", job.attempts_made().to_string())
        .env(
            "KUEUESKI_ATTEMPTS_STARTED",
            job.attempts_started().to_string(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.as_std_mut().process_group(0);
    }

    let mut child = command
        .spawn()
        .map_err(|error| BullError::ProcessingError(format!("failed to start handler: {error}")))?;
    let mut process_group = ProcessGroupGuard::new(child.id());
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| BullError::ProcessingError("failed to capture handler stdout".to_owned()))?;
    let stdout_task = tokio::spawn(read_bounded(stdout, spec.max_result_bytes));
    if let Some(mut stdin) = child.stdin.take() {
        let mut payload = serde_json::to_vec(job.data())?;
        payload.push(b'\n');
        if let Err(error) = stdin.write_all(&payload).await
            && error.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(BullError::ProcessingError(format!(
                "failed to write job data to handler: {error}"
            )));
        }
    }

    let status = tokio::select! {
        status = child.wait() => status.map_err(|error| {
            BullError::ProcessingError(format!("failed to wait for handler: {error}"))
        })?,
        _ = token.cancelled() => {
            process_group.kill();
            let _ = child.wait().await;
            return Err(BullError::WorkerClosed);
        },
    };
    process_group.kill_remaining_and_disarm();
    let stdout = stdout_task
        .await
        .map_err(|error| BullError::ProcessingError(format!("stdout reader failed: {error}")))?
        .map_err(|error| {
            BullError::ProcessingError(format!("failed to read handler stdout: {error}"))
        })?;
    if stdout.exceeded_limit {
        return Err(BullError::ProcessingError(format!(
            "handler stdout exceeded the {} byte result limit",
            spec.max_result_bytes
        )));
    }

    if status.success() {
        Ok(parse_handler_result(&stdout.bytes))
    } else {
        let exit_code = status.code();
        let reason = handler_failure_reason(exit_code);
        if exit_code.is_some_and(|code| spec.unrecoverable_exit_codes.contains(&code)) {
            Err(BullError::Unrecoverable(reason))
        } else {
            Err(BullError::ProcessingError(reason))
        }
    }
}

struct BoundedOutput {
    bytes: Vec<u8>,
    exceeded_limit: bool,
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
) -> std::io::Result<BoundedOutput> {
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0_u8; 8192];
    let mut exceeded_limit = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        let retained = read.min(remaining);
        bytes.extend_from_slice(&buffer[..retained]);
        exceeded_limit |= retained < read;
    }
    Ok(BoundedOutput {
        bytes,
        exceeded_limit,
    })
}

struct ProcessGroupGuard {
    #[cfg(unix)]
    process_group_id: Option<i32>,
}

impl ProcessGroupGuard {
    fn new(child_id: Option<u32>) -> Self {
        Self {
            #[cfg(unix)]
            process_group_id: child_id.and_then(|id| i32::try_from(id).ok()),
        }
    }

    fn kill(&self) {
        #[cfg(unix)]
        if let Some(process_group_id) = self.process_group_id {
            // Negative PID targets the handler's dedicated Unix process group.
            unsafe {
                libc::kill(-process_group_id, libc::SIGKILL);
            }
        }
    }

    fn kill_remaining_and_disarm(&mut self) {
        self.kill();
        #[cfg(unix)]
        {
            self.process_group_id = None;
        }
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

fn parse_handler_result(stdout: &[u8]) -> Value {
    let text = String::from_utf8_lossy(stdout);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(trimmed.to_owned()))
    }
}

fn handler_failure_reason(exit_code: Option<i32>) -> String {
    exit_code
        .map(|code| format!("exit code {code}"))
        .map(|status| format!("handler failed with {status}"))
        .unwrap_or_else(|| "handler was terminated by signal".to_owned())
}

fn emit_worker_event(
    as_json: bool,
    queue: &str,
    event: &str,
    job_id: Option<&str>,
    detail: Option<&Value>,
) {
    if as_json {
        println!(
            "{}",
            json!({ "event": event, "queue": queue, "id": job_id, "detail": detail })
        );
    } else if let Some(job_id) = job_id {
        eprintln!("{event}: job {job_id} on {queue}");
    } else {
        eprintln!("{event}: {queue}");
    }
}

async fn close_worker(worker: &BullWorker, timeout: Duration) -> Result<()> {
    worker
        .close(duration_millis(timeout, "shutdown timeout")?)
        .await
        .context("failed to close worker")?;
    let interrupted_jobs = worker.active_count().await;
    if interrupted_jobs > 0 {
        bail!(
            "shutdown timeout interrupted {interrupted_jobs} active job(s); BullMQ will recover them as stalled"
        );
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler should install");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn duration_millis(duration: Duration, name: &str) -> Result<u64> {
    u64::try_from(duration.as_millis()).map_err(|_| anyhow!("{name} is too large"))
}

fn parse_positive_duration(value: &str) -> std::result::Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.as_millis() == 0 {
        Err("must be at least 1 millisecond".to_owned())
    } else {
        Ok(duration)
    }
}

fn parse_positive_usize(value: &str) -> std::result::Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if parsed == 0 {
        Err("must be 1 or greater".to_owned())
    } else {
        Ok(parsed)
    }
}

fn parse_positive_u64(value: &str) -> std::result::Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if parsed == 0 {
        Err("must be 1 or greater".to_owned())
    } else {
        Ok(parsed)
    }
}

fn parse_positive_u32(value: &str) -> std::result::Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| "must be a positive integer".to_owned())?;
    if parsed == 0 {
        Err("must be 1 or greater".to_owned())
    } else {
        Ok(parsed)
    }
}

fn read_data(data: Option<String>, data_file: Option<PathBuf>) -> Result<Value> {
    let raw = match (data, data_file) {
        (Some(value), None) if value == "-" => {
            std::io::read_to_string(std::io::stdin()).context("failed to read JSON from stdin")?
        }
        (Some(value), None) => value,
        (None, Some(path)) => std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?,
        (None, None) => return Ok(json!({})),
        (Some(_), Some(_)) => unreachable!("clap prevents conflicting arguments"),
    };
    serde_json::from_str(&raw).context("job data is not valid JSON")
}

fn print_status(status: &Status) {
    println!("Queue:   {}", status.queue);
    println!("Paused:  {}", status.paused);
    println!("Workers: {}", status.workers);
    println!("Jobs:");
    let mut counts: Vec<_> = status.counts.iter().collect();
    counts.sort_by_key(|(state, _)| *state);
    for (state, count) in counts {
        println!("  {state:<18} {count}");
    }
}

fn print_action(as_json: bool, queue: &str, action: &str) {
    if as_json {
        println!("{}", json!({ "queue": queue, "status": action }));
    } else {
        println!("Queue {queue} {action}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_and_tls_redis_urls() {
        assert!(RedisUrl::from_str("redis://localhost:6379").is_ok());
        assert!(RedisUrl::from_str("rediss://example.com:6380").is_ok());
    }

    #[test]
    fn rejects_other_url_schemes() {
        let error = RedisUrl::from_str("https://example.com").unwrap_err();
        assert!(error.contains("redis:// or rediss://"));
    }

    #[test]
    fn defaults_job_data_to_empty_object() {
        assert_eq!(read_data(None, None).unwrap(), json!({}));
    }

    #[test]
    fn parses_inline_job_data() {
        assert_eq!(
            read_data(Some(r#"{"answer":42}"#.into()), None).unwrap(),
            json!({ "answer": 42 })
        );
    }
}
