use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use bullmq::options::RedisConnectionOptions;
use bullmq::{Queue, QueueOptions};
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::{Value, json};

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
    },

    /// Pause a queue globally.
    Pause { queue: String },

    /// Resume a paused queue.
    Resume { queue: String },
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

    let queue_name = match &cli.command {
        Command::Status { queue }
        | Command::Add { queue, .. }
        | Command::Pause { queue }
        | Command::Resume { queue } => queue,
    };
    let queue = connect(queue_name, &cli.redis_url, &cli.prefix).await?;

    match cli.command {
        Command::Status { queue: queue_name } => {
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
        }
        Command::Add {
            queue: queue_name,
            name,
            data,
            data_file,
        } => {
            let data = read_data(data, data_file)?;
            let job = queue
                .add(&name, data)
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
        }
        Command::Pause { queue: queue_name } => {
            queue
                .pause()
                .await
                .with_context(|| format!("failed to pause queue '{queue_name}'"))?;
            print_action(cli.json, &queue_name, "paused");
        }
        Command::Resume { queue: queue_name } => {
            queue
                .resume()
                .await
                .with_context(|| format!("failed to resume queue '{queue_name}'"))?;
            print_action(cli.json, &queue_name, "resumed");
        }
    }

    queue.close().await;
    Ok(())
}

async fn connect(name: &str, redis_url: &RedisUrl, prefix: &str) -> Result<Queue> {
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
