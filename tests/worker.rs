use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assert_cmd::Command;
use predicates::prelude::*;

struct RedisHarness {
    url: String,
    child: Option<Child>,
}

impl RedisHarness {
    fn start() -> Self {
        if let Ok(url) = std::env::var("KUEUESKI_TEST_REDIS_URL") {
            return Self { url, child: None };
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve a Redis test port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = ProcessCommand::new("redis-server")
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("redis-server is required for worker integration tests");

        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "Redis did not start in time");
            thread::sleep(Duration::from_millis(25));
        }

        Self {
            url: format!("redis://127.0.0.1:{port}"),
            child: Some(child),
        }
    }
}

impl Drop for RedisHarness {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn dequeue_executes_one_job_and_leaves_retries_to_bullmq() {
    let redis = RedisHarness::start();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let queue = format!("dequeue-test-{}-{unique}", std::process::id());

    cli(&redis)
        .args([
            "add",
            &queue,
            "calculate",
            "--attempts",
            "2",
            "--data",
            r#"{"answer":42}"#,
        ])
        .assert()
        .success();

    cli(&redis)
        .args([
            "--json",
            "dequeue",
            &queue,
            "--",
            "sh",
            "-c",
            "printf 'try again\\n' >&2; exit 9",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("try again"));

    cli(&redis)
        .args(["--json", "status", &queue])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""waiting":1"#))
        .stdout(predicate::str::contains(r#""failed":0"#));

    cli(&redis)
        .args([
            "--json",
            "dequeue",
            &queue,
            "--",
            "sh",
            "-c",
            r#"read payload; test "$KUEUESKI_JOB_NAME" = calculate; printf '{"received":%s}' "$payload""#,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""event":"completed""#))
        .stdout(predicate::str::contains(r#""received":{"answer":42}"#));

    cli(&redis)
        .args(["--json", "status", &queue])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""completed":1"#))
        .stdout(predicate::str::contains(r#""waiting":0"#));
}

#[test]
fn dequeue_claims_exactly_one_job() {
    let redis = RedisHarness::start();
    let queue = unique_queue("one-job");
    add_job(&redis, &queue, "first", None);
    add_job(&redis, &queue, "second", None);

    cli(&redis)
        .args(["dequeue", &queue, "--", "true"])
        .assert()
        .success();

    cli(&redis)
        .args(["--json", "status", &queue])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""completed":1"#))
        .stdout(predicate::str::contains(r#""waiting":1"#));
}

#[test]
fn configured_exit_codes_skip_retries() {
    let redis = RedisHarness::start();
    let queue = unique_queue("unrecoverable");
    add_job(&redis, &queue, "fatal", Some(3));

    cli(&redis)
        .args([
            "dequeue",
            &queue,
            "--unrecoverable-exit-code",
            "9",
            "--",
            "sh",
            "-c",
            "exit 9",
        ])
        .assert()
        .failure();

    cli(&redis)
        .args(["--json", "status", &queue])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""failed":1"#))
        .stdout(predicate::str::contains(r#""waiting":0"#));
}

#[test]
fn handler_result_output_is_bounded() {
    let redis = RedisHarness::start();
    let queue = unique_queue("bounded-output");
    add_job(&redis, &queue, "verbose", None);

    cli(&redis)
        .args([
            "dequeue",
            &queue,
            "--max-result-bytes",
            "4",
            "--",
            "printf",
            "12345",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("exceeded the 4 byte result limit"));
}

#[cfg(unix)]
#[test]
fn worker_processes_concurrently_and_stops_on_sigterm() {
    let redis = RedisHarness::start();
    let queue = unique_queue("continuous");
    add_job(&redis, &queue, "first", None);
    add_job(&redis, &queue, "second", None);
    add_job(&redis, &queue, "third", None);
    let marker = marker_path("continuous");
    let script = format!(
        "read payload; printf '%s\\n' \"$KUEUESKI_JOB_ID\" >> {}; sleep 0.2; printf '{{\"ok\":true}}'",
        marker.display()
    );

    let mut command = process_cli(&redis);
    command.args([
        "worker",
        &queue,
        "--concurrency",
        "2",
        "--",
        "sh",
        "-c",
        &script,
    ]);
    let mut child = command.spawn().expect("start worker CLI");
    wait_for(Duration::from_secs(5), || {
        std::fs::read_to_string(&marker)
            .map(|contents| contents.lines().count() == 2)
            .unwrap_or(false)
    });

    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    assert!(child.wait().unwrap().success());
    assert_eq!(queue_count(&redis, &queue, "completed"), 2);
    assert_eq!(queue_count(&redis, &queue, "waiting"), 1);
    let _ = std::fs::remove_file(marker);
}

#[cfg(unix)]
#[test]
fn shutdown_timeout_kills_the_handler_process_group() {
    let redis = RedisHarness::start();
    let queue = unique_queue("shutdown");
    add_job(&redis, &queue, "slow", None);
    let marker = marker_path("descendant");
    let script = format!("(sleep 1; touch {}) & wait", marker.display());

    let mut command = process_cli(&redis);
    command.args([
        "worker",
        &queue,
        "--shutdown-timeout",
        "100ms",
        "--",
        "sh",
        "-c",
        &script,
    ]);
    let mut child = command.spawn().expect("start worker CLI");
    wait_for(Duration::from_secs(5), || {
        queue_count(&redis, &queue, "active") == 1
    });

    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    assert!(!child.wait().unwrap().success());
    thread::sleep(Duration::from_millis(1200));
    assert!(
        !marker.exists(),
        "a handler descendant survived the shutdown timeout"
    );
}

fn cli(redis: &RedisHarness) -> Command {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command.args(["--redis-url", &redis.url]);
    command
}

fn process_cli(redis: &RedisHarness) -> ProcessCommand {
    let mut command = ProcessCommand::new(assert_cmd::cargo::cargo_bin!("kueueski"));
    command.args(["--redis-url", &redis.url]);
    command
}

fn unique_queue(label: &str) -> String {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{label}-{}-{unique}", std::process::id())
}

fn add_job(redis: &RedisHarness, queue: &str, name: &str, attempts: Option<u32>) {
    let mut command = cli(redis);
    command.args(["add", queue, name, "--data", r#"{"answer":42}"#]);
    if let Some(attempts) = attempts {
        command.args(["--attempts", &attempts.to_string()]);
    }
    command.assert().success();
}

fn queue_count(redis: &RedisHarness, queue: &str, state: &str) -> u64 {
    let output = cli(redis)
        .args(["--json", "status", queue])
        .output()
        .expect("read queue status");
    let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    status["counts"][state].as_u64().unwrap()
}

fn wait_for(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not met in time");
        thread::sleep(Duration::from_millis(25));
    }
}

fn marker_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "kueueski-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
