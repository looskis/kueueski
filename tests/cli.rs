use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn help_describes_core_commands() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("add"))
        .stdout(predicate::str::contains("dequeue"))
        .stdout(predicate::str::contains("worker"))
        .stdout(predicate::str::contains("pause"))
        .stdout(predicate::str::contains("resume"));
}

#[test]
fn dequeue_requires_a_handler_command() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["dequeue", "emails"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("<COMMAND>..."));
}

#[test]
fn worker_rejects_zero_concurrency_before_connecting() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["worker", "emails", "--concurrency", "0", "--", "true"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("1 or greater"));
}

#[test]
fn add_help_exposes_retry_and_scheduling_options() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["add", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--attempts"))
        .stdout(predicate::str::contains("--backoff"))
        .stdout(predicate::str::contains("--delay"))
        .stdout(predicate::str::contains("--priority"));
}

#[test]
fn missing_handler_is_rejected_before_connecting_or_claiming() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args([
            "dequeue",
            "emails",
            "--",
            "kueueski-handler-that-does-not-exist",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("handler executable was not found"));
}

#[test]
fn zero_lock_duration_is_rejected_before_connecting() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["worker", "emails", "--lock-duration", "0s", "--", "true"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 1 millisecond"));
}

#[test]
fn sub_millisecond_lock_duration_is_rejected() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["worker", "emails", "--lock-duration", "1ns", "--", "true"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 1 millisecond"));
}

#[test]
fn lock_renewal_must_happen_before_lock_expiry() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args([
            "worker",
            "emails",
            "--lock-duration",
            "1s",
            "--lock-renew-time",
            "2s",
            "--",
            "true",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "lock renewal interval must be shorter",
        ));
}

#[test]
fn invalid_redis_scheme_fails_before_connecting() {
    let mut command = Command::cargo_bin("kueueski").unwrap();
    command
        .args(["--redis-url", "http://localhost", "status", "test"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("redis:// or rediss://"));
}
