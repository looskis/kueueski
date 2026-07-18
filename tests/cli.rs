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
        .stdout(predicate::str::contains("pause"))
        .stdout(predicate::str::contains("resume"));
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
