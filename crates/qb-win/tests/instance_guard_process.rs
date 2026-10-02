use std::{
    env,
    fs,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use qb_win::{InstanceGuard, PlatformError, RuntimeRoot};

const HELPER_ENV: &str = "QBCTL_INSTANCE_GUARD_HELPER";
const ROOT_ENV: &str = "QBCTL_INSTANCE_GUARD_ROOT";
const READY_MARKER: &str = "QBCTL_LOCK_HELD";

fn unique_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();

    env::temp_dir().join(format!(
        "qbctl-instance-guard-{}-{nanos}",
        std::process::id()
    ))
}

#[test]
fn instance_guard_helper() {
    if env::var_os(HELPER_ENV).is_none() {
        return;
    }

    let path = PathBuf::from(env::var_os(ROOT_ENV).expect("helper root"));
    let root = RuntimeRoot::at(path);
    let _guard = InstanceGuard::acquire(&root).expect("helper acquires runtime ownership");

    println!("{READY_MARKER}");
    std::io::stdout().flush().expect("flush marker");

    thread::sleep(Duration::from_secs(30));
}

#[test]
fn second_process_is_rejected_for_same_runtime_root() {
    let root_path = unique_root();
    fs::create_dir_all(&root_path).expect("create test root");

    let executable = env::current_exe().expect("current integration test executable");
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg("instance_guard_helper")
        .arg("--nocapture")
        .env(HELPER_ENV, "1")
        .env(ROOT_ENV, &root_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn lock helper");

    let stdout = child.stdout.take().expect("helper stdout");
    let mut reader = BufReader::new(stdout);
    let mut ready = false;

    for _ in 0..20 {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).expect("read helper output");
        if bytes == 0 {
            break;
        }
        if line.contains(READY_MARKER) {
            ready = true;
            break;
        }
    }

    assert!(ready, "helper process did not report lock acquisition");

    let root = RuntimeRoot::at(&root_path);
    let second = InstanceGuard::acquire(&root);
    assert!(matches!(second, Err(PlatformError::AlreadyRunning)));

    child.kill().expect("terminate helper");
    let _ = child.wait();
    let _ = fs::remove_dir_all(root_path);
}
