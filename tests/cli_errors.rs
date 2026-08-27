use std::process::Command;

#[test]
fn errors_do_not_print_backtraces_when_backtraces_are_enabled() {
    let data_dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_swarmlite"))
        .args([
            "--data-dir",
            data_dir.path().to_str().unwrap(),
            "connection-info",
        ])
        .env("RUST_BACKTRACE", "1")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.starts_with("Error: "), "unexpected stderr: {stderr}");
    assert!(
        stderr.contains("this node is not initialized"),
        "missing error cause: {stderr}"
    );
    assert!(
        !stderr.contains("Stack backtrace:"),
        "unexpected backtrace: {stderr}"
    );
}
