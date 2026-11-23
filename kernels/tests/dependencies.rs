//! A kernel crate depends on `core`/`alloc` and `vstd` only: no tokio, no I/O, no clock.

use std::process::Command;

const ALLOWED: &[&str] = &[
    "glidefs-kernels",
    "vstd",
    "verus_builtin",
    "verus_builtin_macros",
    "verus_state_machines_macros",
    "verus_prettyplease",
    "verus_syn",
    "proc-macro2",
    "quote",
    "syn",
    "synstructure",
    "unicode-ident",
    "convert_case",
    "hashbrown",
    "equivalent",
    "indexmap",
];

#[test]
fn only_kernel_crates_and_vstd() {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = Command::new(cargo)
        .args([
            "tree",
            "-p",
            "glidefs-kernels",
            "-e",
            "normal",
            "--prefix",
            "none",
            "--offline",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo tree");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let tree = String::from_utf8(out.stdout).unwrap();
    let bad: Vec<&str> = tree
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|name| !ALLOWED.contains(name))
        .collect();
    assert!(bad.is_empty(), "unexpected dependencies: {bad:?}");
}
