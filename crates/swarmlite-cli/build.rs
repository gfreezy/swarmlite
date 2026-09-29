use std::{env, fs, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=SWARMLITE_UI_DIST");
    let manifest = env::var_os("CARGO_MANIFEST_DIR").unwrap();
    let ui = Path::new(&manifest).join("../../ui");
    let output = std::path::PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let dist = if let Some(dist) = env::var_os("SWARMLITE_UI_DIST") {
        let dist =
            fs::canonicalize(dist).expect("SWARMLITE_UI_DIST must point to a built UI directory");
        println!("cargo:rerun-if-changed={}", dist.display());
        dist
    } else {
        for path in [
            "src",
            "public",
            "index.html",
            "package.json",
            "package-lock.json",
            "tsconfig.json",
            "tsconfig.app.json",
            "tsconfig.node.json",
            "vite.config.ts",
        ] {
            println!("cargo:rerun-if-changed={}", ui.join(path).display());
        }
        let dist = output.join("ui-dist");
        run_npm(&ui, &["ci", "--no-audit", "--no-fund"]);
        run_npm(
            &ui,
            &[
                "run",
                "build",
                "--",
                "--outDir",
                dist.to_str().unwrap(),
                "--emptyOutDir",
            ],
        );
        dist
    };
    assert!(
        dist.join("index.html").is_file(),
        "UI build is missing index.html"
    );
    let mut assets = Vec::new();
    collect(&dist, &dist, &mut assets);
    assets.sort();
    let mut source = String::from("static ASSETS: &[(&str, &[u8])] = &[\n");
    for (url, path) in assets {
        source.push_str(&format!("({url:?}, include_bytes!({path:?})),\n"));
    }
    source.push_str("];\n");
    fs::write(output.join("ui_assets.rs"), source).expect("write embedded UI asset index");
}

fn run_npm(directory: &Path, args: &[&str]) {
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    let result = Command::new(npm).args(args).current_dir(directory).output()
        .expect("building the UI requires Node.js and npm; alternatively set SWARMLITE_UI_DIST to prebuilt assets");
    assert!(
        result.status.success(),
        "npm {} failed:\n{}\n{}",
        args.join(" "),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

fn collect(root: &Path, directory: &Path, assets: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(directory).expect("read UI assets") {
        let path = entry.expect("read UI asset").path();
        if path.is_dir() {
            collect(root, &path, assets);
        } else if path.is_file() {
            let url = format!(
                "/{}",
                path.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            );
            assets.push((url, path.to_str().expect("UTF-8 asset path").to_owned()));
        }
    }
}
