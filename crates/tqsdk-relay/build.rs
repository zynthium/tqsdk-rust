use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let dashboard_ui_dir = manifest_dir.join("dashboard-ui");
    let dist_dir = dashboard_ui_dir.join("dist");

    // Tell cargo to re-run this script only if frontend files change
    println!("cargo:rerun-if-changed=dashboard-ui/src");
    println!("cargo:rerun-if-changed=dashboard-ui/package.json");
    println!("cargo:rerun-if-changed=dashboard-ui/pnpm-lock.yaml");
    println!("cargo:rerun-if-changed=dashboard-ui/tsconfig.json");
    println!("cargo:rerun-if-changed=dashboard-ui/vite.config.ts");
    println!("cargo:rerun-if-changed=dashboard-ui/index.html");

    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    // Only run build, assume dependencies are already installed.
    // Running `npm install` in a `pnpm` workspace can cause infinite hangs.
    let build_status = Command::new(npm)
        .arg("run")
        .arg("build")
        .current_dir(&dashboard_ui_dir)
        .status();

    match build_status {
        Ok(status) if status.success() => {}
        Ok(status) => {
            println!("cargo:warning=Failed to build dashboard-ui with npm run build");
            println!("cargo:warning=dashboard-ui build exited with status {status}");
            ensure_fallback_dist(&dist_dir);
        }
        Err(error) => {
            println!("cargo:warning=Failed to execute npm run build: {error}");
            ensure_fallback_dist(&dist_dir);
        }
    }

    if !dist_dir.join("index.html").exists() {
        println!("cargo:warning=dashboard-ui/dist/index.html is missing after build");
        ensure_fallback_dist(&dist_dir);
    }
}

fn ensure_fallback_dist(dist_dir: &Path) {
    let assets_dir = dist_dir.join("assets");
    fs::create_dir_all(&assets_dir).unwrap_or_else(|error| {
        panic!(
            "failed to create fallback dashboard dist at {}: {error}",
            assets_dir.display()
        )
    });

    write_if_missing(
        &dist_dir.join("index.html"),
        concat!(
            "<!doctype html><html><head><meta charset=\"UTF-8\" />",
            "<title>\u{4e2d}\u{7ee7}\u{884c}\u{60c5}\u{76d1}\u{63a7}\u{4e2d}\u{5fc3}</title>",
            "<script type=\"module\" crossorigin src=\"/dashboard/assets/app.js\"></script>",
            "<link rel=\"stylesheet\" crossorigin href=\"/dashboard/assets/app.css\">",
            "</head><body><div id=\"app\">Dashboard assets unavailable</div></body></html>\n",
        ),
    );
    write_if_missing(
        &assets_dir.join("app.js"),
        "const dashboardSnapshot = '/dashboard-snapshot';\n\
const expectedFields = ['instrument_name', 'closed', 'upstream_stage'];\n\
console.info('fallback dashboard assets', dashboardSnapshot, expectedFields);\n",
    );
    write_if_missing(
        &assets_dir.join("app.css"),
        ":root { --relay-bg: #07111c; }\n\
body { margin: 0; background: var(--relay-bg); color: #c3dbe6; }\n",
    );
}

fn write_if_missing(path: &Path, contents: &str) {
    if path.exists() {
        return;
    }
    fs::write(path, contents)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
}
