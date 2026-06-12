use std::env;
use std::process::Command;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let dashboard_ui_dir = format!("{}/dashboard-ui", manifest_dir);

    // Tell cargo to re-run this script only if frontend files change
    println!("cargo:rerun-if-changed=dashboard-ui/src");
    println!("cargo:rerun-if-changed=dashboard-ui/package.json");
    println!("cargo:rerun-if-changed=dashboard-ui/package-lock.json");
    println!("cargo:rerun-if-changed=dashboard-ui/tsconfig.json");
    println!("cargo:rerun-if-changed=dashboard-ui/vite.config.ts");
    println!("cargo:rerun-if-changed=dashboard-ui/index.html");

    // Only run npm build in release mode, or if the dist folder doesn't exist
    // In debug mode, if developers want hot reload, they should use npm run dev
    // However, the dashboard.rs still needs *some* dashboard-dist to compile.
    // If we require npm to be installed for *any* cargo run, we can just run it.
    
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    // Only run build, assume dependencies are already installed.
    // Running `npm install` in a `pnpm` workspace can cause infinite hangs.
    let build_status = Command::new(npm)
        .arg("run")
        .arg("build")
        .current_dir(&dashboard_ui_dir)
        .status();
        
    if let Ok(b_status) = build_status {
        if !b_status.success() {
            println!("cargo:warning=Failed to build dashboard-ui with npm run build");
        }
    } else {
        println!("cargo:warning=Failed to execute npm run build. Is npm in PATH?");
    }
}
