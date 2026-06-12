#![cfg_attr(not(test), forbid(unsafe_code))]

use include_dir::{Dir, include_dir};

static DASHBOARD_DIST: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/dashboard-ui/dist");

#[derive(Debug, Clone, Copy)]
pub struct DashboardAsset<'a> {
    pub content_type: &'static str,
    pub bytes: &'a [u8],
}

pub fn dashboard_asset(path: &str) -> Option<DashboardAsset<'static>> {
    let path = normalize_dashboard_path(path)?;
    let file = DASHBOARD_DIST.get_file(path)?;
    Some(DashboardAsset {
        content_type: content_type(path),
        bytes: file.contents(),
    })
}

fn normalize_dashboard_path(path: &str) -> Option<&str> {
    let clean = path.strip_prefix('/').unwrap_or(path);
    let clean = clean.strip_prefix("dashboard").unwrap_or(clean);
    let clean = clean.strip_prefix('/').unwrap_or(clean);
    if clean.is_empty() {
        return Some("index.html");
    }
    if clean.contains("..") || clean.starts_with('/') {
        return None;
    }
    Some(clean)
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        _ => "application/octet-stream",
    }
}
