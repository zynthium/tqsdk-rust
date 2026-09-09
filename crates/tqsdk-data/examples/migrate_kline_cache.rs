//! Offline, resumable canonical Kline migration. No credentials or network.
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: migrate_kline_cache ROOT BACKUP [--apply]")?,
    );
    let backup = PathBuf::from(args.next().ok_or("missing backup directory")?);
    let apply = match args.next().as_deref() {
        None => false,
        Some("--apply") => true,
        Some(_) => return Err("expected --apply".into()),
    };
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let report = tqsdk_data::migrate_kline_cache(root, backup, apply)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
