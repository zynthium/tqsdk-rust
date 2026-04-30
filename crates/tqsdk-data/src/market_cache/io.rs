pub(super) fn write_market_cache_event_line<W: Write>(
    writer: &mut W,
    event: &MarketCacheEvent,
) -> Result<()> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    Ok(())
}

pub(super) fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

pub(super) fn system_time_ns() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DataError::InvalidState("system clock is before unix epoch"))?;
    i64::try_from(elapsed.as_nanos())
        .map_err(|_| DataError::InvalidState("system clock nanoseconds overflow i64"))
}
