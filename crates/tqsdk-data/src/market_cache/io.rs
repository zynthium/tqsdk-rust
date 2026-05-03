pub(super) fn write_market_cache_event_line<W: Write>(
    writer: &mut W,
    event: &MarketCacheEvent,
) -> Result<()> {
    serde_json::to_writer(&mut *writer, event)?;
    writer.write_all(b"\n")?;
    Ok(())
}
