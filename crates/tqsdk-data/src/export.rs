#![cfg_attr(not(test), forbid(unsafe_code))]

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tqsdk_core::{Kline, Tick};

use crate::client::{DataClient, KlineDataSeriesRequest, TickDataSeriesRequest};
use crate::error::Result;

const KLINE_CSV_HEADER: &str = "id,datetime,open,high,low,close,volume,open_oi,close_oi,_epoch\n";
const TICK_CSV_HEADER: &str = concat!(
    "id,datetime,last_price,average,highest,lowest,",
    "ask_price1,ask_volume1,bid_price1,bid_volume1,",
    "ask_price2,ask_volume2,bid_price2,bid_volume2,",
    "ask_price3,ask_volume3,bid_price3,bid_volume3,",
    "ask_price4,ask_volume4,bid_price4,bid_volume4,",
    "ask_price5,ask_volume5,bid_price5,bid_volume5,",
    "volume,amount,open_interest,_epoch\n"
);

/// Final summary returned by kline CSV export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineCsvExportSummary {
    pub symbol: String,
    pub duration_ns: i64,
    pub start_datetime_ns: i64,
    pub end_datetime_ns: i64,
    pub rows_written: usize,
    pub pages_written: usize,
}

/// Final summary returned by tick CSV export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickCsvExportSummary {
    pub symbol: String,
    pub start_datetime_ns: i64,
    pub end_datetime_ns: i64,
    pub rows_written: usize,
    pub pages_written: usize,
}

impl DataClient {
    /// Exports a kline range to CSV through the caller-provided async writer.
    pub async fn export_kline_data_csv<W>(
        &self,
        request: KlineDataSeriesRequest,
        writer: &mut W,
    ) -> Result<KlineCsvExportSummary>
    where
        W: AsyncWrite + Unpin,
    {
        if let Some(session) = self.session() {
            self.require_history_download_permission_async(session)
                .await?;
        }
        let mut download = self.kline_data_download(request)?;

        writer.write_all(KLINE_CSV_HEADER.as_bytes()).await?;
        while let Some(page) = download.next_page().await? {
            for row in page.rows() {
                writer
                    .write_all(format_kline_csv_row(row).as_bytes())
                    .await?;
            }
        }
        writer.flush().await?;

        let progress = download.progress();
        Ok(KlineCsvExportSummary {
            symbol: download.symbol().to_string(),
            duration_ns: download.duration_ns(),
            start_datetime_ns: download.start_datetime_ns(),
            end_datetime_ns: download.end_datetime_ns(),
            rows_written: progress.emitted_rows(),
            pages_written: progress.emitted_pages(),
        })
    }

    /// Exports a tick range to CSV through the caller-provided async writer.
    pub async fn export_tick_data_csv<W>(
        &self,
        request: TickDataSeriesRequest,
        writer: &mut W,
    ) -> Result<TickCsvExportSummary>
    where
        W: AsyncWrite + Unpin,
    {
        if let Some(session) = self.session() {
            self.require_history_download_permission_async(session)
                .await?;
        }
        let mut download = self.tick_data_download(request)?;

        writer.write_all(TICK_CSV_HEADER.as_bytes()).await?;
        while let Some(page) = download.next_page().await? {
            for row in page.rows() {
                writer
                    .write_all(format_tick_csv_row(row).as_bytes())
                    .await?;
            }
        }
        writer.flush().await?;

        let progress = download.progress();
        Ok(TickCsvExportSummary {
            symbol: download.symbol().to_string(),
            start_datetime_ns: download.start_datetime_ns(),
            end_datetime_ns: download.end_datetime_ns(),
            rows_written: progress.emitted_rows(),
            pages_written: progress.emitted_pages(),
        })
    }
}

fn format_kline_csv_row(row: &Kline) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{}\n",
        row.id,
        row.datetime,
        row.open,
        row.high,
        row.low,
        row.close,
        row.volume,
        row.open_oi,
        row.close_oi,
        option_i64_csv_cell(row.epoch),
    )
}

fn format_tick_csv_row(row: &Tick) -> String {
    format!(
        concat!(
            "{},{},{},{},{},{},",
            "{},{},{},{},",
            "{},{},{},{},",
            "{},{},{},{},",
            "{},{},{},{},",
            "{},{},{},{},",
            "{},{},{},{}\n"
        ),
        row.id,
        row.datetime,
        row.last_price,
        row.average,
        row.highest,
        row.lowest,
        row.ask_price1,
        row.ask_volume1,
        row.bid_price1,
        row.bid_volume1,
        row.ask_price2,
        row.ask_volume2,
        row.bid_price2,
        row.bid_volume2,
        row.ask_price3,
        row.ask_volume3,
        row.bid_price3,
        row.bid_volume3,
        row.ask_price4,
        row.ask_volume4,
        row.bid_price4,
        row.bid_volume4,
        row.ask_price5,
        row.ask_volume5,
        row.bid_price5,
        row.bid_volume5,
        row.volume,
        row.amount,
        row.open_interest,
        option_i64_csv_cell(row.epoch),
    )
}

fn option_i64_csv_cell(value: Option<i64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;
    use tqsdk_core::{Kline, Tick};

    use super::*;

    #[test]
    fn format_kline_csv_row_omits_missing_epoch() {
        let row = Kline {
            id: 1,
            datetime: 2,
            open: 3.0,
            high: 4.0,
            low: 5.0,
            close: 6.0,
            volume: 7,
            open_oi: 8,
            close_oi: 9,
            epoch: None,
        };

        assert_eq!(format_kline_csv_row(&row), "1,2,3,4,5,6,7,8,9,\n");
    }

    #[test]
    fn format_tick_csv_row_includes_all_columns() {
        let row = Tick {
            id: 1,
            datetime: 2,
            last_price: 3.0,
            average: 4.0,
            highest: 5.0,
            lowest: 6.0,
            ask_price1: 7.0,
            ask_volume1: 8,
            bid_price1: 9.0,
            bid_volume1: 10,
            ask_price2: 11.0,
            ask_volume2: 12,
            bid_price2: 13.0,
            bid_volume2: 14,
            ask_price3: 15.0,
            ask_volume3: 16,
            bid_price3: 17.0,
            bid_volume3: 18,
            ask_price4: 19.0,
            ask_volume4: 20,
            bid_price4: 21.0,
            bid_volume4: 22,
            ask_price5: 23.0,
            ask_volume5: 24,
            bid_price5: 25.0,
            bid_volume5: 26,
            volume: 27,
            amount: 28.0,
            open_interest: 29,
            epoch: Some(30),
        };

        assert_eq!(
            format_tick_csv_row(&row),
            concat!(
                "1,2,3,4,5,6,",
                "7,8,9,10,",
                "11,12,13,14,",
                "15,16,17,18,",
                "19,20,21,22,",
                "23,24,25,26,",
                "27,28,29,30\n"
            )
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn writes_header_and_rows_to_async_writer() {
        let mut writer = MemoryWriter::default();
        writer.write_all(KLINE_CSV_HEADER.as_bytes()).await.unwrap();
        writer
            .write_all(
                format_kline_csv_row(&Kline {
                    id: 1,
                    datetime: 2,
                    open: 3.0,
                    high: 4.0,
                    low: 5.0,
                    close: 6.0,
                    volume: 7,
                    open_oi: 8,
                    close_oi: 9,
                    epoch: Some(10),
                })
                .as_bytes(),
            )
            .await
            .unwrap();
        writer.flush().await.unwrap();

        assert_eq!(
            String::from_utf8(writer.into_inner()).unwrap(),
            "id,datetime,open,high,low,close,volume,open_oi,close_oi,_epoch\n1,2,3,4,5,6,7,8,9,10\n"
        );
    }

    #[derive(Default)]
    struct MemoryWriter {
        buffer: Vec<u8>,
    }

    impl MemoryWriter {
        fn into_inner(self) -> Vec<u8> {
            self.buffer
        }
    }

    impl AsyncWrite for MemoryWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.buffer.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
}
