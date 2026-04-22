use chrono::NaiveDate;
use tqsdk_data::DataClient;

fn parse_end_date() -> Result<Option<NaiveDate>, Box<dyn std::error::Error>> {
    std::env::var("TQ_CONT_END_DATE")
        .ok()
        .map(|value| NaiveDate::parse_from_str(&value, "%Y-%m-%d"))
        .transpose()
        .map_err(Into::into)
}

fn parse_symbols() -> Vec<String> {
    std::env::var("TQ_CONT_SYMBOLS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .filter(|symbols: &Vec<String>| !symbols.is_empty())
        .unwrap_or_else(|| vec!["KQ.m@SHFE.au".to_string()])
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let days = std::env::var("TQ_CONT_DAYS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let end_date = parse_end_date()?;
    let symbols = parse_symbols();
    let symbol_refs: Vec<&str> = symbols.iter().map(String::as_str).collect();

    let rows = DataClient::new()
        .query_his_cont_quotes(&symbol_refs, days, end_date)
        .await?;

    for row in rows {
        print!("{}", row.date);
        for symbol in &symbols {
            let underlying = row.underlyings.get(symbol).cloned().unwrap_or_default();
            print!("\t{}\t{}", symbol, underlying);
        }
        println!();
    }

    Ok(())
}
