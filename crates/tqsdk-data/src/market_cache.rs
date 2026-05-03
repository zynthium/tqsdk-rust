#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Quote, Tick};

use crate::{DataError, Result};

mod io {
    use super::*;
    include!("market_cache/io.rs");
}

use io::write_market_cache_event_line;

mod event {
    use super::*;
    include!("market_cache/event.rs");
}

pub use event::{
    MarketCacheEvent, MarketCachePayload, MarketCachePayloadKind, MarketCacheReader,
    MarketCacheReplay, MarketCacheWriter,
};
