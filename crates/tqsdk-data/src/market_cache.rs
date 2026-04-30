#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Lines, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tqsdk_core::{Kline, Quote, Tick};

use crate::{DataError, Result};

mod io {
    use super::*;
    include!("market_cache/io.rs");
}

use io::{path_with_suffix, system_time_ns, write_market_cache_event_line};

mod event {
    use super::*;
    include!("market_cache/event.rs");
}

pub use event::{
    MarketCacheEvent, MarketCachePayload, MarketCachePayloadKind, MarketCacheReader,
    MarketCacheReplay, MarketCacheWriter,
};

mod index {
    use super::*;
    include!("market_cache/index.rs");
}

pub use index::{MarketCacheIndex, MarketCacheIndexEntry, MarketCacheIndexKey};

mod checkpoint {
    use super::*;
    include!("market_cache/checkpoint.rs");
}

pub use checkpoint::{
    MarketCacheReaderCheckpoint, MarketCacheReaderLag, MarketCacheReaderManifest,
};

mod recovery {
    use super::*;
    include!("market_cache/recovery.rs");
}

pub use recovery::{
    MarketCacheRecoveryFileKind, MarketCacheRecoveryFileReport, MarketCacheRecoveryReport,
    MarketCacheRecoveryScan,
};

mod lock {
    use super::*;
    include!("market_cache/lock.rs");
}

pub use lock::{MarketCacheLock, MarketCacheLockOptions};
use lock::{create_lock_file, lock_file_is_stale};

mod election {
    use super::*;
    include!("market_cache/election.rs");
}

pub use election::{
    MarketCacheWriterElection, MarketCacheWriterElectionOutcome, MarketCacheWriterElectionReport,
    MarketCacheWriterElectionStatus, MarketCacheWriterLease,
};

mod queue {
    use super::*;
    include!("market_cache/queue.rs");
}

pub use queue::{
    MarketCacheQueue, MarketCacheQueueDrainError, MarketCacheQueueDrainReport,
    MarketCacheRecoveryAction, MarketCacheRecoveryActionReport,
};

mod compaction {
    use super::*;
    include!("market_cache/compaction.rs");
}

pub use compaction::{
    MarketCacheAtomicCompactionReport, MarketCacheCompaction, MarketCacheCompactionOwnership,
    MarketCacheCompactionOwnershipReport, MarketCacheCompactionReport,
};

mod service {
    use super::*;
    include!("market_cache/service.rs");
}

pub use service::{
    MarketCacheService, MarketCacheServiceConfig, MarketCacheServiceOpen,
    MarketCacheServiceOpenReport, MarketCacheServiceShutdownReport,
};

mod daemon {
    use super::*;
    include!("market_cache/daemon.rs");
}

pub use daemon::{
    MarketCacheDaemon, MarketCacheDaemonConfig, MarketCacheDaemonShutdownReport,
    MarketCacheSupervisor, MarketCacheSupervisorConfig, MarketCacheSupervisorShutdownReport,
};
