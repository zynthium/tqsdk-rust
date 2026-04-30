#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(u64);

impl CommandId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CursorId(u64);

impl CursorId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(Symbol);
string_id!(AccountId);
string_id!(OrderId);
string_id!(TradeId);
string_id!(QueryId);
string_id!(SchemaId);
string_id!(ReplaySessionId);
string_id!(AuthId);
string_id!(ChartId);
string_id!(NotificationId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolDomain {
    System,
    Market,
    Trade,
    Replay,
    Query,
    Schema,
}

impl ProtocolDomain {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Market => "market",
            Self::Trade => "trade",
            Self::Replay => "replay",
            Self::Query => "query",
            Self::Schema => "schema",
        }
    }
}
