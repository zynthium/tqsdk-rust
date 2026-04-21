#![cfg_attr(not(test), forbid(unsafe_code))]

#[doc(hidden)]
pub mod api;
#[doc(hidden)]
pub mod builder;
#[doc(hidden)]
pub mod change;
#[doc(hidden)]
pub mod driver;
#[doc(hidden)]
pub mod error;
#[doc(hidden)]
pub mod refs;
#[doc(hidden)]
pub mod views;

pub use api::TqApi;
pub use builder::TqApiBuilder;
pub use change::ChangeTrackedRef;
pub use error::{Result, WaitFacadeError};
pub use refs::{
    AccountRef, KlineSerialRef, OrderRef, PositionRef, QuoteRef, TickSerialRef, TradeRef,
    TradingStatusRef,
};
pub use views::{KlineWindow, TickWindow};
