//! Where readings come from. Both sources are optional.
//!
//! - `mqtt`: devices that report themselves, above all battery devices. They sleep and cannot be
//!   asked.
//! - `http`: mains-powered devices, asked in the background. Needs no change on the device.

pub mod http;
pub mod mqtt;
pub mod parse;
