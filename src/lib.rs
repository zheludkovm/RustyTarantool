//! Tarantool async tokio based client.
//!
//! main features : auth by login and password, auto reconnect, optional timeout
//!
//! supported tarantool api :
//! - call function
//! - select
//! - insert
//! - replace
//! - update
//! - upsert
//! - delete
//! - eval
//!
//! # Examples
//!
//! ```text
//!
//! let client = ClientConfig::new("127.0.0.1:3301", "rust", "rust").set_timeout_time_ms(1000).build();
//! let response = client.call_fn2("test", &("param11", "param12") , &2).await?;
//! let res : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
//! ```
//!

#[macro_use]
extern crate log;
extern crate env_logger;

pub mod tarantool;
