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
//! let mut rt = Runtime::new().unwrap();
//!
//! let addr = "127.0.0.1:3301".parse().unwrap();
//! let client = ClientConfig::new(addr, "rust", "rust").set_timeout_time_ms(1000).build();
//!
//! let response_future = client.call_fn2("test", &("param11", "param12") , &2)
//!     .and_then(|response| {
//!         let res : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
//!         Ok(res)
//!     }) ;
//!
//! match rt.block_on(response_future) {
//!     Err(e) => println!("err={:?}", e),
//!     Ok(res) => println!("stored procedure response ={:?}", res)
//! }
//! ```
//!

#[macro_use]
extern crate log;
extern crate env_logger;

pub mod tarantool;
