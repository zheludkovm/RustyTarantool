extern crate rusty_tarantool;

extern crate bytes;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_service;
extern crate futures;

extern crate rmpv;
extern crate rmp_serde;
extern crate serde;
extern crate rmp;
extern crate env_logger;

use rusty_tarantool::tarantool::{ClientFactory};
use tokio_core::reactor::Core;
use futures::{Future};


fn main() {
    println!("Connect to tarantool and call simple stored procedure!");
    env_logger::init();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = "127.0.0.1:3301".parse().unwrap();
    let client_factory = ClientFactory::new(addr, "rust", "rust", handle);

    let response_future = client_factory.get_connection()
        .and_then(|client| {
            client.call_fn2("test", &("param11", "param12") , &2)
        })
        .and_then(|mut response| {
            let (value1, value2, value3) : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
            Ok((value1, value2, value3))
        }) ;
    match core.run(response_future) {
        Err(e) => println!("err={:?}", e),
        Ok(res) => println!("stored procedure response ={:?}", res)
    }
}
