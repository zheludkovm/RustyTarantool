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

use rusty_tarantool::tarantool::{Client};
use tokio_core::reactor::Core;
use futures::{Future};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn main() {
    println!("Simple client run!");
    env_logger::init();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = "127.0.0.1:3301".parse().unwrap();
    let client_f = Client::connect(&addr,"rust","rust", &handle);

    let client = core.run(client_f).unwrap();
    let start = Instant::now();
    let count = 1000000;
    

    for x in 0..count {
        let resp = client.call_fn("test", &(("aa", "aa"), x))
            .and_then(move |mut response| {
                let s: (Vec<String>, Vec<u64>, Vec<Option<u64>>) = response.decode()?;
                let v = COUNTER.fetch_add(1, Ordering::SeqCst);
                if v==count-1 {
                    println!("All finished res={:?}", s);
                    let elapsed = start.elapsed();
                    // debug format:
                    println!("{:?}", elapsed);
                    std::process::exit(0);
                }
                Ok(())
            })
            .map_err(|_e| ())
        ;
        handle.spawn(resp);
    };

    core.run(futures::future::empty::<(), ()>()).unwrap();
}
