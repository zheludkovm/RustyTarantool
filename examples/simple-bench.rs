use rusty_tarantool::tarantool::ClientConfig;

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Simple client run!");

    let client = ClientConfig::new("127.0.0.1:3301", "rust", "rust")
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build();
    let start = Instant::now();
    let count = 100000;

    for x in 0..count {
        let response = client.call_fn("test", &(("aa", "aa"), x)).await?;
        let s: (Vec<String>, u64, Option<u64>) = response.decode()?;
        let v = COUNTER.fetch_add(1, Ordering::SeqCst);
        if v == count - 1 {
            println!("All finished res={:?}", s);
            let elapsed = start.elapsed();
            // debug format:
            println!("{:?}", elapsed);
            std::process::exit(0);
        }
    }

    Ok(())
}
