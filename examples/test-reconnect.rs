use futures::{Future, Stream};
use rusty_tarantool::tarantool::packets::CommandPacket;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::timer::Interval;

fn main() {
    env_logger::init();

    let addr: SocketAddr = "127.0.0.1:3301".parse().unwrap();

    println!("start client!");

    let space_id: i32 = 1000;
    let tuple_replace = (3, "test_insert", "replace");

    let command = CommandPacket::replace(space_id, &tuple_replace).unwrap();

    //we create client without variable - if we don't, awaiting task wil be forever
    let print_task = rusty_tarantool::tarantool::ClientConfig::new(addr, "rust", "rust")
        .set_reconnect_time_ms(2000)
        .build()
        .send_command(command)
        .map(|resp| println!("response! {:?}", resp))
        .map_err(|e| println!("error! {:?}", e));
    println!("spawn single task!");
    tokio::run(print_task);
    println!("finish single task!");

    let tarantool = rusty_tarantool::tarantool::ClientConfig::new(addr, "rust", "rust")
        .set_reconnect_time_ms(2000)
        .set_timeout_time_ms(1000)
        .build();

    let task = Interval::new(Instant::now(), Duration::from_millis(1000))
        .for_each(move |instant| {
            println!("fire; instant={:?}", instant);
            let print_task = tarantool
                .replace(space_id, &tuple_replace)
                .map(|resp| println!("response! {:?}", resp))
                .map_err(|e| println!("error! {:?}", e));
            tokio::spawn(print_task);
            Ok(())
        })
        .map_err(|e| panic!("interval errored; err={:?}", e));

    tokio::run(task);
    println!("finish!");
}
