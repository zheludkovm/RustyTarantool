extern crate rusty_tarantool;

extern crate bytes;
extern crate futures;
extern crate tokio;
extern crate tokio_codec;

extern crate env_logger;
extern crate rmp;
extern crate rmp_serde;
extern crate rmpv;
extern crate serde;

use rusty_tarantool::tarantool::codec::TarantoolCodec;
use rusty_tarantool::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest};

use tokio::net::TcpStream;
use tokio_codec::Decoder;

use futures::{Future, Sink, Stream};

#[test]
fn test() {
    env_logger::init();

    //    let mut core = Core::new().unwrap();
    //    let handle = core.handle();

    let tcp_stream = TcpStream::connect(&"127.0.0.1:3301".parse().unwrap());
    //    let tcp = TcpStream::from_stream(tcp_stream, &handle);

    // Once we connect, send a `Handshake` with our name.
    let test_task = tcp_stream
        .and_then(|stream| {
            println!("stream={:?}", stream);
            let framed_io = TarantoolCodec::new().framed(stream);
            framed_io
                .into_future()
                .map_err(|e| e.0)
                .and_then(|(r, framed_io)| {
                    println!("Received first packet {:?}", r);
                    framed_io.send((
                        2,
                        TarantoolRequest::Auth(AuthPacket {
                            login: String::from("rust"),
                            password: String::from("rust"),
                        }),
                    ))
                })
                .and_then(|framed_io| framed_io.into_future().map_err(|e| e.0))
                .and_then(|(r, framed_io)| {
                    println!("Received auth answer packet {:?}", r);
                    framed_io.send((
                        2,
                        TarantoolRequest::Command(
                            CommandPacket::call("test", &(("aa", "aa"), 1)).unwrap(),
                        ),
                    ))
                })
                .and_then(|framed_io| framed_io.into_future().map_err(|e| e.0))
                .and_then(|(resp, _framed_io)| {
                    println!("Received test result packet {:?}", resp);
                    if let Some((_id, resp_packet)) = resp {
                        let s: (Vec<String>, Vec<u64>) = resp_packet?.decode()?;
                        println!("resp value={:?}", s);
                    }
                    Ok(())
                })
        })
        //        .map(|res| ());
        .map_err(|e| {
            println!("error={:?}", e);
        });

    tokio::run(test_task);
}
