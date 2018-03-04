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


use rusty_tarantool::tarantool::codec::TarantoolCodec;
use rusty_tarantool::tarantool::packets::{TarantoolRequest, AuthPacket, CommandPacket};

use tokio_io::codec::Framed;
use tokio_io::AsyncRead;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

use futures::{Stream, Sink};
use std::net;

#[test]
fn test() {
    env_logger::init();


    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let tcp_stream = net::TcpStream::connect("127.0.0.1:3301").unwrap();
    let tcp = TcpStream::from_stream(tcp_stream, &handle);

    // Once we connect, send a `Handshake` with our name.
    let _handshake = tcp.and_then(|stream| {
        println!("stream={:?}", stream);
        let framed_io = AsyncRead::framed(stream, TarantoolCodec::new());

        let (resp, framed_io) = core.run(framed_io.into_future()).unwrap();

        let framed_io: Framed<TcpStream, TarantoolCodec> = framed_io;
        println!("resp={:?}", resp);
        println!("framed_io={:?}", framed_io);

        let send_future = framed_io.send((2, TarantoolRequest::Auth(AuthPacket {
            login: String::from("rust"),
            password: String::from("rust"),
        })));

        let framed_io = core.run(send_future).unwrap();

        //receive response for auth
        let (resp, framed_io) = core.run(framed_io.into_future()).unwrap();
        println!("resp={:?}", resp);

        let send_future = framed_io.send(
            (2, TarantoolRequest::Command(
                CommandPacket::call("test",
                                    &(("aa","aa"), 1))?
            )
            )
        );

        let framed_io = core.run(send_future).unwrap();

        //receive response for auth
        let (resp, _framed_io) = core.run(framed_io.into_future()).unwrap();
        println!("resp={:?}", resp);
        if let Some((_id, resp_packet)) = resp {
            let s:(Vec<String>, Vec<u64>) = resp_packet?.decode()?;
            println!("resp value={:?}", s);
        }


        Ok(())
    }).unwrap();
}
