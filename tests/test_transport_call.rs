extern crate rusty_tarantool;

extern crate bytes;
extern crate tokio;

extern crate env_logger;
extern crate rmp;
extern crate rmp_serde;
extern crate rmpv;
extern crate serde;

use rusty_tarantool::tarantool::codec::TarantoolCodec;
use rusty_tarantool::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest};

use futures::SinkExt;
use std::io;
use tokio::net::TcpStream;
use tokio_util::codec::Decoder;

use crate::tokio::stream::StreamExt;

#[tokio::test]
async fn test() -> io::Result<()> {
    env_logger::init();

    //    let mut core = Core::new().unwrap();
    //    let handle = core.handle();
    println!("try connect !");
    let tcp_stream = TcpStream::connect("127.0.0.1:3301").await?;
    //    let tcp = TcpStream::from_stream(tcp_stream, &handle);

    // Once we connect, send a `Handshake` with our name.
    println!("stream={:?}", tcp_stream);
    let mut framed_io = TarantoolCodec::new().framed(tcp_stream);
    let first_response = framed_io.next().await;
    println!("Received first packet {:?}", first_response);
    framed_io
        .send((
            2,
            TarantoolRequest::Auth(AuthPacket {
                login: String::from("rust"),
                password: String::from("rust"),
            }),
        ))
        .await?;
    let auth_response = framed_io.next().await;
    println!("Received auth packet {:?}", auth_response);
    framed_io
        .send((
            2,
            TarantoolRequest::Command(CommandPacket::call("test", &(("aa", "aa"), 1)).unwrap()),
        ))
        .await?;
    let test_call_response = framed_io.next().await;
    println!("Received test result packet {:?}", test_call_response);
    if let Some(Ok((_id, resp_packet))) = test_call_response {
        let s: (Vec<String>, Vec<u64>) = resp_packet?.decode()?;
        println!("resp value={:?}", s);
    }

    Ok(())
}
