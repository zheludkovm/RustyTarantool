use rusty_tarantool::tarantool::ClientConfig;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Connect to tarantool and call simple stored procedure!");
    let client = ClientConfig::new("127.0.0.1:3301", "rust", "rust")
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build();

    let response = client.call_fn2("test", &("param11", "param12") , &2).await?;
    let res : ((String,String), u64, Option<u64>) = response.decode_trio()?;
    println!("stored procedure response ={:?}", res);
    Ok(())
}
