extern crate rusty_tarantool;

extern crate bytes;
extern crate tokio;

extern crate env_logger;
extern crate rmp;
extern crate rmp_serde;
extern crate rmpv;
extern crate serde;

use rusty_tarantool::tarantool::packets::CommandPacket;
use rusty_tarantool::tarantool::{serialize_to_vec_u8, Client, ClientConfig, IteratorType};

use std::io;
use std::sync::Once;

static INIT: Once = Once::new();

static SPACE_ID: i32 = 1000;

fn setup_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

fn init_client() -> Client {
    setup_logger();

    ClientConfig::new("127.0.0.1:3301", "rust", "rust")
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build()
}

#[tokio::test]
async fn test_low_level_call() -> io::Result<()> {
    let client = init_client();
    let response = client
        .send_command(CommandPacket::call("test", &(("aa", "aa"), 1)).unwrap())
        .await?;
    let s: (Vec<String>, u64) = response.decode_pair()?;
    println!("resp value={:?}", s);
    assert_eq!((vec!["aa".to_string(), "aa".to_string()], 1), s);
    Ok(())
}

#[tokio::test]
async fn test_call_fn() -> io::Result<()> {
    let client = init_client();
    let response = client.call_fn("test", &(("aa", "aa"), 1)).await?;
    let s: (Vec<String>, u64) = response.decode_pair()?;
    println!("resp value={:?}", s);
    assert_eq!((vec!["aa".to_string(), "aa".to_string()], 1), s);
    Ok(())
}

#[tokio::test]
async fn test_call_fn_args() -> io::Result<()> {
    let client = init_client();
    let response = client
        .prepare_call_args()
        .add_arg(&("aa", "aa"))?
        .add_arg(&1)?
        .call_fn("test").await?;
    let s: (Vec<String>, u64) = response.decode_pair()?;
    println!("resp value={:?}", s);
    assert_eq!((vec!["aa".to_string(), "aa".to_string()], 1), s);
    Ok(())
}

#[tokio::test]
async fn test_select() -> io::Result<()> {
    let client = init_client();
    let key = (1,);
    let response = client.select(SPACE_ID, 0, &key, 0, 100, IteratorType::EQ).await?;
    println!("response2: {:?}", response);
    let s: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(vec![(1, "test-row".to_string())], s);
    Ok(())
}

#[tokio::test]
async fn test_error() -> io::Result<()> {
    let client = init_client();
    let tuple = (44, "test_insert");
    client.delete(SPACE_ID, &tuple).await?;
    let _r = client.insert(SPACE_ID, &tuple).await?;
    let response = client.insert(SPACE_ID, &tuple).await;
    println!("response={:?}", response);
    match response {
        Err(_) => {}
        _ => assert!(false)
    }
    client.delete(SPACE_ID, &tuple).await?;
    // assert_eq!(Err(io::Error::new(io::ErrorKind::Other, "Duplicate key exists in unique index \'primary\' in space \'target_space\'")),
    //            response);

    Ok(())
}

#[tokio::test]
async fn test_delete_insert_update() -> io::Result<()> {
    let client = init_client();
    let tuple = (3, "test_insert");
    let tuple_replace = (3, "test_insert", "replace");
    let tuple_replace_raw = (3, "test_insert", "replace", "replace_raw");
    let update_op = (('=', 2, "test_update"),);
    client.delete(SPACE_ID, &tuple).await?;
    let response = client.insert(SPACE_ID, &tuple).await?;
    let s: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(vec![(3, "test_insert".to_string())], s);

    let response = client.update(SPACE_ID, &tuple, &update_op).await?;
    let s: Vec<(u32, String, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(
        vec![(3, "test_insert".to_string(), "test_update".to_string())],
        s
    );

    client.replace(SPACE_ID, &tuple_replace).await?;
    let raw_buf = serialize_to_vec_u8(&tuple_replace_raw).unwrap();
    let response = client.replace_raw(SPACE_ID, raw_buf).await?;
    let s: Vec<(u32, String, String, String)> = response.decode()?;
    println!("resp value={:?}", s);
    assert_eq!(
        vec![(
            3,
            "test_insert".to_string(),
            "replace".to_string(),
            "replace_raw".to_string()
        )],
        s
    );

    Ok(())
}

#[tokio::test]
async fn test_upsert() -> io::Result<()> {
    let client = init_client();
    let key = (4, "test_upsert");
    let update_op = (('=', 2, "test_update_upsert"),);

    let response = client.upsert(SPACE_ID, &key, &key, &update_op).await?;
    let s: Vec<u8> = response.decode()?;
    let empty: Vec<u8> = vec![];
    println!("resp value={:?}", s);
    assert_eq!(empty, s);
    Ok(())
}

#[tokio::test]
async fn test_eval() -> io::Result<()> {
    let client = init_client();

    let response = client.eval("return ...\n".to_string(), &(1, 2)).await?;
    let s: (u32, u32) = response.decode()?;
    let id: (u32, u32) = (1, 2);
    println!("resp value={:?}", s);
    assert_eq!(id, s);
    Ok(())
}

#[tokio::test]
async fn test_sql() -> io::Result<()> {
    let client = init_client();

    let response = client.exec_sql("select * from TABLE1 where COLUMN1=?", &(1,)).await?;
    let row: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", row);
    assert_eq!(row, vec![(1,"1".to_string())]);
    Ok(())
}

#[tokio::test]
async fn test_sql_args() -> io::Result<()> {
    let client = init_client();

    let response = client
        .prepare_call_args()
        .add_arg(&1)?
        .exec_sql("select * from TABLE1 where COLUMN1=?").await?;
    let row: Vec<(u32, String)> = response.decode()?;
    println!("resp value={:?}", row);
    assert_eq!(row, vec![(1,"1".to_string())]);
    Ok(())
}

// #[tokio::test]
// async fn test_prepare_stmt() -> io::Result<()> {
//     let client = init_client();
//
//     let response = client.prepare_stmt("select * from TABLE1 where COLUMN1=?".to_string()).await?;
//     let row: Vec<(u32, String)> = response.decode()?;
//     println!("resp value={:?}", row);
//     assert_eq!(row, vec![(1,"1".to_string())]);
//     Ok(())
// }

#[tokio::test]
async fn test_ping() -> io::Result<()> {
    let client = init_client();
    let response = client.ping().await?;
    println!("response ping: {:?}", response);
    Ok(())
}
