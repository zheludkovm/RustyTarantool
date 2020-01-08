extern crate bytes;
extern crate env_logger;
extern crate futures;
extern crate rmp;
extern crate rmp_serde;
extern crate rmpv;
extern crate rusty_tarantool;
extern crate serde;
extern crate tokio;
extern crate tokio_codec;

use futures::Future;
use rusty_tarantool::tarantool::{serialize_to_vec_u8, Client, ClientConfig};
use std::io;
use std::sync::Once;
use tokio::runtime::current_thread::Runtime;

static INIT: Once = Once::new();

static SPACE_ID: i32 = 1000;

fn setup_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

fn init_client() -> Client {
    setup_logger();

    let addr = "127.0.0.1:3301".parse().unwrap();
    ClientConfig::new(addr, "rust", "rust").build()
}

pub fn test_result(r: Result<(), io::Error>) {
    match r {
        Err(e) => {
            println!("err={:?}", e);
            assert!(false);
        }
        Ok(res) => {
            println!("ressp={:?}", res);
        }
    }
}

#[test]
fn test_call_fn() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();
    let resp = client
        .call_fn("test", &(("aa", "aa"), 1))
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: (Vec<String>, Vec<u64>) = response.decode_pair()?;
            println!("resp value={:?}", s);
            assert_eq!((vec!["aa".to_string(), "aa".to_string()], vec![1]), s);
            Ok(())
        });

    test_result(rt.block_on(resp));
}

#[test]
fn test_select() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();
    let key = (1,);

    let resp = client
        .select(SPACE_ID, 0, &key, 0, 100, 0)
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: Vec<(u32, String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(1, "test-row".to_string())], s);
            Ok(())
        });
    test_result(rt.block_on(resp));
}

#[test]
fn test_delete_insert_update() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();
    let tuple = (3, "test_insert");
    let tuple_replace = (3, "test_insert", "replace");
    let tuple_replace_raw = (3, "test_insert", "replace", "replace_raw");
    let update_op = (('=', 2, "test_update"),);

    let resp = client
        .delete(SPACE_ID, &tuple)
        .and_then(|_| client.insert(SPACE_ID, &tuple))
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: Vec<(u32, String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(3, "test_insert".to_string())], s);
            Ok(())
        })
        .and_then(|_| client.update(SPACE_ID, &tuple, &update_op))
        .and_then(move |response| {
            let s: Vec<(u32, String, String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(
                vec![(3, "test_insert".to_string(), "test_update".to_string())],
                s
            );
            Ok(())
        })
        .and_then(|_| client.replace(SPACE_ID, &tuple_replace))
        .and_then(|_| {
            let raw_buf = serialize_to_vec_u8(&tuple_replace_raw).unwrap();
            client.replace_raw(SPACE_ID, raw_buf)
        })
        .and_then(move |response| {
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
        });
    test_result(rt.block_on(resp));
}

#[test]
fn test_upsert() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();
    let key = (4, "test_upsert");
    let update_op = (('=', 2, "test_update_upsert"),);

    let resp = client
        .upsert(SPACE_ID, &key, &key, &update_op)
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: Vec<u8> = response.decode()?;
            let empty: Vec<u8> = vec![];
            println!("resp value={:?}", s);
            assert_eq!(empty, s);
            Ok(())
        });
    test_result(rt.block_on(resp));
}

#[test]
fn test_eval() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();

    let resp = client
        .eval("return ...\n".to_string(), &(1, 2))
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: (u32, u32) = response.decode()?;
            let id: (u32, u32) = (1, 2);
            println!("resp value={:?}", s);
            assert_eq!(id, s);
            Ok(())
        });
    test_result(rt.block_on(resp));
}

#[test]
fn test_ping() {
    let mut rt = Runtime::new().unwrap();
    let client = init_client();

    let resp = client.ping().and_then(move |response| {
        println!("response ping: {:?}", response);
        Ok(())
    });
    test_result(rt.block_on(resp));
}
