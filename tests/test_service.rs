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

use rusty_tarantool::tarantool::Client;
use tokio_core::reactor::Core;
use futures::Future;
use std::io;

use std::sync::{Once, ONCE_INIT};

static INIT: Once = ONCE_INIT;

static SPACE_ID:i32=1000;

fn setup_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

fn init_client() -> (Core, Client) {
    setup_logger();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = "127.0.0.1:3301".parse().unwrap();
    let client = Client::connect(&addr,"rust","rust",&handle);

    let client = core.run(client).unwrap();

    (core,client)
}

pub fn test_result(r: Result<(), io::Error>)   {
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
    let (mut core, client) = init_client();

    let resp = client.call_fn("test", &(("aa", "aa"), 1))
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: (Vec<String>, Vec<u64>) = response.decode_pair()?;
            println!("resp value={:?}", s);
            assert_eq!((vec!["aa".to_string(), "aa".to_string()], vec![1]), s);
            Ok(())
        })
    ;

    test_result(core.run(resp));
}

#[test]
fn test_select() {
    let (mut core, client) = init_client();
    let key= (1,);

    let resp = client.select(SPACE_ID,0, &key,0,100,0)
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: Vec<(u32, String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(1, "test-row".to_string())], s);
            Ok(())
        })
    ;
    test_result(core.run(resp));
}

#[test]
fn test_delete_insert_update() {
    let (mut core, client) = init_client();
    let tuple= (3,"test_insert");
    let tuple_replace= (3,"test_insert","replace");
    let update_op= (('=',2,"test_update"),);


    let resp =
        client.delete(SPACE_ID,&tuple)
        .and_then(|_|{
            client.insert(SPACE_ID,&tuple)
        })
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s: Vec<(u32, String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(3, "test_insert".to_string())], s);
            Ok(())
        })
        .and_then(|_| {
            client.update(SPACE_ID, &tuple, &update_op)
        })
        .and_then(move |response| {
            let s: Vec<(u32, String,String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(3, "test_insert".to_string(), "test_update".to_string())], s);
            Ok(())
        })
        .and_then(|_| {
            client.replace(SPACE_ID, &tuple_replace)
        })
        .and_then(move |response| {
            let s: Vec<(u32, String,String)> = response.decode()?;
            println!("resp value={:?}", s);
            assert_eq!(vec![(3, "test_insert".to_string(), "replace".to_string())], s);
            Ok(())
        })

    ;
    test_result(core.run(resp));
}

#[test]
fn test_upsert() {
    let (mut core, client) = init_client();
    let key= (4,"test_upsert");
    let update_op= (('=',2,"test_update_upsert"),);

    let resp =
        client.upsert(SPACE_ID,&key, &key,&update_op)
        .and_then(move |response| {
            println!("response2: {:?}", response);
            let s:Vec<u8> = response.decode()?;
            let empty:Vec<u8> = vec![];
            println!("resp value={:?}", s);
            assert_eq!(empty, s);
            Ok(())
        })
    ;
    test_result(core.run(resp));
}

#[test]
fn test_eval() {
    let (mut core, client) = init_client();

    let resp =
        client.eval("return ...\n".to_string(),&(1,2))
            .and_then(move |response| {
                println!("response2: {:?}", response);
                let s:(u32,u32) = response.decode()?;
                let id:(u32,u32) = (1,2);
                println!("resp value={:?}", s);
                assert_eq!(id, s);
                Ok(())
            })
    ;
    test_result(core.run(resp));
}