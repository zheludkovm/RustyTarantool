use futures::{Future, IntoFuture};
use futures::sync::mpsc;
use futures::sync::oneshot;
use serde::Serialize;
use std::io;
use std::sync::{Arc, Mutex};
use tarantool::dispatch::{CallbackSender, Dispatch, ERROR_CLIENT_DISCONNECTED, ERROR_DISPATCH_THREAD_IS_DEAD};
pub use tarantool::dispatch::ClientConfig;
use tarantool::packets::{ CommandPacket,  TarantoolResponse};
use tokio;


pub mod packets;
pub mod codec;
mod tools;
mod dispatch;

impl ClientConfig {
    pub fn build(self) -> Client {
        Client::new(self)
    }
}

#[derive(Clone)]
pub struct Client {
    command_sender: mpsc::UnboundedSender<(CommandPacket, CallbackSender)>,
    dispatch: Arc<Mutex<Option<Dispatch>>>,
}

impl Client {
    pub fn new(config: ClientConfig) -> Client {
        let (command_sender, command_receiver) = mpsc::unbounded();

        Client {
            command_sender,
            dispatch: Arc::new(Mutex::new(Some(Dispatch::new(
                config,
                command_receiver,
            )))),
        }
    }

    pub fn send_command(&self, req: CommandPacket) -> impl Future<Item=TarantoolResponse, Error=io::Error> {
        let dispatch = self.dispatch.clone();

        let (callback_sender, callback_receiver) = oneshot::channel();
        let send_res = self.command_sender.unbounded_send((req, callback_sender));
        send_res.into_future()
            .map_err(|_e| io::Error::new(io::ErrorKind::Other, ERROR_DISPATCH_THREAD_IS_DEAD))
            .and_then(move |_r| {
                if let Some(extracted_dispatch) = dispatch.lock().unwrap().take() {
                    debug!("spawn coroutine!");
                    tokio::spawn(extracted_dispatch);
                }
                callback_receiver
                    .into_future()
                    .map_err(|_e| io::Error::new(io::ErrorKind::Other, ERROR_CLIENT_DISCONNECTED))
            })
            .and_then(|r| { r })
    }

    #[inline(always)]
    pub fn call_fn<T>(&self, function: &str, params: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T: Serialize
    {
        self.send_command(CommandPacket::call(function, params).unwrap())
    }

    ///call tarantool stored procedure with one parameter
    ///
    #[inline(always)]
    pub fn call_fn1<T1>(&self, function: &str, param1: &T1) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T1: Serialize
    {
        self.send_command(CommandPacket::call(function, &(param1, )).unwrap())
    }

    ///call tarantool stored procedure with two parameters
    ///
    #[inline(always)]
    pub fn call_fn2<T1, T2>(&self, function: &str, param1: &T1, param2: &T2) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T1: Serialize,
              T2: Serialize
    {
        self.send_command(CommandPacket::call(function, &(param1, param2)).unwrap())
    }

    ///call tarantool stored procedure with three parameters
    ///
    #[inline(always)]
    pub fn call_fn3<T1, T2, T3>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T1: Serialize,
              T2: Serialize,
              T3: Serialize
    {
        self.send_command(CommandPacket::call(function, &(param1, param2, param3)).unwrap())
    }

    ///call tarantool stored procedure with four parameters
    ///
    #[inline(always)]
    pub fn call_fn4<T1, T2, T3, T4>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3, param4: &T4) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T1: Serialize, T2: Serialize, T3: Serialize, T4: Serialize
    {
        self.send_command(CommandPacket::call(function, &(param1, param2, param3, param4)).unwrap())
    }

    ///call tarantool stored procedure with five parameters
    ///
    #[inline(always)]
    pub fn call_fn5<T1, T2, T3, T4, T5>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3, param4: &T4, param5: &T5) -> impl Future<Item=TarantoolResponse, Error=io::Error>
        where T1: Serialize, T2: Serialize, T3: Serialize, T4: Serialize, T5: Serialize
    {
        self.send_command(CommandPacket::call(function, &(param1, param2, param3, param4, param5)).unwrap())
    }

    ///call "select" from tarantool
    /// space - i32 space id
    /// index - i32 index id
    /// key - key part used for select, may be sequence (vec or tuple)
    /// offset - i32 select offset
    /// limit - i32 limit of rows
    /// iterator - type of iterator
    ///
    #[inline(always)]
    pub fn select<T>(&self, space: i32, index: i32, key: &T, offset: i32, limit: i32, iterator: i32) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize {
        self.send_command(CommandPacket::select(space, index, key, offset, limit, iterator).unwrap())
    }

    ///insert tuple to space
    /// space - space id
    /// tuple - sequence of fields(can be vec or rust tuple)
    ///
    #[inline(always)]
    pub fn insert<T>(&self, space: i32, tuple: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize {
        self.send_command(CommandPacket::insert(space, tuple).unwrap())
    }

    #[inline(always)]

//replace tuple in space by primary key
    /// space - space id
    /// tuple - sequence of fields(can be vec or rust tuple)
    /// # Examples
    /// let tuple_replace= (3,"test_insert","replace");
    /// client.replace(SPACE_ID, &tuple_replace)
    ///
    pub fn replace<T>(&self, space: i32, tuple: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize {
        self.send_command(CommandPacket::replace(space, tuple).unwrap())
    }

    ///update row in tarantool
    /// space - space id
    /// key - sequence of fields(rust tuple or vec)
    /// args - sequence of update operations, for example (('=',2,"test_update"),)
    /// # Examples
    /// let tuple= (3,"test_insert");
    /// let update_op= (('=',2,"test_update"),);
    /// client.update(SPACE_ID, &tuple, &update_op)
    ///
    #[inline(always)]
    pub fn update<T, T2>(&self, space: i32, key: &T2, args: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize, T2: Serialize {
        self.send_command(CommandPacket::update(space, key, args).unwrap())
    }

    ///upsert row in tuple
    /// # Examples
    /// let key= (4,"test_upsert");
    /// let update_op= (('=',2,"test_update_upsert"),);
    /// client.upsert(SPACE_ID,&key, &key,&update_op)
    ///
    #[inline(always)]
    pub fn upsert<T, T2, T3>(&self, space: i32, key: &T2, def: &T3, args: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize, T2: Serialize, T3: Serialize {
        self.send_command(CommandPacket::upsert(space, key, def, args).unwrap())
    }

    ///delete row in space
    /// # Examples
    /// let tuple= (3,"test_insert");
    /// client.delete(SPACE_ID,&tuple)
    #[inline(always)]
    pub fn delete<T>(&self, space: i32, key: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize {
        self.send_command(CommandPacket::delete(space, key).unwrap())
    }

    ///eval expression in tarantool
    /// # Examples
    /// client.eval("return ...\n".to_string(),&(1,2))
    ///
    #[inline(always)]
    pub fn eval<T>(&self, expression: String, args: &T) -> impl Future<Item=TarantoolResponse, Error=io::Error> where T: Serialize {
        self.send_command(CommandPacket::eval(expression, args).unwrap())
    }
}
