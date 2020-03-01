pub use crate::tarantool::packets::{CommandPacket, TarantoolRequest, TarantoolResponse};
pub use crate::tarantool::tools::serialize_to_vec_u8;
use futures_channel::mpsc;
use futures_channel::oneshot;
use serde::Serialize;
use std::io;
use std::sync::{Arc, Mutex, RwLock};
use tokio;

pub mod codec;
mod dispatch;
pub mod packets;
mod tools;

use crate::tarantool::dispatch::{
    CallbackSender, Dispatch, ERROR_CLIENT_DISCONNECTED, ERROR_DISPATCH_THREAD_IS_DEAD,
};
pub use crate::tarantool::dispatch::{ClientConfig, ClientStatus};

impl ClientConfig {
    pub fn build(self) -> Client {
        Client::new(self)
    }
}

///
/// API to tarantool.
///
/// Create client by call build() on ClientConfig.
///
/// # Examples
///
/// let client = ClientConfig::new(addr, "rust", "rust").set_timeout_time_ms(1000).set_reconnect_time_ms(10000).build();
///
#[derive(Clone)]
pub struct Client {
    command_sender: mpsc::UnboundedSender<(CommandPacket, CallbackSender)>,
    dispatch: Arc<Mutex<Option<Dispatch>>>,
    status: Arc<RwLock<ClientStatus>>,
    notify_callbacks: Arc<Mutex<Vec<dispatch::ReconnectNotifySender>>>,
}

impl Client {
    /// manually create client by consume Client Config
    pub fn new(config: ClientConfig) -> Client {
        let (command_sender, command_receiver) = mpsc::unbounded();

        let status = Arc::new(RwLock::new(ClientStatus::Init));
        let notify_callbacks = Arc::new(Mutex::new(Vec::new()));

        Client {
            command_sender,
            dispatch: Arc::new(Mutex::new(Some(Dispatch::new(
                config,
                command_receiver,
                status.clone(),
                notify_callbacks.clone(),
            )))),
            status,
            notify_callbacks,
        }
    }

    /// return client status
    pub fn get_status(&self) -> ClientStatus {
        self.status.read().unwrap().clone()
    }

    /// return notify stream with connection statuses
    pub fn subscribe_to_notify_stream(&self) -> mpsc::UnboundedReceiver<ClientStatus> {
        let (callback_sender, callback_receiver) = mpsc::unbounded();
        self.notify_callbacks.lock().unwrap().push(callback_sender);
        callback_receiver
    }

    /// send any command you manually create, this method is low level and not intended to be used
    pub async fn send_command(&self, req: CommandPacket) -> io::Result<TarantoolResponse> {
        //        let dispatch = self.dispatch.clone();

        if let Some(mut extracted_dispatch) = self.dispatch.clone().lock().unwrap().take() {
            debug!("spawn coroutine!");
            //lazy spawning main coroutine in first tarantool call
            tokio::spawn(async move {
                extracted_dispatch.run().await;
            });
        }

        let (callback_sender, callback_receiver) = oneshot::channel();
        let send_res = self.command_sender.unbounded_send((req, callback_sender));
        match send_res {
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    ERROR_DISPATCH_THREAD_IS_DEAD,
                ));
            }
            _ => {}
        };
        match callback_receiver.await {
            Err(_) => Err(io::Error::new(
                io::ErrorKind::Other,
                ERROR_CLIENT_DISCONNECTED,
            )),
            Ok(res) => res,
        }
    }

    /// call tarantool stored procedure
    ///
    /// params mast be serializable to MsgPack tuple by Serde - rust tuple or vector or struct (by default structs serialized as tuple)
    ///
    /// order of fields in serializes tuple is order od parameters of procedure
    ///
    ///  # Examples
    ///
    /// lua function on tarantool
    /// ```lua
    /// function test(a,b)
    ///   return a,b,11
    ///  end
    /// ```
    ///
    /// rust code
    /// ```text
    ///  let response = client.call_fn("test", &(("aa", "aa"), 1)).await?;
    ///  let s: (Vec<String>, Vec<u64>) = response.decode_pair()?;
    ///  println!("resp value={:?}", s);
    ///  assert_eq!((vec!["aa".to_string(), "aa".to_string()], vec![1]), s);
    ///
    #[inline(always)]
    pub async fn call_fn<T>(&self, function: &str, params: &T) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(CommandPacket::call(function, params).unwrap())
            .await
    }

    ///call tarantool stored procedure with one parameter
    ///
    #[inline(always)]
    pub async fn call_fn1<T1>(&self, function: &str, param1: &T1) -> io::Result<TarantoolResponse>
    where
        T1: Serialize,
    {
        self.send_command(CommandPacket::call(function, &(param1,)).unwrap())
            .await
    }

    ///call tarantool stored procedure with two parameters
    ///
    /// # Examples
    ///
    ///```text
    /// let response = client.call_fn2("test", &("param11", "param12") , &2).await?;
    ///  let s: (Vec<String>, Vec<u64>) = response.decode_pair()?;
    ///  println!("resp value={:?}", s);
    ///  assert_eq!((vec!["aa".to_string(), "aa".to_string()], vec![1]), s);
    ///
    #[inline(always)]
    pub async fn call_fn2<T1, T2>(
        &self,
        function: &str,
        param1: &T1,
        param2: &T2,
    ) -> io::Result<TarantoolResponse>
    where
        T1: Serialize,
        T2: Serialize,
    {
        self.send_command(CommandPacket::call(function, &(param1, param2)).unwrap())
            .await
    }

    ///call tarantool stored procedure with three parameters
    ///
    #[inline(always)]
    pub async fn call_fn3<T1, T2, T3>(
        &self,
        function: &str,
        param1: &T1,
        param2: &T2,
        param3: &T3,
    ) -> io::Result<TarantoolResponse>
    where
        T1: Serialize,
        T2: Serialize,
        T3: Serialize,
    {
        self.send_command(CommandPacket::call(function, &(param1, param2, param3)).unwrap())
            .await
    }

    ///call tarantool stored procedure with four parameters
    ///
    #[inline(always)]
    pub async fn call_fn4<T1, T2, T3, T4>(
        &self,
        function: &str,
        param1: &T1,
        param2: &T2,
        param3: &T3,
        param4: &T4,
    ) -> io::Result<TarantoolResponse>
    where
        T1: Serialize,
        T2: Serialize,
        T3: Serialize,
        T4: Serialize,
    {
        self.send_command(CommandPacket::call(function, &(param1, param2, param3, param4)).unwrap())
            .await
    }

    ///call tarantool stored procedure with five parameters
    ///
    #[inline(always)]
    pub async fn call_fn5<T1, T2, T3, T4, T5>(
        &self,
        function: &str,
        param1: &T1,
        param2: &T2,
        param3: &T3,
        param4: &T4,
        param5: &T5,
    ) -> io::Result<TarantoolResponse>
    where
        T1: Serialize,
        T2: Serialize,
        T3: Serialize,
        T4: Serialize,
        T5: Serialize,
    {
        self.send_command(
            CommandPacket::call(function, &(param1, param2, param3, param4, param5)).unwrap(),
        )
        .await
    }

    ///call "select" from tarantool
    /// - space - i32 space id
    /// - index - i32 index id
    /// - key - key part used for select, may be sequence (vec or tuple)
    /// - offset - i32 select offset
    /// - limit - i32 limit of rows
    /// - iterator - type of iterator
    ///
    #[inline(always)]
    pub async fn select<T>(
        &self,
        space: i32,
        index: i32,
        key: &T,
        offset: i32,
        limit: i32,
        iterator: i32,
    ) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(
            CommandPacket::select(space, index, key, offset, limit, iterator).unwrap(),
        )
        .await
    }

    ///insert tuple to space
    /// - space - space id
    /// - tuple - sequence of fields(can be vec or rust tuple)
    ///
    #[inline(always)]
    pub async fn insert<T>(&self, space: i32, tuple: &T) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(CommandPacket::insert(space, tuple).unwrap())
            .await
    }

    #[inline(always)]
    ///replace tuple in space by primary key
    /// - space - space id
    /// - tuple - sequence of fields(can be vec or rust tuple)
    ///
    /// # Examples
    /// ```text
    /// let tuple_replace= (3,"test_insert","replace");
    /// client.replace(SPACE_ID, &tuple_replace).await?;
    ///
    pub async fn replace<T>(&self, space: i32, tuple: &T) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(CommandPacket::replace(space, tuple).unwrap())
            .await
    }

    #[inline(always)]
    ///replace tuple in space by primary key - raw method if you have already serialized msgpack
    /// - space - space id
    /// - tuple - sequence of fields stored as msgpack
    ///
    /// # Examples
    /// ```text
    /// let tuple_replace= (3,"test_insert","replace");
    /// let raw_buf = serialize_to_vec_u8(&tuple_replace).unwrap();
    /// client.replace_raw(SPACE_ID, raw_buf).await?;
    ///
    pub async fn replace_raw(
        &self,
        space: i32,
        tuple_raw: Vec<u8>,
    ) -> io::Result<TarantoolResponse> {
        self.send_command(CommandPacket::replace_raw(space, tuple_raw).unwrap())
            .await
    }

    ///update row in tarantool
    /// - space - space id
    /// - key - sequence of fields(rust tuple or vec)
    /// - args - sequence of update operations, for example (('=',2,"test_update"),)
    ///
    /// # Examples
    /// ```text
    /// let tuple= (3,"test_insert");
    /// let update_op= (('=',2,"test_update"),);
    /// client.update(SPACE_ID, &tuple, &update_op).await?;
    ///
    #[inline(always)]
    pub async fn update<T, T2>(
        &self,
        space: i32,
        key: &T2,
        args: &T,
    ) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
        T2: Serialize,
    {
        self.send_command(CommandPacket::update(space, key, args).unwrap())
            .await
    }

    ///upsert row in tuple
    ///
    /// # Examples
    /// ```text
    /// let key= (4,"test_upsert");
    /// let update_op= (('=',2,"test_update_upsert"),);
    /// client.upsert(SPACE_ID,&key, &key,&update_op).await?;
    ///
    #[inline(always)]
    pub async fn upsert<T, T2, T3>(
        &self,
        space: i32,
        key: &T2,
        def: &T3,
        args: &T,
    ) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
        T2: Serialize,
        T3: Serialize,
    {
        self.send_command(CommandPacket::upsert(space, key, def, args).unwrap())
            .await
    }

    ///delete row in space
    ///
    /// # Examples
    /// ```text
    /// let tuple= (3,"test_insert");
    /// client.delete(SPACE_ID,&tuple).await?;
    #[inline(always)]
    pub async fn delete<T>(&self, space: i32, key: &T) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(CommandPacket::delete(space, key).unwrap())
            .await
    }

    ///eval expression in tarantool
    ///
    /// # Examples
    ///
    /// ```text
    /// client.eval("return ...\n".to_string(),&(1,2)).await?
    ///
    #[inline(always)]
    pub async fn eval<T>(&self, expression: String, args: &T) -> io::Result<TarantoolResponse>
    where
        T: Serialize,
    {
        self.send_command(CommandPacket::eval(expression, args).unwrap())
            .await
    }

    ///ping tarantool server, return empty response in success
    ///
    /// # Examples
    ///
    /// ```text
    /// client.ping().await?
    ///
    #[inline(always)]
    pub async fn ping(&self) -> io::Result<TarantoolResponse> {
        self.send_command(CommandPacket::ping().unwrap()).await
    }
}
