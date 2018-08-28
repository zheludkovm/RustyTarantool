use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink, Stream};
use futures::future;
use futures::stream::{SplitSink, SplitStream};
use futures::sync::mpsc;
use futures::sync::oneshot;
use serde::Serialize;
use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
//use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tarantool::codec::{RequestId, TarantoolCodec, TarantoolFramedRequest};
use tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest, TarantoolResponse};
use tokio;
use tokio::net::{ConnectFuture, TcpStream};
use tokio::timer::{Delay, DelayQueue};
use tokio::timer::delay_queue;
use tokio_codec::{Decoder, Framed};


pub mod packets;
pub mod codec;
pub mod tools;

pub type TarantoolFramed = Framed<TcpStream, TarantoolCodec>;
pub type CallbackSender = oneshot::Sender<io::Result<TarantoolResponse>>;

static ERROR_SERVER_DISCONNECT: &str = "SERVER DISCONNECTED!";
static ERROR_DISPATCH_THREAD_IS_DEAD: &str = "DISPATCH THREAD IS DEAD!";
static ERROR_CLIENT_DISCONNECTED: &str = "CLIENT DISCONNECTED!";
static ERROR_TIMEOUT: &str = "TIMEOUT!";

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ClientConfig {
    addr: SocketAddr,
    login: String,
    password: String,
    reconnect_time_ms: u64,
    timeout_time_ms: Option<u64>,
}

enum DispatchState {
    New,
    OnConnect(ConnectFuture),
    OnHandshake(Box<Future<Item=TarantoolFramed, Error=io::Error> + Send>),
    OnProcessing((SplitSink<TarantoolFramed>, SplitStream<TarantoolFramed>)),

    OnReconnect(String),
    OnSleep(Delay),
}

impl fmt::Display for DispatchState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DispatchState::New => write!(f, "New"),
            DispatchState::OnConnect(_) => write!(f, "OnConnect"),
            DispatchState::OnHandshake(_) => write!(f, "OnHandshake"),
            DispatchState::OnProcessing(_) => write!(f, "OnProcessing"),
            DispatchState::OnReconnect(_) => write!(f, "OnReconnect"),
            DispatchState::OnSleep(_) => write!(f, "OnSleep"),
        }
    }
}

struct DispatchEngine {
    command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
    awaiting_callbacks: HashMap<RequestId, CallbackSender>,

    buffered_command: Option<TarantoolFramedRequest>,
    command_counter: RequestId,

    timeout_time_ms: Option<u64>,
    timeout_queue: DelayQueue<RequestId>,
    timeout_id_to_key: HashMap<RequestId, delay_queue::Key>,
}

impl DispatchEngine {
    fn new(command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>, timeout_time_ms: Option<u64>) -> DispatchEngine {
        DispatchEngine {
            command_receiver,
            buffered_command: None,
            awaiting_callbacks: HashMap::new(),
            command_counter: 3,
            timeout_time_ms,
            timeout_queue: DelayQueue::new(),
            timeout_id_to_key: HashMap::new(),
        }
    }

    fn try_send_buffered_command(&mut self, sink: &mut SplitSink<TarantoolFramed>) -> bool {
        if let Some(command) = self.buffered_command.take() {
            if let Ok(AsyncSink::NotReady(command)) = sink.start_send(command) {
//return command to buffer
                self.buffered_command = Some(command);
                return false;
            }
        }
        true
    }

    fn send_error_to_all(&mut self, error_description: &String) {
        for (_, callback_sender) in self.awaiting_callbacks.drain() {
            let _res = callback_sender.send(Err(io::Error::new(io::ErrorKind::Other, error_description.clone())));
        }
        self.buffered_command = None;

        if let Some(timeout_time_ms) = self.timeout_time_ms {
            self.timeout_id_to_key.clear();
            self.timeout_queue.clear();
        }

        loop {
            match self.command_receiver.poll() {
                Ok(Async::Ready(Some((_, callback_sender)))) => {
                    let _res = callback_sender.send(Err(io::Error::new(io::ErrorKind::Other, error_description.clone())));
                }
                _ => break
            };
        }
    }

    fn process_commands(&mut self, sink: &mut SplitSink<TarantoolFramed>) -> Poll<(), ()> {
        let mut continue_send = self.try_send_buffered_command(sink);
        while continue_send {
            continue_send = match self.command_receiver.poll() {
                Ok(Async::Ready(Some((command_packet, callback_sender)))) => {
                    let request_id = self.increment_command_counter();
                    self.awaiting_callbacks.insert(request_id, callback_sender);
                    self.buffered_command = Some((request_id, TarantoolRequest::Command(command_packet)));
                    if let Some(timeout_time_ms) = self.timeout_time_ms {
                        let delay_key = self.timeout_queue.insert_at(request_id, Instant::now() + Duration::from_millis(timeout_time_ms));
                        self.timeout_id_to_key.insert(request_id, delay_key);
                    }

                    self.try_send_buffered_command(sink)
                }
                Ok(Async::Ready(None)) => {
//inbound sink is finished. close coroutine
                    return Ok(Async::Ready(()));
                }
                _ => false,
            };
        }
//skip results of poll complete
        let _r = sink.poll_complete();
        Ok(Async::NotReady)
    }

    fn process_tarantool_responses(&mut self, stream: &mut SplitStream<TarantoolFramed>) -> bool {
        loop {
            match stream.poll() {
                Ok(Async::Ready(Some((request_id, command_packet)))) => {
                    debug!("receive command! {} {:?} ", request_id, command_packet);
                    if let Some(_) = self.timeout_time_ms {
                        if let Some(delay_key) = self.timeout_id_to_key.remove(&request_id) {
                            self.timeout_queue.remove(&delay_key);
                        }
                    }

                    self.awaiting_callbacks
                        .remove(&request_id)
                        .map(|sender| { sender.send(command_packet) });
                }
                Ok(Async::Ready(None)) | Err(_) => {
                    return true;
                }
                _ => {
                    return false;
                }
            }
        }
    }

    fn process_timeouts(&mut self) {
        if let Some(_) = self.timeout_time_ms {
            loop {
                match self.timeout_queue.poll() {
                    Ok(Async::Ready(Some(request_id_ref))) => {
                        let request_id = request_id_ref.get_ref();
                        info!("timeout command! {} ", request_id);
                        self.timeout_id_to_key.remove(request_id);
                        if let Some(callback_sender) =  self.awaiting_callbacks.remove(request_id) {
                            callback_sender.send(Err(io::Error::new(io::ErrorKind::Other, ERROR_TIMEOUT)));
                        }
                    }
                    _ => {
                        return;
                    }
                }
            }
        }
    }

    fn increment_command_counter(&mut self) -> RequestId {
        self.command_counter = self.command_counter + 1;
        self.command_counter
    }

    fn clean_command_counter(&mut self) {
        self.command_counter = 3;
    }
}

pub struct Dispatch {
    config: ClientConfig,
    state: DispatchState,
    engine: DispatchEngine,
}


impl Dispatch {
    pub fn new(config: ClientConfig, command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>) -> Dispatch {
        let timeout_time_ms = config.timeout_time_ms.clone();
        Dispatch {
            state: DispatchState::New,
            config,
            engine: DispatchEngine::new(command_receiver, timeout_time_ms),
        }
    }

    fn get_auth_seq(stream: TcpStream, config: &ClientConfig) -> Box<Future<Item=TarantoolFramed, Error=io::Error> + Send> {
        let login = config.login.clone();
        let password = config.password.clone();

        Box::new(
            TarantoolCodec::new().framed(stream)
                .into_future()
                .map_err(|e| { e.0 })
                .and_then(|(_first_resp, framed_io)| {
                    framed_io.send((2, TarantoolRequest::Auth(AuthPacket {
                        login,
                        password,
                    })))
                        .into_future()
                })
                .and_then(|framed| {
                    framed.into_future().map_err(|e| { e.0 })
                })
                .and_then(|(r, framed_io)| {
                    match r {
                        Some((_, Err(e))) => future::err(e),
                        _ => future::ok(framed_io)
                    }
                }))
    }
}

impl Future for Dispatch {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        debug!("poll ! {}", self.state);

        loop {
            let new_state = match self.state {
                DispatchState::New => {
                    Some(DispatchState::OnConnect(TcpStream::connect(&self.config.addr)))
                }
                DispatchState::OnReconnect(ref mut error_description) => {
                    error!("Reconnect! error={}", error_description);
                    self.engine.send_error_to_all(error_description);
                    let delay_future = Delay::new(Instant::now() + Duration::from_millis(self.config.reconnect_time_ms));
                    Some(DispatchState::OnSleep(delay_future))
                }
                DispatchState::OnSleep(ref mut delay_future) => {
                    match delay_future.poll() {
                        Ok(Async::Ready(_)) => Some(DispatchState::New),
                        Ok(Async::NotReady) => None,
                        Err(err) => Some(DispatchState::OnReconnect(err.to_string()))
                    }
                }
                DispatchState::OnConnect(ref mut connect_future) => {
                    match connect_future.poll() {
                        Ok(Async::Ready(stream)) => Some(DispatchState::OnHandshake(Dispatch::get_auth_seq(stream, &self.config))),
                        Ok(Async::NotReady) => None,
                        Err(err) => Some(DispatchState::OnReconnect(err.to_string()))
                    }
                }

                DispatchState::OnHandshake(ref mut handshake_future) => {
                    match handshake_future.poll() {
                        Ok(Async::Ready(framed)) => {
                            self.engine.clean_command_counter();
                            Some(DispatchState::OnProcessing(framed.split()))
                        }
                        Ok(Async::NotReady) => None,
                        Err(err) => Some(DispatchState::OnReconnect(err.to_string()))
                    }
                }

                DispatchState::OnProcessing((ref mut sink, ref mut stream)) => {
                    match self.engine.process_commands(sink) {
                        Ok(Async::Ready(())) => {
//                          stop client !!! exit from event loop !
                            return Ok(Async::Ready(()));
                        }
                        _ => {}
                    }

                    if self.engine.process_tarantool_responses(stream) {
                        Some(DispatchState::OnReconnect(ERROR_SERVER_DISCONNECT.to_string()))
                    } else {
                        self.engine.process_timeouts();
                        None
                    }
                }
            };

            if let Some(new_state_value) = new_state {
                self.state = new_state_value;
            } else {
                break;
            }
        }

        Ok(Async::NotReady)
    }
}

#[derive(Clone)]
pub struct Client {
    command_sender: mpsc::UnboundedSender<(CommandPacket, CallbackSender)>,
    dispatch: Arc<Mutex<Option<Dispatch>>>,
}

impl ClientConfig {
    pub fn new<S, S1>(addr: SocketAddr, login: S, password: S1) -> ClientConfig
        where S: Into<String>,
              S1: Into<String> {
        ClientConfig {
            addr,
            login: login.into(),
            password: password.into(),
            reconnect_time_ms: 10000,
            timeout_time_ms: None,
        }
    }

    pub fn set_timeout_time_ms(mut self, timeout_time_ms: u64) -> ClientConfig {
        self.timeout_time_ms = Some(timeout_time_ms);
        self
    }


    pub fn set_reconnect_time_ms(mut self, reconnect_time_ms: u64) -> ClientConfig {
        self.reconnect_time_ms = reconnect_time_ms;
        self
    }

    pub fn build(self) -> Client {
        Client::new(self)
    }
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
