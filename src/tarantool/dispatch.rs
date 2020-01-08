use std::boxed::Box;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::string::ToString;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use futures::future;
use futures::stream::{SplitSink, SplitStream};
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Async, AsyncSink, Future, IntoFuture, Poll, Sink, Stream};
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tokio::timer::delay_queue;
use tokio::timer::{Delay, DelayQueue};
use tokio_codec::{Decoder, Framed};

use crate::tarantool::codec::{RequestId, TarantoolCodec, TarantoolFramedRequest};
use crate::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest, TarantoolResponse};

pub type TarantoolFramed = Framed<TcpStream, TarantoolCodec>;
pub type CallbackSender = oneshot::Sender<io::Result<TarantoolResponse>>;
pub type ReconnectNotifySender = mpsc::UnboundedSender<ClientStatus>;

static ERROR_SERVER_DISCONNECT: &str = "SERVER DISCONNECTED!";
pub static ERROR_DISPATCH_THREAD_IS_DEAD: &str = "DISPATCH THREAD IS DEAD!";
pub static ERROR_CLIENT_DISCONNECTED: &str = "CLIENT DISCONNECTED!";
static ERROR_TIMEOUT: &str = "TIMEOUT!";

///
/// Tarantool client config
///
/// # Examples
/// ```text
/// let client = ClientConfig::new(addr, "rust", "rust")
///            .set_timeout_time_ms(1000)
///            .set_reconnect_time_ms(10000)
///            .build();
///
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ClientConfig {
    addr: SocketAddr,
    login: String,
    password: String,
    reconnect_time_ms: u64,
    timeout_time_ms: Option<u64>,
}

impl ClientConfig {
    pub fn new<S, S1>(addr: SocketAddr, login: S, password: S1) -> ClientConfig
    where
        S: Into<String>,
        S1: Into<String>,
    {
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
}

#[derive(Clone, Debug)]
pub enum ClientStatus {
    Init,
    Connecting,
    Handshaking,
    Connected,
    Disconnecting(String),
    Disconnected(String),
}

enum DispatchState {
    New,
    OnConnect(ConnectFuture),
    OnHandshake(Box<dyn Future<Item = TarantoolFramed, Error = io::Error> + Send>),
    OnProcessing((SplitSink<TarantoolFramed>, SplitStream<TarantoolFramed>)),

    OnReconnect(String),
    OnSleep(Delay, String),
}

impl fmt::Display for DispatchState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let status = match *self {
            DispatchState::New => "New",
            DispatchState::OnConnect(_) => "OnConnect",
            DispatchState::OnHandshake(_) => "OnHandshake",
            DispatchState::OnProcessing(_) => "OnProcessing",
            DispatchState::OnReconnect(_) => "OnReconnect",
            DispatchState::OnSleep(_, _) => "OnSleep",
        };
        write!(f, "{}", status)
    }
}

impl DispatchState {
    fn get_client_status(&self) -> ClientStatus {
        match *self {
            DispatchState::New => ClientStatus::Init,
            DispatchState::OnConnect(_) => ClientStatus::Connecting,
            DispatchState::OnHandshake(_) => ClientStatus::Handshaking,
            DispatchState::OnProcessing(_) => ClientStatus::Connected,
            DispatchState::OnReconnect(ref error_message) => {
                ClientStatus::Disconnecting(error_message.clone())
            }
            DispatchState::OnSleep(_, ref error_message) => {
                ClientStatus::Disconnected(error_message.clone())
            }
        }
    }
}

struct DispatchEngine {
    command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
    awaiting_callbacks: HashMap<RequestId, CallbackSender>,
    notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,

    buffered_command: Option<TarantoolFramedRequest>,
    command_counter: RequestId,

    timeout_time_ms: Option<u64>,
    timeout_queue: DelayQueue<RequestId>,
    timeout_id_to_key: HashMap<RequestId, delay_queue::Key>,
}

impl DispatchEngine {
    fn new(
        command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
        timeout_time_ms: Option<u64>,
        notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,
    ) -> DispatchEngine {
        DispatchEngine {
            command_receiver,
            buffered_command: None,
            awaiting_callbacks: HashMap::new(),
            notify_callbacks,
            command_counter: 3,
            timeout_time_ms,
            timeout_queue: DelayQueue::new(),
            timeout_id_to_key: HashMap::new(),
        }
    }

    fn send_notify(&mut self, status: &ClientStatus) {
        let mut guard = self.notify_callbacks.write().unwrap();
        let callbacks: &mut Vec<ReconnectNotifySender> = guard.as_mut();

        //dirty code - send status to all callbacks and remove dead callbacks
        let mut i = 0;
        while i != callbacks.len() {
            if let Ok(_) = &callbacks[i].unbounded_send(status.clone()) {
                i = i + 1;
            } else {
                callbacks.remove(i);
            }
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
            let _res = callback_sender.send(Err(io::Error::new(
                io::ErrorKind::Other,
                error_description.clone(),
            )));
        }
        self.buffered_command = None;

        if let Some(_) = self.timeout_time_ms {
            self.timeout_id_to_key.clear();
            self.timeout_queue.clear();
        }

        loop {
            match self.command_receiver.poll() {
                Ok(Async::Ready(Some((_, callback_sender)))) => {
                    let _res = callback_sender.send(Err(io::Error::new(
                        io::ErrorKind::Other,
                        error_description.clone(),
                    )));
                }
                _ => break,
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
                    self.buffered_command =
                        Some((request_id, TarantoolRequest::Command(command_packet)));
                    if let Some(timeout_time_ms) = self.timeout_time_ms {
                        let delay_key = self.timeout_queue.insert_at(
                            request_id,
                            Instant::now() + Duration::from_millis(timeout_time_ms),
                        );
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
                        .map(|sender| sender.send(command_packet));
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
                        if let Some(callback_sender) = self.awaiting_callbacks.remove(request_id) {
                            //don't process result of send
                            let _res = callback_sender
                                .send(Err(io::Error::new(io::ErrorKind::Other, ERROR_TIMEOUT)));
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
    status: Arc<RwLock<ClientStatus>>,
}

impl Dispatch {
    pub fn new(
        config: ClientConfig,
        command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
        status: Arc<RwLock<ClientStatus>>,
        notify_callbacks: Arc<RwLock<Vec<ReconnectNotifySender>>>,
    ) -> Dispatch {
        let timeout_time_ms = config.timeout_time_ms.clone();
        Dispatch {
            state: DispatchState::New,
            config,
            engine: DispatchEngine::new(command_receiver, timeout_time_ms, notify_callbacks),
            status,
        }
    }

    fn update_status(&mut self) {
        let status_tmp = self.state.get_client_status();
        let mut status = self.status.write().unwrap();
        *status = status_tmp.clone();
        self.engine.send_notify(&status_tmp);
    }

    fn get_auth_seq(
        stream: TcpStream,
        config: &ClientConfig,
    ) -> Box<dyn Future<Item = TarantoolFramed, Error = io::Error> + Send> {
        let login = config.login.clone();
        let password = config.password.clone();

        Box::new(
            TarantoolCodec::new()
                .framed(stream)
                .into_future()
                .map_err(|e| e.0)
                .and_then(|(_first_resp, framed_io)| {
                    framed_io
                        .send((2, TarantoolRequest::Auth(AuthPacket { login, password })))
                        .into_future()
                })
                .and_then(|framed| framed.into_future().map_err(|e| e.0))
                .and_then(|(r, framed_io)| match r {
                    Some((_, Err(e))) => future::err(e),
                    _ => future::ok(framed_io),
                }),
        )
    }
}

impl Future for Dispatch {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        debug!("poll ! {}", self.state);

        loop {
            let new_state = match self.state {
                DispatchState::New => Some(DispatchState::OnConnect(TcpStream::connect(
                    &self.config.addr,
                ))),
                DispatchState::OnReconnect(ref error_description) => {
                    error!("Reconnect! error={}", error_description);
                    self.engine.send_error_to_all(error_description);
                    let delay_future = Delay::new(
                        Instant::now() + Duration::from_millis(self.config.reconnect_time_ms),
                    );
                    Some(DispatchState::OnSleep(
                        delay_future,
                        error_description.clone(),
                    ))
                }
                DispatchState::OnSleep(ref mut delay_future, _) => match delay_future.poll() {
                    Ok(Async::Ready(_)) => Some(DispatchState::New),
                    Ok(Async::NotReady) => None,
                    Err(err) => Some(DispatchState::OnReconnect(err.to_string())),
                },
                DispatchState::OnConnect(ref mut connect_future) => match connect_future.poll() {
                    Ok(Async::Ready(stream)) => Some(DispatchState::OnHandshake(
                        Dispatch::get_auth_seq(stream, &self.config),
                    )),
                    Ok(Async::NotReady) => None,
                    Err(err) => Some(DispatchState::OnReconnect(err.to_string())),
                },

                DispatchState::OnHandshake(ref mut handshake_future) => {
                    match handshake_future.poll() {
                        Ok(Async::Ready(framed)) => {
                            self.engine.clean_command_counter();
                            Some(DispatchState::OnProcessing(framed.split()))
                        }
                        Ok(Async::NotReady) => None,
                        Err(err) => Some(DispatchState::OnReconnect(err.to_string())),
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
                        Some(DispatchState::OnReconnect(
                            ERROR_SERVER_DISCONNECT.to_string(),
                        ))
                    } else {
                        self.engine.process_timeouts();
                        None
                    }
                }
            };

            if let Some(new_state_value) = new_state {
                self.state = new_state_value;
                self.update_status();
            } else {
                break;
            }
        }

        Ok(Async::NotReady)
    }
}
