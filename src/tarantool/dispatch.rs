use core::pin::Pin;
use std::collections::HashMap;
use std::io;
use std::string::ToString;
use std::sync::{Arc, Mutex, RwLock};

use futures::select;
use futures::stream::StreamExt;
use futures::SinkExt;
use futures_channel::mpsc;
use futures_channel::oneshot;
use futures_util::FutureExt;

use tokio::net::TcpStream;
use tokio::time::{delay_for, delay_queue, DelayQueue, Duration, Instant};
use tokio_util::codec::{Decoder, Framed};

use crate::tarantool::codec::{RequestId, TarantoolCodec, TarantoolFramedRequest};
use crate::tarantool::packets::{AuthPacket, CommandPacket, TarantoolRequest, TarantoolResponse};

pub type TarantoolFramed = Framed<TcpStream, TarantoolCodec>;
pub type CallbackSender = oneshot::Sender<io::Result<TarantoolResponse>>;
pub type ReconnectNotifySender = mpsc::UnboundedSender<ClientStatus>;

//static ERROR_SERVER_DISCONNECT: &str = "SERVER DISCONNECTED!";
pub static ERROR_DISPATCH_THREAD_IS_DEAD: &str = "DISPATCH THREAD IS DEAD!";
pub static ERROR_CLIENT_DISCONNECTED: &str = "CLIENT DISCONNECTED!";
//static ERROR_TIMEOUT: &str = "TIMEOUT!";

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
    addr: String,
    login: String,
    password: String,
    reconnect_time_ms: u64,
    timeout_time_ms: Option<u64>,
}

impl ClientConfig {
    pub fn new<S0, S, S1>(addr: S0, login: S, password: S1) -> ClientConfig
    where
        S0: Into<String>,
        S: Into<String>,
        S1: Into<String>,
    {
        ClientConfig {
            addr: addr.into(),
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
    Closed,
}

pub struct Dispatch {
    config: ClientConfig,
    // engine: DispatchEngine,
    command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
    is_command_receiver_closed: bool,
    awaiting_callbacks: HashMap<RequestId, CallbackSender>,
    notify_callbacks: Arc<Mutex<Vec<ReconnectNotifySender>>>,

    buffered_command: Option<TarantoolFramedRequest>,
    command_counter: RequestId,

    timeout_time_ms: Option<u64>,
    timeout_queue: DelayQueue<RequestId>,
    timeout_id_to_key: HashMap<RequestId, delay_queue::Key>,

    status: Arc<RwLock<ClientStatus>>,
}

impl Dispatch {
    pub fn new(
        config: ClientConfig,
        command_receiver: mpsc::UnboundedReceiver<(CommandPacket, CallbackSender)>,
        status: Arc<RwLock<ClientStatus>>,
        notify_callbacks: Arc<Mutex<Vec<ReconnectNotifySender>>>,
    ) -> Dispatch {
        let timeout_time_ms = config.timeout_time_ms.clone();
        Dispatch {
            config,
            command_receiver,
            is_command_receiver_closed:false,
            buffered_command: None,
            awaiting_callbacks: HashMap::new(),
            notify_callbacks,
            command_counter: 3,
            timeout_time_ms,
            timeout_queue: DelayQueue::new(),
            timeout_id_to_key: HashMap::new(),
            status,
        }
    }

    ///send status notification to all subscribers
    async fn send_notify(&mut self, status: &ClientStatus) {
        let callbacks: Vec<ReconnectNotifySender> =
            self.notify_callbacks.lock().unwrap().split_off(0);
        let mut filtered_callbacks: Vec<ReconnectNotifySender> = Vec::new();
        for mut callback in callbacks {
            if let Ok(_) = callback.send(status.clone()).await {
                filtered_callbacks.push(callback);
            }
        }

        self.notify_callbacks
            .lock()
            .unwrap()
            .extend(filtered_callbacks.iter().cloned());
    }

    ///send command from buffer. if not success, return command to buffer and initiate reconnect
    async fn try_send_buffered_command(&mut self, sink: &mut TarantoolFramed) -> io::Result<()> {
        if let Some(command) = self.buffered_command.take() {
            if let Err(e) = Pin::new(sink).send(command.clone()).await {
                //return command to buffer
                self.buffered_command = Some(command);
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    e.to_string(),
                ));
            }
        }
        Ok(())
    }

    ///send error to all awaiting callbacks
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

        if !self.is_command_receiver_closed {
            loop {
                match self.command_receiver.try_next() {
                    Ok(Some((_, callback_sender))) => {
                        let _res = callback_sender.send(Err(io::Error::new(
                            io::ErrorKind::Other,
                            error_description.clone(),
                        )));
                    }
                    _ => break,
                };
            }
        }
    }

    ///process command - send to tarantool, store callback
    async fn process_command(
        &mut self,
        command: Option<(CommandPacket, CallbackSender)>,
        sink: &mut TarantoolFramed,
    ) -> io::Result<()> {
        self.try_send_buffered_command(sink).await?;

        match command {
            Some((command_packet, callback_sender)) => {
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
                //if return disconnected - retry
                self.try_send_buffered_command(sink).await
            }
            None => {
                self.is_command_receiver_closed = true;
                //inbound sink is finished. close coroutine
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "inbound commands queue is over",
                ))
            }
        }
    }

    ///process tarantool response
    async fn process_tarantool_response(
        &mut self,
        response: Option<io::Result<(RequestId, io::Result<TarantoolResponse>)>>,
    ) -> io::Result<()> {
        debug!("receive command! {:?} ", response);
        return match response {
            Some(Ok((request_id, Ok(command_packet)))) => {
                debug!("receive command! {} {:?} ", request_id, command_packet);
                if let Some(_) = self.timeout_time_ms {
                    if let Some(delay_key) = self.timeout_id_to_key.remove(&request_id) {
                        self.timeout_queue.remove(&delay_key);
                    }
                }
                if let Some(callback) = self.awaiting_callbacks.remove(&request_id) {
                    let _send_res = callback.send(Ok(command_packet));
                }

                Ok(())
            },
            Some(Ok((request_id, Err(e)))) => {
                debug!("receive command! {} {:?} ", request_id, e);
                if let Some(_) = self.timeout_time_ms {
                    if let Some(delay_key) = self.timeout_id_to_key.remove(&request_id) {
                        self.timeout_queue.remove(&delay_key);
                    }
                }
                if let Some(callback) = self.awaiting_callbacks.remove(&request_id) {
                    let _send_res = callback.send(Err(e));
                }

                Ok(())
            },
            None => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "return none from stream!",
            )),
            _ => Ok(()),
        };
    }

    fn increment_command_counter(&mut self) -> RequestId {
        self.command_counter = self.command_counter + 1;
        self.command_counter
    }

    fn clean_command_counter(&mut self) {
        self.command_counter = 3;
    }

    async fn set_status(&mut self, client_status: ClientStatus) {
        self.send_notify(&client_status).await;
        *(self.status.write().unwrap()) = client_status;
    }

    ///main dispatch look function
    pub async fn run(&mut self) {
        self.set_status(ClientStatus::Connecting).await;
        loop {
            match self.connect_and_process_commands().await {
                Ok(()) => {
                    //finish
                    return;
                }
                Err(e) => {
                    self.set_status(ClientStatus::Disconnected(e.to_string()))
                        .await;
                    self.send_error_to_all(&e.to_string());
                    delay_for(Duration::from_millis(self.config.reconnect_time_ms)).await;
                }
            }

            if self.is_command_receiver_closed {
                self.set_status(ClientStatus::Closed).await;
                return;
            }
        }
    }

    async fn connect_and_process_commands(&mut self) -> io::Result<()> {
        let tcp_stream = TcpStream::connect(self.config.addr.clone()).await?;
        let mut framed_io = self.auth(tcp_stream).await?;
        self.set_status(ClientStatus::Connected).await;
        loop {
            select! {
                tarantool_response = framed_io.next().fuse() => {
                    self.process_tarantool_response(tarantool_response).await?
                }
                command = self.command_receiver.next() => {
                    self.process_command(command, &mut framed_io).await?
                }
            }
        }
    }

    async fn auth(&mut self, tcp_stream: TcpStream) -> io::Result<TarantoolFramed> {
        let mut framed_io = TarantoolCodec::new().framed(tcp_stream);
        let _first_response = framed_io.next().await;
        // println!("Received first packet {:?}", first_response);
        framed_io
            .send((
                2,
                TarantoolRequest::Auth(AuthPacket {
                    login: String::from(self.config.login.clone()),
                    password: String::from(self.config.password.clone()),
                }),
            ))
            .await?;
        let auth_response = framed_io.next().await;
        match auth_response {
            Some(Ok((_, Err(e)))) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                e.to_string(),
            )),
            _ => {
                self.clean_command_counter();
                Ok(framed_io)
            }
        }
    }
}
