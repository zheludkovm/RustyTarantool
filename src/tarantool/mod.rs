use futures::{Async, Future, Poll};
use futures::future;
use serde::Serialize;
use std::cell::{RefCell};
use std::io;
use std::net::SocketAddr;
use std::str;
use std::rc::Rc;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool,Ordering};
use tarantool::codec::TarantoolProto;
use tarantool::packets::{CommandPacket, TarantoolRequest, TarantoolResponse};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_proto::multiplex::ClientService;
use tokio_proto::TcpClient;
use tokio_service::Service;

pub mod packets;
pub mod codec;
pub mod tools;

#[derive(Clone)]
enum ClientFactoryState {
    ClientInitialized(Client),
    Initializing,
    NotInitialized,
}

///Client factory for tarantool.
///holds one connection. and rereate it if some problems occure
///
/// # Examples
///
/// let mut core = Core::new().unwrap();
/// let handle = core.handle();
/// let addr = "127.0.0.1:3301".parse().unwrap();
/// let client_factory = ClientFactory::new(addr, "rust", "rust", handle);
/// self.client_factory
///   .get_connection()
///   .and_then(move |client| client.call_fn3("test_search", &country_name, &region, &sub_region ))
///   ...
///
///
#[derive(Clone)]
pub struct ClientFactory {
    /// current state of client Factory
    state: Rc<RefCell<ClientFactoryState>>,

    ///stored attrs fo create connections
    addr: SocketAddr,
    login: String,
    password: String,
    handle: Handle,
}

impl ClientFactory {

    /// init client Factory
    ///
    pub fn new<S, S1>(addr: SocketAddr, login: S, password: S1, handle: Handle) -> ClientFactory
        where S: Into<String>,
              S1: Into<String>
    {
        ClientFactory {
            state: Rc::new(RefCell::new(ClientFactoryState::NotInitialized)),
            addr,
            login:login.into().clone(),
            password:password.into().clone(),
            handle,
        }
    }

    ///internal metod used for client creation.
    /// init client and return it in future result. Also caches client in factory state
    ///
    #[inline(always)]
    fn init_client(&self) -> Box<Future<Item=Client, Error=io::Error>> {
        let state_holder = self.state.clone();
        let state_holder2 = self.state.clone();
        self.state.replace(ClientFactoryState::Initializing);

        Box::new(
            Client::connect(&self.addr, self.login.clone(), self.password.clone(), &self.handle)
            .and_then(move |client| {
                state_holder.replace(ClientFactoryState::ClientInitialized(client.clone()));
                Ok(client)
            }).map_err(move |e| {
                error!("Tarantool on login ERROR >> {:?}",e);
                state_holder2.replace(ClientFactoryState::NotInitialized);
                e
            })
        )
    }

    ///method to get connection.
    ///return future with client
    ///
    #[inline(always)]
    pub fn get_connection(&self) -> Box<Future<Item=Client, Error=io::Error>> {
        match *self.state.borrow() {
            ClientFactoryState::ClientInitialized(ref client) if client.is_ok.load(Ordering::Relaxed) => {
                return Box::new(future::ok(client.clone()))
            },
            ClientFactoryState::Initializing => {
                return Box::new(ClientFactoryFuture { client_factory: self.clone() })
            },
            _ => {}
        };
        return self.init_client();
    }
}


///internal polling future - checks factory state
///current not working because of tokio reactor logic
///
struct ClientFactoryFuture {
    client_factory: ClientFactory
}

impl Future for ClientFactoryFuture {
    type Item = Client;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let state = (*self.client_factory.state).borrow() ;
        match *state {
            ClientFactoryState::ClientInitialized(ref client) => {
                if client.is_ok.load(Ordering::Relaxed) {
                    Ok(Async::Ready(client.clone()))
                } else {
                    Ok(Async::NotReady)
                }
            }
            _ => Ok(Async::NotReady)
        }
    }
}


///single threaded client for tarantool
///
/// # Examples
///  let mut core = Core::new().unwrap();
///  let handle = core.handle();
///  let addr = "127.0.0.1:3301".parse().unwrap();
///  let client_f = Client::connect(&addr,"rust","rust", &handle);
///  let client = core.run(client_f).unwrap();
///
#[derive(Debug, Clone)]
pub struct Client {
    inner: ClientService<TcpStream, TarantoolProto>,
    pub is_ok: Arc<AtomicBool>,
}


impl Service for Client {
    type Request = TarantoolRequest;
    type Response = io::Result<TarantoolResponse>;
    type Error = io::Error;
    type Future = Box<Future<Item=io::Result<TarantoolResponse>, Error=io::Error>>;

    #[inline(always)]
    fn call(&self, req: TarantoolRequest) -> Self::Future {
        Box::new(self.inner.call(req))
    }
}

/// Client for tarantool
/// provides method to call function, insert tuple e.t.c.
///
impl Client {

    ///internal methods to wrap all calls
    /// process errors and returns them as future result.
    /// process fatal errors of proto and mark is_ok state as dead
    ///
    #[inline(always)]
    fn call_wrap(&self, req: TarantoolRequest) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> {
        let is_ok_ref = self.is_ok.clone();
        Box::new(Client::call(self, req)
            .map_err(move |e| {
                is_ok_ref.store(false,Ordering::Relaxed);
                e
            })
            .and_then(|resp: io::Result<TarantoolResponse>| {
                resp
            })
        )
    }

    /// Establish a connection to a multiplexed line-based server at the
    /// provided `addr`.
    pub fn connect<S, S1>(addr: &SocketAddr, login: S, password: S1, handle: &Handle) -> Box<Future<Item=Client, Error=io::Error>>
        where S:Into<String>,S1:Into<String>
    {
        let ret = TcpClient::new(TarantoolProto { login:login.into(), password:password.into() })
            .connect(addr, handle)
            .map(|client_service| {
                Client {
                    inner: client_service,
                    is_ok: Arc::new(AtomicBool::new(true)),
                }
            });

        Box::new(ret)
    }

    ///call tarantool stored procedure
    /// params shoud be sequence : vec o tuple
    ///for example :
    /// &(("aa", "aa"), 1)
    /// &("11", 1)
    ///
    #[inline(always)]
    pub fn call_fn<T>(&self, function: &str, params: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, params).unwrap()))
    }

    ///call tarantool stored procedure with one parameter
    ///
    #[inline(always)]
    pub fn call_fn1<T1>(&self, function: &str, param1: &T1) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T1: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, &(param1,)).unwrap()))
    }

    ///call tarantool stored procedure with two parameters
    ///
    #[inline(always)]
    pub fn call_fn2<T1,T2>(&self, function: &str, param1: &T1, param2: &T2) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T1: Serialize,
              T2: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, &(param1,param2)).unwrap()))
    }

    ///call tarantool stored procedure with three parameters
    ///
    #[inline(always)]
    pub fn call_fn3<T1,T2, T3>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T1: Serialize,
              T2: Serialize,
              T3: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, &(param1,param2,param3)).unwrap()))
    }

    ///call tarantool stored procedure with four parameters
    ///
    #[inline(always)]
    pub fn call_fn4<T1,T2, T3,T4>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3, param4: &T4) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T1: Serialize,T2: Serialize,T3: Serialize,T4: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, &(param1,param2,param3,param4)).unwrap()))
    }

    ///call tarantool stored procedure with five parameters
    ///
    #[inline(always)]
    pub fn call_fn5<T1,T2, T3,T4,T5>(&self, function: &str, param1: &T1, param2: &T2, param3: &T3, param4: &T4, param5: &T5) -> Box<Future<Item=TarantoolResponse, Error=io::Error>>
        where T1: Serialize,T2: Serialize,T3: Serialize,T4: Serialize,T5: Serialize
    {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::call(function, &(param1,param2,param3,param4,param5)).unwrap()))
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
    pub fn select<T>(&self, space: i32, index: i32, key: &T, offset: i32, limit: i32, iterator: i32) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::select(space, index, key, offset, limit, iterator).unwrap()))
    }

    ///insert tuple to space
    /// space - space id
    /// tuple - sequence of fields(can be vec or rust tuple)
    ///
    #[inline(always)]
    pub fn insert<T>(&self, space: i32, tuple: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::insert(space, tuple).unwrap()))
    }

    #[inline(always)]

    //replace tuple in space by primary key
    /// space - space id
    /// tuple - sequence of fields(can be vec or rust tuple)
    /// # Examples
    /// let tuple_replace= (3,"test_insert","replace");
    /// client.replace(SPACE_ID, &tuple_replace)
    ///
    pub fn replace<T>(&self, space: i32, tuple: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::replace(space, tuple).unwrap()))
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
    pub fn update<T, T2>(&self, space: i32, key: &T2, args: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize, T2: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::update(space, key, args).unwrap()))
    }

    ///upsert row in tuple
    /// # Examples
    /// let key= (4,"test_upsert");
    /// let update_op= (('=',2,"test_update_upsert"),);
    /// client.upsert(SPACE_ID,&key, &key,&update_op)
    ///
    #[inline(always)]
    pub fn upsert<T, T2, T3>(&self, space: i32, key: &T2, def: &T3, args: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize, T2: Serialize, T3: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::upsert(space, key, def, args).unwrap()))
    }

    ///delete row in space
    /// # Examples
    /// let tuple= (3,"test_insert");
    /// client.delete(SPACE_ID,&tuple)
    #[inline(always)]
    pub fn delete<T>(&self, space: i32, key: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::delete(space, key).unwrap()))
    }

    ///eval expression in tarantool
    /// # Examples
    /// client.eval("return ...\n".to_string(),&(1,2))
    ///
    #[inline(always)]
    pub fn eval<T>(&self, expression: String, args: &T) -> Box<Future<Item=TarantoolResponse, Error=io::Error>> where T: Serialize {
        self.call_wrap(TarantoolRequest::Command(CommandPacket::eval(expression, args).unwrap()))
    }
}