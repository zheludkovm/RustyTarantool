#[macro_use]
extern crate log;
extern crate hyper;
extern crate futures;
extern crate tokio_core;

extern crate rusty_tarantool;
extern crate env_logger;
#[macro_use]
extern crate serde_derive;
extern crate rmpv;
extern crate url;

use rusty_tarantool::tarantool::ClientFactory;

use tokio_core::reactor::Core;

use futures::Stream;
use futures::future::Future;
use hyper::header::{ContentLength,ContentType};
use hyper::server::{Http, Request, Response, Service};
use hyper::{Method, StatusCode};
use hyper::mime;


use std::error::Error;
use std::net::SocketAddr;
use std::collections::HashMap;

extern crate serde_json;


#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct CountryInfo {
    #[serde(rename = "country-code", default)]
    country_code: u16,
    name: String,
    region: Option<String>,
    #[serde(rename = "sub-region", default)]
    sub_region: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Result {
    countries:Vec<CountryInfo>
}

impl Result {
    pub fn new(countries:Vec<CountryInfo>)-> Result {
        Result{countries}
    }
}

struct SimpleService {
    client_factory: ClientFactory
}

fn parse_query(query: &str) -> HashMap<String, String> {
    url::form_urlencoded::parse(&query.as_bytes())
        .into_owned()
        .collect::<HashMap<String, String>>()
}

impl SimpleService {
    fn process_query_request(&self, req: Request)->Box<Future<Item=Response, Error=hyper::Error>> {
        let (country_name, region, sub_region ) =  match req.uri().query() {
            Some(query) => {
                let mut query_params = parse_query(query);
                (query_params.remove("country_name"), query_params.remove("region"), query_params.remove("sub_region"))
            } ,
            None => (None, None, None)
        };
        Box::new(self.client_factory
            .get_connection()
            .and_then(move |client| client.call_fn3("test_search", &country_name, &region, &sub_region ))
            .and_then(move |mut response| {
                Ok(Result::new(response.decode_single()?))
            })
            .and_then(|result| {
                let body = serde_json::to_string(&result)?;
                Ok(Response::new()
                    .with_header(ContentType(mime::APPLICATION_JSON))
                    .with_header(ContentLength(body.len() as u64))
                    .with_body(body))
            })
            .or_else(|err| {
                Ok(Response::new()
                .with_header(ContentType(mime::TEXT_PLAIN))
                .with_status(StatusCode::InternalServerError)
                .with_body(format!("Internal error: {}",  err.description().to_string())))
            }))
    }
}

impl Service for SimpleService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Method::Get, "/countries/query") => self.process_query_request(req),
            _ =>  Box::new(futures::future::ok(Response::new().with_body("Not Found").with_status(StatusCode::NotFound)))
        }
    }
}

fn main() {
    println!("start server!");
    env_logger::init();

    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = "127.0.0.1:3301".parse().unwrap();

    let client_factory = ClientFactory::new(addr, "rust", "rust", handle);

    let addr2: SocketAddr = "127.0.0.1:8097".parse().unwrap();

//    let http = Http::new();
//    let listener = TcpListener::bind(&addr2, &handle).unwrap();
//
//    let server = listener
//        .incoming()
//        .for_each(move |(sock, _)| {
//            http.bind_connection(&handle,
//                                 sock,
//                                 addr,
//                                 SimpleService {
//                                     client_factory: client_factory.clone()
//                                 });
//            Ok(())
//        });
//
//    core.run(server).unwrap();

    let main_handle = core.handle();
    let server_handle = core.handle();
    let server = Http::new().serve_addr_handle(&addr2, &main_handle, move || {
        Ok(SimpleService {
            client_factory: client_factory.clone()
        })
    }).unwrap_or_else(|why| {
        error!("Http Server Initialization Error: {}", why);
        std::process::exit(1);
    });

    main_handle.spawn(
        server
            .for_each(move |conn| {
                server_handle.spawn(
                    conn.map(|_| ())
                        .map_err(|why| error!("Server Error: {:?}", why))
                );
                Ok(())
            })
            .map_err(|_| ()),
    );

    core.run(futures::future::empty::<(), ()>()).unwrap();
}
