#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;

use std::collections::HashMap;
use std::iter::Iterator;
use std::sync::Once;

use futures::future;
use futures::stream::Stream;
use hyper::header;
use hyper::rt::Future;
use hyper::service::service_fn;
use hyper::{Body, Method, Request, Response, Server, StatusCode};

use rusty_tarantool::tarantool;

static INIT_STATUS_CHANGE: Once = Once::new();

type BoxFut = Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send>;

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
struct CountryResponse {
    countries: Vec<CountryInfo>,
}

fn parse_query(query: &str) -> HashMap<String, String> {
    url::form_urlencoded::parse(&query.as_bytes())
        .into_owned()
        .collect::<HashMap<String, String>>()
}

fn http_handler(req: Request<Body>, tarantool: &tarantool::Client) -> BoxFut {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/countries/query") => {
            let (country_name, region, sub_region) = match req.uri().query() {
                Some(query) => {
                    let mut query_params = parse_query(query);
                    (
                        query_params.remove("country_name"),
                        query_params.remove("region"),
                        query_params.remove("sub_region"),
                    )
                }
                None => (None, None, None),
            };

            let tarantool_c = tarantool.clone();
            let response = tarantool
                .call_fn3("test_search", &country_name, &region, &sub_region)
                .and_then(move |response| {
                    Ok(CountryResponse {
                        countries: response.decode_single()?,
                    })
                })
                .map(|result| {
                    let body = serde_json::to_string(&result).unwrap();
                    Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .status(StatusCode::OK)
                        .body(body.into())
                        .unwrap()
                })
                .or_else(move |err| {
                    println!("status = {:?}", tarantool_c.get_status());

                    future::ok(
                        Response::builder()
                            .header(header::CONTENT_TYPE, "text/plain")
                            .body(format!("Internal error: {}", err.to_string()).into())
                            .unwrap(),
                    )
                });
            Box::new(response)
        }
        _ => {
            let resp = Response::builder()
                .header(header::CONTENT_TYPE, "text/plain")
                .status(StatusCode::NOT_FOUND)
                .body("Url not found!".into())
                .unwrap();
            Box::new(future::ok(resp))
        }
    }
}

fn main() {
    let addr = ([127, 0, 0, 1], 3078).into();

    let tarantool =
        tarantool::ClientConfig::new("127.0.0.1:3301".parse().unwrap(), "rust", "rust").build();

    let service = move || {
        let tarantool_ref = (&tarantool).clone();

        //init status tracker only once
        INIT_STATUS_CHANGE.call_once(|| {
            hyper::rt::spawn(
                tarantool_ref
                    .subscribe_to_notify_stream()
                    .for_each(|status| {
                        println!("status change to = {:?}", status);
                        Ok(())
                    }),
            );
        });

        service_fn(move |body| http_handler(body, &tarantool_ref))
    };

    let server = Server::bind(&addr)
        .serve(service)
        .map_err(|e| eprintln!("server error: {}", e));

    println!("Listening on http://{}", addr);
    hyper::rt::run(server);
}
