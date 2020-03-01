use rusty_tarantool::tarantool::{Client, ClientConfig};
use serde_derive::{Deserialize, Serialize};
use std::convert::Infallible;

use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Request, Response, Server, StatusCode};
use std::collections::HashMap;
use std::io;
use url;

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

async fn hello(req: Request<Body>, tarantool_client: Client) -> io::Result<Response<Body>> {
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
            let response = tarantool_client
                .call_fn3("test_search", &country_name, &region, &sub_region)
                .await?;
            let country_response = CountryResponse {
                countries: response.decode_single()?,
            };
            let body = serde_json::to_string(&country_response).unwrap();
            let resp = Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .status(StatusCode::OK)
                .body(body.into())
                .unwrap();

            Ok(resp)
        }
        _ => {
            let resp = Response::builder()
                .header(header::CONTENT_TYPE, "text/plain")
                .status(StatusCode::NOT_FOUND)
                .body("Url not found!".into())
                .unwrap();
            Ok(resp)
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    let tarantool_client = ClientConfig::new("127.0.0.1:3301", "rust", "rust")
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build();

    let make_svc = make_service_fn(|_conn| {
        let tarantool_clone = tarantool_client.clone();

        async { Ok::<_, Infallible>(service_fn(move |req| hello(req, tarantool_clone.clone()))) }
    });

    let addr = ([127, 0, 0, 1], 8080).into();
    let server = Server::bind(&addr).serve(make_svc);
    println!("Listening on http://{}", addr);
    server.await?;
    Ok(())
}
