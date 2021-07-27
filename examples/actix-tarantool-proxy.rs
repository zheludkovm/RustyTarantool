//extern crate rusty_tarantool;
extern crate serde_derive;
extern crate serde_json;

use serde_derive::{Deserialize, Serialize};

use actix_web::{get, web, App, HttpResponse, HttpServer};
use futures::stream::StreamExt;
use futures::{select, FutureExt};
use rusty_tarantool::tarantool::{Client, ClientConfig};
use std::io;

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

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct CountryRequest {
    country_name: Option<String>,
    region: Option<String>,
    sub_region: Option<String>,
}

#[get("/countries/query")]
async fn index(
    params: web::Query<CountryRequest>,
    tarantool_client: web::Data<Client>,
) -> io::Result<HttpResponse> {
    let response = tarantool_client
        .call_fn3(
            "test_search",
            &params.country_name,
            &params.region,
            &params.sub_region,
        )
        .await?;
    Ok(HttpResponse::Ok().json(CountryResponse {
        countries: response.decode_single()?,
    }))
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let tarantool_client = ClientConfig::new("127.0.0.1:3301", "rust", "rust")
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build();

    let mut notify_future = Box::pin(
        tarantool_client
            .subscribe_to_notify_stream()
            .for_each_concurrent(0, |s| async move { println!("current status {:?}", s) }),
    );

    let mut server_future =
        HttpServer::new(move || App::new().data(tarantool_client.clone()).service(index))
            .bind("127.0.0.1:8080")?
            .run()
            .fuse();

    select! {
        _r = notify_future => println!("Status notify stream is finished!"),
        _r = server_future => println!("Server is stopped!"),
    };
    Ok(())
}
