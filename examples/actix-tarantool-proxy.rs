#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;

use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use futures::future;
use futures::future::Future;

use rusty_tarantool::tarantool;
use rusty_tarantool::tarantool::Client;

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

fn handler(
    req: HttpRequest,
    params: web::Query<CountryRequest>,
) -> Box<dyn Future<Item = HttpResponse, Error = Error>> {
    let tarantool = req.app_data::<Client>().unwrap();
    println!("call tarantool! {:?}", params);
    let response = tarantool
        .call_fn3(
            "test_search",
            &params.country_name,
            &params.region,
            &params.sub_region,
        )
        .and_then(move |response| {
            Ok(CountryResponse {
                countries: response.decode_single()?,
            })
        })
        .map(|result| HttpResponse::Ok().json(result))
        .or_else(move |err| {
            let body = format!("Internal error: {}", err.to_string());
            future::ok::<_, Error>(HttpResponse::InternalServerError().body(body))
        });
    Box::new(response)
}

fn main() {
    HttpServer::new(|| {
        let tarantool =
            tarantool::ClientConfig::new("127.0.0.1:3301".parse().unwrap(), "rust", "rust").build();

        App::new()
            .data(tarantool)
            .route("/countries/query", web::to_async(handler))
    })
    .bind("127.0.0.1:8088")
    .unwrap()
    .run()
    .unwrap();
}
