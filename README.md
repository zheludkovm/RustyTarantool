# Simple Rust client to tarantool  

Ported java connector to tarantool db 
 
https://tarantool.io

https://github.com/tarantool/tarantool-java

## Overview

Use tokio.io as base client framework

## Usage

[![Latest Version]][crates.io]

[Latest Version]: https://img.shields.io/crates/v/rusty_tarantool.svg
[crates.io]: https://crates.io/crates/rusty_tarantool

## Example

Call echo stored procedure

run tarantool

```bash
cd test-tarantool;tarantool init-tarantool.lua
```

or via docker


```bash
# in rusty_tarantool dir
run_tests.sh
```

Lua stored procedure: 
```lua
function test(a,b)
   return a,b,11
end

```

Rust client :

```rust

println!("Connect to tarantool and call simple stored procedure!");
let client = ClientConfig::new("127.0.0.1:3301", "rust", "rust")
                     .set_timeout_time_ms(2000)
                     .set_reconnect_time_ms(2000)
                     .build();

let response = client
    .prepare_fn_call("test")
    .bind_ref(&("aa", "aa"))?
    .bind(1)?
    .execute().await?;
let res: ((String,String), u64) = response.decode_pair()?;
println!("stored procedure response ={:?}", res);

let response_sql = client
    .prepare_sql("select * from TABLE1 where COLUMN1=?")
    .bind(1)?
    .execute().await?;
    let meta = response.metadata();
    let rows: Vec<(u32, String)> = response.decode_result_set()?;
    println!("resp value={:?}", row);

```

Output :

```log
Connect to tarantool and call simple stored procedure!
stored procedure response =(("param11", "param12"), (2,), (Some(11),))
```

On examples part of project you can also see more complicated examples :

hyper http server connecting to tarantool

actix-web example

simple benchmark
