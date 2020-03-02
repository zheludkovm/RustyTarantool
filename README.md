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

let response = client.call_fn2("test", &("param11", "param12") , &2).await?;
let res : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
println!("stored procedure response ={:?}", res);

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
