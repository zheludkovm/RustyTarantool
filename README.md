# Simple Rust client to tarantool  

Ported java connector to tarantool db 
 
https://tarantool.io

https://github.com/tarantool/tarantool-java

## Overview

Use tokio.io and multiplex tokio-proto

## Example

Call echo stored procedure

Lua: 
```lua
function test(a,b)
   return a,b,11
end

```

Rust client :

```rust

let mut core = Core::new().unwrap();
let handle = core.handle();
let addr = "127.0.0.1:3301".parse().unwrap();
let client_factory = ClientFactory::new(addr, "rust", "rust", handle);

let response_future = client_factory.get_connection()
    .and_then(|client| {
        client.call_fn2("test", &("param11", "param12") , &2)
    })
    .and_then(move |mut response| {
        let (value1, value2, value3) : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
        Ok((value1, value2, value3))
    }) ;
match core.run(response_future) {
    Err(e) => println!("err={:?}", e),
    Ok(res) => println!("stored procedure response ={:?}", res)
}

```

On examples part of project you can also see more complicated example :

http server connecting to tarantool