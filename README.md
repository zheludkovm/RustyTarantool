# Simple Rust client to tarantool  

Ported java connector to tarantool db 
 
https://tarantool.io

https://github.com/tarantool/tarantool-java

## Overview

Use tokio.io as base client framework

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
let mut rt = Runtime::new().unwrap();

let addr = "127.0.0.1:3301".parse().unwrap();
let client = ClientConfig::new(addr, "rust", "rust").build();

let response_future = client.call_fn2("test", &("param11", "param12") , &2)
    .and_then(|response| {
        let res : ((String,String), (u64,), (Option<u64>,)) = response.decode_trio()?;
        Ok(res)
    }) ;

match rt.block_on(response_future) {
    Err(e) => println!("err={:?}", e),
    Ok(res) => println!("stored procedure response ={:?}", res)
}

```

Output :

```log
Connect to tarantool and call simple stored procedure!
stored procedure response =(("param11", "param12"), (2,), (Some(11),))
```

On examples part of project you can also see more complicated example :

http server connecting to tarantool