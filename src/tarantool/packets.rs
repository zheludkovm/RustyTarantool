#![allow(non_camel_case_types)]
use std::io;
use std::str;

use crate::tarantool::tools;
use rmpv::Value;
use serde::{Deserialize, Serialize};

use bytes::{Bytes, IntoBuf};

/// tarantool auth packet
#[derive(Debug)]
pub struct AuthPacket {
    pub login: String,
    pub password: String,
}

/// tarantool packet intended for serialize and cross thread send
#[derive(Debug, Clone)]
pub struct CommandPacket {
    pub code: Code,
    pub internal_fields: Vec<(Key, Value)>,
    pub command_field: Vec<(Key, Vec<u8>)>,
}

/// Tarantool request enum (auth or ordinary packet)
#[derive(Debug)]
pub enum TarantoolRequest {
    Auth(AuthPacket),
    Command(CommandPacket),
}

/// Tarantool response struct
///
/// use any decode method to decode tarantool response to custom struct by serde
/// please look examples
/// https://github.com/zheludkovm/RustyTarantool/tree/master/examples
///
#[derive(Debug)]
pub struct TarantoolResponse {
    pub code: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub enum Code {
    SELECT = 0x01,
    INSERT = 0x02,
    REPLACE = 0x03,
    UPDATE = 0x04,
    DELETE = 0x05,
    OLD_CALL = 0x06,
    AUTH = 0x07,
    EVAL = 0x08,
    UPSERT = 0x09,
    CALL = 0x010,
    PING = 0x040,
    SUBSCRIBE = 0x066,
}

#[derive(Debug, Copy, Clone)]
pub enum Key {
    //header
    CODE = 0x00,
    SYNC = 0x01,
    SCHEMA_ID = 0x05,

    //body
    SPACE = 0x10,
    INDEX = 0x11,
    LIMIT = 0x12,
    OFFSET = 0x13,
    ITERATOR = 0x14,
    KEY = 0x20,
    TUPLE = 0x21,
    FUNCTION = 0x22,
    USER_NAME = 0x23,
    EXPRESSION = 0x27,
    UPSERT_OPS = 0x28,
    DATA = 0x30,
    ERROR = 0x31,
}

impl TarantoolResponse {
    pub fn new(code: u64, data: Bytes) -> TarantoolResponse {
        TarantoolResponse { code, data }
    }

    /// decode tarantool response to any serder deserializable struct
    pub fn decode<'de, T>(self) -> io::Result<T>
    where
        T: Deserialize<'de>,
    {
        tools::decode_serde(self.data.into_buf())
    }

    /// decode tarantool response to tuple wih one element and return this element
    pub fn decode_single<'de, T>(self) -> io::Result<T>
    where
        T: Deserialize<'de>,
    {
        let (res,) = tools::decode_serde(self.data.into_buf())?;
        Ok(res)
    }

    /// decode tarantool response to tuple of two elements
    pub fn decode_pair<'de, T1, T2>(self) -> io::Result<(T1, T2)>
    where
        T1: Deserialize<'de>,
        T2: Deserialize<'de>,
    {
        Ok(tools::decode_serde(self.data.into_buf())?)
    }

    ///decode tarantool response to three elements
    pub fn decode_trio<'de, T1, T2, T3>(self) -> io::Result<(T1, T2, T3)>
    where
        T1: Deserialize<'de>,
        T2: Deserialize<'de>,
        T3: Deserialize<'de>,
    {
        let (r1, r2, r3) = tools::decode_serde(self.data.into_buf())?;
        Ok((r1, r2, r3))
    }
}

impl CommandPacket {
    pub fn call<T>(function: &str, params: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::OLD_CALL,
            internal_fields: vec![(Key::FUNCTION, Value::from(function))],
            command_field: vec![(Key::TUPLE, tools::serialize_to_vec_u8(params)?)],
        })
    }

    pub fn select<T>(
        space: i32,
        index: i32,
        key: &T,
        offset: i32,
        limit: i32,
        iterator: i32,
    ) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::SELECT,
            internal_fields: vec![
                (Key::SPACE, Value::from(space)),
                (Key::INDEX, Value::from(index)),
                (Key::ITERATOR, Value::from(iterator)),
                (Key::LIMIT, Value::from(limit)),
                (Key::OFFSET, Value::from(offset)),
            ],
            command_field: vec![(Key::KEY, tools::serialize_to_vec_u8(key)?)],
        })
    }

    pub fn insert<T>(space: i32, tuple: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::INSERT,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![(Key::TUPLE, tools::serialize_to_vec_u8(tuple)?)],
        })
    }

    pub fn replace<T>(space: i32, tuple: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::REPLACE,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![(Key::TUPLE, tools::serialize_to_vec_u8(tuple)?)],
        })
    }

    pub fn replace_raw(space: i32, tuple_raw: Vec<u8>) -> io::Result<CommandPacket> {
        Ok(CommandPacket {
            code: Code::REPLACE,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![(Key::TUPLE, tuple_raw)],
        })
    }

    pub fn update<T, T2>(space: i32, key: &T2, args: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
        T2: Serialize,
    {
        Ok(CommandPacket {
            code: Code::UPDATE,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![
                (Key::KEY, tools::serialize_to_vec_u8(key)?),
                (Key::TUPLE, tools::serialize_to_vec_u8(args)?),
            ],
        })
    }

    pub fn upsert<T, T2, T3>(space: i32, key: &T2, def: &T3, args: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
        T2: Serialize,
        T3: Serialize,
    {
        Ok(CommandPacket {
            code: Code::UPSERT,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![
                (Key::KEY, tools::serialize_to_vec_u8(key)?),
                (Key::TUPLE, tools::serialize_to_vec_u8(def)?),
                (Key::UPSERT_OPS, tools::serialize_to_vec_u8(args)?),
            ],
        })
    }

    pub fn delete<T>(space: i32, key: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::DELETE,
            internal_fields: vec![(Key::SPACE, Value::from(space))],
            command_field: vec![(Key::KEY, tools::serialize_to_vec_u8(key)?)],
        })
    }

    pub fn eval<T>(expression: String, args: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        Ok(CommandPacket {
            code: Code::EVAL,
            internal_fields: vec![(Key::EXPRESSION, Value::from(expression))],
            command_field: vec![(Key::TUPLE, tools::serialize_to_vec_u8(args)?)],
        })
    }

    pub fn ping() -> io::Result<CommandPacket> {
        Ok(CommandPacket {
            code: Code::PING,
            internal_fields: vec![],
            command_field: vec![],
        })
    }
}
