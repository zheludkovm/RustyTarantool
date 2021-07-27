#![allow(non_camel_case_types)]
use std::io;
use std::io::Cursor;
use std::str;

use crate::tarantool::tools;
use rmpv::Value;
use serde::{Deserialize, Serialize};

use bytes::Bytes;
use std::collections::HashMap;

/// tarantool auth packet
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
    pub sql_metadata: Option<Bytes>,
    pub sql_info: Option<Bytes>,
}

pub struct TarantoolSqlResponse {
    response: TarantoolResponse,
}

pub type UntypedRow = Vec<Value>;

#[derive(Debug, PartialEq)]
pub enum SqlMetaType {
    boolean,
    integer,
    unsigned,
    number,
    string,
    varbinary,
    scalar,
    unknown(String),
}

impl<'de> Deserialize<'de> for SqlMetaType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "boolean" => SqlMetaType::boolean,
            "integer" => SqlMetaType::integer,
            "unsigned" => SqlMetaType::unsigned,
            "number" => SqlMetaType::number,
            "string" => SqlMetaType::string,
            "varbinary" => SqlMetaType::varbinary,
            "scalar" => SqlMetaType::scalar,

            v => SqlMetaType::unknown(String::from(v)),
        })
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SqlResultMetadataFieldInfo {
    name: String,
    sql_type: SqlMetaType,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SqlResultMetadata {
    pub fields: Option<Vec<SqlResultMetadataFieldInfo>>,
    pub row_count: Option<u64>,
    pub auto_increment_ids: Option<Vec<u64>>,
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
    CALL = 0x0a,
    PING = 0x040,
    SUBSCRIBE = 0x066,
    EXECUTE = 0x0b,
    // PREPARE = 0x0d,
}

#[derive(Debug, Clone)]
pub enum SqlInfo {
    SQL_INFO_ROW_COUNT = 0x00,
    SQL_INFO_AUTO_INCREMENT_IDS = 0x01,
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
    METADATA = 0x32,
    SQL_INFO = 0x42,

    STMT_ID = 0x43,
    SQL_TEXT = 0x40,
    SQL_BIND = 0x41,
    OPTIONS = 0x2b,
}

impl TarantoolResponse {
    pub fn new_short_response(code: u64, data: Bytes) -> TarantoolResponse {
        TarantoolResponse {
            code,
            data,
            sql_info: None,
            sql_metadata: None,
        }
    }

    pub fn new_full_response(
        code: u64,
        data: Bytes,
        sql_metadata: Option<Bytes>,
        sql_info: Option<Bytes>,
    ) -> TarantoolResponse {
        TarantoolResponse {
            code,
            data,
            sql_info,
            sql_metadata,
        }
    }

    /// decode tarantool response to any serder deserializable struct
    pub fn decode<'de, T>(self) -> io::Result<T>
    where
        T: Deserialize<'de>,
    {
        tools::decode_serde(Cursor::new(self.data))
    }

    /// decode tarantool response to any serder deserializable struct
    pub fn decode_result_set<'de, T>(self) -> io::Result<Vec<T>>
    where
        T: Deserialize<'de>,
    {
        tools::decode_serde(Cursor::new(self.data))
    }

    /// decode tarantool response to tuple wih one element and return this element
    pub fn decode_single<'de, T>(self) -> io::Result<T>
    where
        T: Deserialize<'de>,
    {
        let (res,) = tools::decode_serde(Cursor::new(self.data))?;
        Ok(res)
    }

    /// decode tarantool response to tuple of two elements
    pub fn decode_pair<'de, T1, T2>(self) -> io::Result<(T1, T2)>
    where
        T1: Deserialize<'de>,
        T2: Deserialize<'de>,
    {
        Ok(tools::decode_serde(Cursor::new(self.data))?)
    }

    ///decode tarantool response to three elements
    pub fn decode_trio<'de, T1, T2, T3>(self) -> io::Result<(T1, T2, T3)>
    where
        T1: Deserialize<'de>,
        T2: Deserialize<'de>,
        T3: Deserialize<'de>,
    {
        let (r1, r2, r3) = tools::decode_serde(Cursor::new(self.data))?;
        Ok((r1, r2, r3))
    }
}

impl Into<TarantoolSqlResponse> for TarantoolResponse {
    fn into(self) -> TarantoolSqlResponse {
        TarantoolSqlResponse { response: self }
    }
}

impl TarantoolSqlResponse {
    /// decode tarantool response to any serder deserializable struct
    pub fn decode_result_set<'de, T>(self) -> io::Result<Vec<T>>
    where
        T: Deserialize<'de>,
    {
        tools::decode_serde(Cursor::new(self.response.data))
    }

    ///decode rows to vec of columns
    pub fn decode_untyped_result_set(self) -> io::Result<Vec<UntypedRow>> {
        tools::decode_serde(Cursor::new(self.response.data))
    }

    ///result set metadata
    pub fn metadata(&self) -> SqlResultMetadata {
        let sql_info: Option<HashMap<u8, Value>> =
            tools::decode_serde_optional(&self.response.sql_info);
        println!("sql_info={:?}", sql_info);
        let sql_info_fields = sql_info
            .map(|mut info| {
                (
                    info.remove(&(SqlInfo::SQL_INFO_ROW_COUNT as u8))
                        .and_then(|v| v.as_u64()),
                    info.remove(&(SqlInfo::SQL_INFO_AUTO_INCREMENT_IDS as u8))
                        .and_then(|val| {
                            val.as_array()
                                .map(|arr| arr.iter().flat_map(|e| e.as_u64()).collect())
                        }),
                )
            })
            .unwrap_or((None, None));

        let fields: Option<Vec<SqlResultMetadataFieldInfo>> =
            tools::decode_serde_optional(&self.response.sql_metadata);
        SqlResultMetadata {
            fields,
            row_count: sql_info_fields.0,
            auto_increment_ids: sql_info_fields.1,
        }
    }
}

impl CommandPacket {
    pub fn call<T>(function: &str, params: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        CommandPacket::call_raw(function, tools::serialize_to_vec_u8(params)?)
    }

    pub fn call_raw(function: &str, params: Vec<u8>) -> io::Result<CommandPacket> {
        Ok(CommandPacket {
            code: Code::CALL,
            internal_fields: vec![(Key::FUNCTION, Value::from(function))],
            command_field: vec![(Key::TUPLE, params)],
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

    pub fn exec_sql<T>(sql: &str, args: &T) -> io::Result<CommandPacket>
    where
        T: Serialize,
    {
        CommandPacket::exec_sql_raw(sql, tools::serialize_to_vec_u8(args)?)
    }

    pub fn exec_sql_raw(sql: &str, args_raw: Vec<u8>) -> io::Result<CommandPacket> {
        Ok(CommandPacket {
            code: Code::EXECUTE,
            internal_fields: vec![(Key::SQL_TEXT, Value::from(sql))],
            command_field: vec![
                (Key::SQL_BIND, args_raw),
                (Key::OPTIONS, tools::serialize_to_vec_u8(&())?),
            ],
        })
    }

    // pub fn prepare_stmt(sql: String) -> io::Result<CommandPacket>
    // {
    //     Ok(CommandPacket {
    //         code: Code::PREPARE,
    //         internal_fields: vec![(Key::SQL_TEXT, Value::from(sql))],
    //         command_field: vec![],
    //     })
    // }
}
