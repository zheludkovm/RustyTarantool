use crate::tarantool::packets::{Code, Key, TarantoolRequest, TarantoolResponse};
use crate::tarantool::tools::{
    decode_serde, get_map_value, make_auth_digest, map_err_to_io, parse_msgpack_map,
    serialize_to_buf_mut, write_u32_to_slice, SafeBytesMutWriter,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rmp::encode;
use rmpv::{self, decode, Value};
use std::io;
use std::io::Cursor;
use std::str;
use tokio_util::codec::{Decoder, Encoder};

pub type RequestId = u64;
pub type TarantoolFramedRequest = (RequestId, TarantoolRequest);

const GREETINGS_HEADER_LENGTH: usize = 9;
const GREETINGS_HEADER: &str = "Tarantool";

///
/// Tokio framed codec for tarantool
/// use it if you want manually set it on your tokio framed transport
///
#[derive(Debug)]
pub struct TarantoolCodec {
    is_greetings_received: bool,
    salt: Option<Vec<u8>>,
}

impl Default for TarantoolCodec {
    fn default() -> Self {
        TarantoolCodec {
            is_greetings_received: false,
            salt: None,
        }
    }
}

impl Decoder for TarantoolCodec {
    type Item = (RequestId, io::Result<TarantoolResponse>);
    type Error = io::Error;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> io::Result<Option<(RequestId, io::Result<TarantoolResponse>)>> {
        if !self.is_greetings_received {
            if buf.len() < 128 {
                Ok(None)
            } else {
                self.is_greetings_received = true;
                decode_greetings(self, buf)
            }
        } else if buf.len() < 5 {
            Ok(None)
        } else {
            let size: usize = decode_serde(&buf[0..5])?;

            if buf.len() - 5 < size {
                Ok(None)
            } else {
                Ok(Some(parse_response(buf, size)?))
            }
        }
    }
}

fn parse_response(
    buf: &mut BytesMut,
    size: usize,
) -> io::Result<(RequestId, io::Result<TarantoolResponse>)> {
    //    buf.split_to(5);
    buf.advance(5);
    let response_body = buf.split_to(size);
    let mut r = Cursor::new(response_body);

    let headers = decode::read_value(&mut r).map_err(map_err_to_io)?;
    let (code, sync) = parse_headers(headers)?;
    let mut response_fields = parse_msgpack_map(r)?;

    match code {
        0 => {
            Ok((
                sync,
                Ok(TarantoolResponse::new_full_response(
                    code,
                    response_fields
                        .remove(&(Key::DATA as u64))
                        .unwrap_or_default(),
                    response_fields.remove(&(Key::METADATA as u64)),
                    response_fields.remove(&(Key::SQL_INFO as u64)), // search_key_in_msgpack_map(r, Key::DATA as u64)?,
                )),
            ))
        }
        _ => {
            let response_data = TarantoolResponse::new_short_response(
                code,
                response_fields
                    .remove(&(Key::ERROR as u64))
                    .unwrap_or_default(),
            );
            let s: String = response_data.decode()?;
            error!("Tarantool ERROR >> {:?}", s);
            Ok((sync, Err(io::Error::new(io::ErrorKind::Other, s))))
        }
    }
}

pub fn parse_headers(headers: Value) -> Result<(u64, u64), io::Error> {
    match headers {
        Value::Map(headers_vec) => Ok((
            get_map_value(&headers_vec, Key::CODE as u64)?,
            get_map_value(&headers_vec, Key::SYNC as u64)?,
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            "Incorrect headers msg pack type!",
        )),
    }
}

impl Encoder<(RequestId, TarantoolRequest)> for TarantoolCodec {
    // type Item = ;
    type Error = io::Error;

    fn encode(
        &mut self,
        command: (RequestId, TarantoolRequest),
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match command {
            (sync_id, TarantoolRequest::Auth(packet)) => {
                info!("send auth_packet={:?}", packet);
                let digest =
                    make_auth_digest(self.salt.clone().unwrap(), packet.password.as_bytes())?;

                create_packet(
                    dst,
                    Code::AUTH,
                    sync_id,
                    None,
                    vec![
                        (Key::USER_NAME, Value::from(packet.login)),
                        (
                            Key::TUPLE,
                            Value::Array(vec![
                                Value::from("chap-sha1"),
                                Value::from(&digest as &[u8]),
                            ]),
                        ),
                    ],
                    vec![],
                )
            }
            (sync_id, TarantoolRequest::Command(packet)) => {
                debug!("send normal packet={:?}", packet);
                create_packet(
                    dst,
                    packet.code,
                    sync_id,
                    None,
                    packet.internal_fields,
                    packet.command_field,
                )
            }
        }
    }
}

fn decode_greetings(
    codec: &mut TarantoolCodec,
    buf: &mut BytesMut,
) -> io::Result<Option<(RequestId, io::Result<TarantoolResponse>)>> {
    let header = buf.split_to(GREETINGS_HEADER_LENGTH);
    let test = str::from_utf8(&header).map_err(map_err_to_io)?;

    let res = match test {
        GREETINGS_HEADER => Ok(Some((
            0,
            Ok(TarantoolResponse::new_short_response(0, Bytes::new())),
        ))),
        _ => Err(io::Error::new(io::ErrorKind::Other, "Unknown header!")),
    };
    //    buf.split_to(64 - GREETINGS_HEADER_LENGTH);
    buf.advance(64 - GREETINGS_HEADER_LENGTH);
    let salt_buf = buf.split_to(64);
    codec.salt = Some(salt_buf.to_vec());

    res
}

fn create_packet(
    buf: &mut BytesMut,
    code: Code,
    sync_id: u64,
    schema_id: Option<u64>,
    data: Vec<(Key, Value)>,
    additional_data: Vec<(Key, Vec<u8>)>,
) -> io::Result<()> {
    let mut header_vec = vec![
        (Value::from(Key::CODE as u8), Value::from(code as u8)),
        (Value::from(Key::SYNC as u8), Value::from(sync_id)),
    ];

    if let Some(schema_id_v) = schema_id {
        header_vec.push((Value::from(Key::SCHEMA_ID as u8), Value::from(schema_id_v)))
    }

    buf.reserve(5);
    let start_position = buf.len() + 1;
    buf.put_slice(&[0xce, 0x00, 0x00, 0x00, 0x00]);
    {
        let mut writer = SafeBytesMutWriter::writer(buf);

        serialize_to_buf_mut(&mut writer, &Value::Map(header_vec))?;
        encode::write_map_len(
            &mut writer,
            data.len() as u32 + (additional_data.len() as u32),
        )?;
        for (ref key, ref val) in data {
            rmpv::encode::write_value(&mut writer, &Value::from((*key) as u8))?;
            rmpv::encode::write_value(&mut writer, val)?;
        }
        for (ref key, ref val) in additional_data {
            rmpv::encode::write_value(&mut writer, &Value::from((*key) as u8))?;
            io::Write::write(&mut writer, val)?;
        }
    }

    let len = (buf.len() - start_position - 4) as u32;
    write_u32_to_slice(&mut buf[start_position..start_position + 4], len);

    Ok(())
}
