//901-36-89-47-8
use base64;
use byteorder::ReadBytesExt;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rmp_serde::{Deserializer, Serializer};
use rmp::encode;
use rmpv::decode;
use rmpv::Value;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use std::error;
use std::io;
use std::io::{Cursor};
use std::collections::HashMap;

pub fn decode_serde_optional<'de, T>( data: &Option<Bytes>) -> Option<T>
    where T:Deserialize<'de>
{
    data.as_ref()
        .and_then(|meta_bytes| {
            let res: io::Result<T> = decode_serde(Cursor::new(meta_bytes));
            res.ok()
        })
}

pub fn decode_serde<'de, T, R>(r: R) -> io::Result<T>
where
    T: Deserialize<'de>,
    R: io::Read,
{
    Deserialize::deserialize(&mut Deserializer::new(r)).map_err(map_err_to_io)
}

pub fn serialize_to_buf_mut<W: io::Write, S: Serialize>(wr: &mut W, v: &S) -> io::Result<()> {
    v.serialize(&mut Serializer::new(wr)).map_err(map_err_to_io)
}

pub fn serialize_to_vec_u8<S: Serialize>(v: &S) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    serialize_to_buf_mut(&mut buf, v)?;
    Ok(buf)
}

pub fn serialize_one_element_map(name:String, value: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    let mut writer = SafeBytesMutWriter::writer(&mut buf);

    encode::write_map_len(
        &mut writer,
        1,
    )?;
    encode::write_str(&mut writer, name.as_str())?;
    io::Write::write(&mut writer, &value)?;
    Ok(buf.to_vec())
}

pub fn serialize_array(args: &Vec<Vec<u8>>) -> io::Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    let mut writer = SafeBytesMutWriter::writer(&mut buf);

    encode::write_array_len(
        &mut writer,
        args.len() as u32 ,
    )?;
    for ref arg in args {
        io::Write::write(&mut writer, arg)?;
    }
    Ok(buf.to_vec())
}

pub fn map_err_to_io<E>(e: E) -> io::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    let errr = e.into().to_string();
    error!("Error! {:?}", errr);
    io::Error::new(io::ErrorKind::Other, errr)
}

#[allow(dead_code)]
pub fn make_map_err_to_io() -> io::Error {
    error!("make Error! ");
    io::Error::new(io::ErrorKind::Other, "Cant get key from map!")
}

pub fn parse_msgpack_map(rr: Cursor<BytesMut>) -> io::Result<HashMap<u64,Bytes>> {
    let pos = rr.position();
    let mut bytes_mut = rr.into_inner();
    let positions = get_entries_positions(pos, bytes_mut.as_ref())?;
    let mut result: HashMap<u64, Bytes> = HashMap::new();

    let mut counter:usize = 0;
    for (key, (start, end)) in positions {
        bytes_mut.advance(start-counter);
        let value = bytes_mut.split_to(end-start);
        result.insert(key, value.freeze());
        counter = end;
    }

    Ok(result)
}

fn get_entries_positions(pos: u64, inner: &[u8]) -> io::Result<Vec<(u64, (usize, usize))>> {
    let mut r = Cursor::new(inner);
    r.set_position(pos);
    let mut result: Vec<(u64, (usize, usize))> = Vec::new();
    r.read_u8()?;
    while r.remaining() != 0 {
        let key = decode::read_value_ref(&mut r).map_err(map_err_to_io)?;
        let start = r.position();
        let _value = decode::read_value_ref(&mut r).map_err(map_err_to_io)?;
        let end = r.position();
        result.push((key.as_u64().unwrap(), (start as usize, end as usize)));
    }
    Ok(result)
}

// pub fn search_key_in_msgpack_map(r: Cursor<BytesMut>, search_key: u64) -> io::Result<Bytes> {
//     Ok(parse_msgpack_map(r)?.remove(&search_key).unwrap_or(Bytes::new()))
// }

// pub fn search_key_in_msgpack_map1(mut r: Cursor<BytesMut>, search_key: u64) -> io::Result<Bytes> {
//     r.read_u8()?;
//     if r.remaining() == 0 {
//         Ok(Bytes::new())
//     } else {
//         while r.remaining() != 0 {
//             let key = decode::read_value(&mut r).map_err(map_err_to_io)?;
//             match key {
//                 Value::Integer(k) if k.is_u64() && k.as_u64().unwrap() == search_key => {
//                     let pos = r.position();
//                     let mut res_buf = r.into_inner();
//                     //                    res_buf.split_to(pos as usize);
//                     res_buf.advance(pos as usize);
//                     return Ok(res_buf.freeze());
//                 }
//                 _ => {}
//             }
//         }
//         Err(io::Error::new(
//             io::ErrorKind::Other,
//             "Incorrect headers msg pack type!",
//         ))
//     }
// }

pub fn get_map_value(map: &Vec<(Value, Value)>, key: u64) -> io::Result<u64> {
    map.iter()
        .filter_map(|row| match row {
            &(Value::Integer(row_key), Value::Integer(val))
                if row_key.as_u64() == Some(key) && val.is_u64() =>
            {
                val.as_u64()
            }
            _ => None,
        })
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Not found header !"))
}

#[allow(dead_code)]
pub fn transform_u32_to_array_of_u8(x: u32) -> [u8; 4] {
    let b1: u8 = ((x >> 24) & 0xff) as u8;
    let b2: u8 = ((x >> 16) & 0xff) as u8;
    let b3: u8 = ((x >> 8) & 0xff) as u8;
    let b4: u8 = (x & 0xff) as u8;
    [b1, b2, b3, b4]
}

pub fn write_u32_to_slice(buf: &mut [u8], x: u32) {
    buf[0] = ((x >> 24) & 0xff) as u8;
    buf[1] = ((x >> 16) & 0xff) as u8;
    buf[2] = ((x >> 8) & 0xff) as u8;
    buf[3] = (x & 0xff) as u8;
}

pub fn make_auth_digest(salt_bytes: Vec<u8>, password: &[u8]) -> io::Result<[u8; 20]> {
    let mut sha1 = Sha1::new();
    sha1.update(password);
    let digest1 = sha1.digest().bytes();
    sha1.reset();

    sha1.update(&digest1);
    let digest2 = sha1.digest().bytes();

    sha1.reset();
    let salt_str = String::from_utf8(salt_bytes).map_err(map_err_to_io)?;

    let decoded_salt = &base64::decode(&salt_str.trim()).map_err(map_err_to_io)?[..20];
    sha1.update(&decoded_salt);
    sha1.update(&digest2);
    let digest3 = sha1.digest().bytes();

    let mut digest4: [u8; 20] = [0; 20];

    for (i, digest1_b) in digest1.iter().enumerate() {
        digest4[i] = digest1_b ^ digest3[i];
    }
    Ok(digest4)
}

//safe buf mut writer

#[derive(Debug)]
pub struct SafeBytesMutWriter<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> SafeBytesMutWriter<'a> {
    pub fn writer(buf: &'a mut BytesMut) -> SafeBytesMutWriter {
        Self { buf }
    }
}

impl<'a> io::Write for SafeBytesMutWriter<'a> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.buf.reserve(src.len());
        self.buf.put(src);
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
