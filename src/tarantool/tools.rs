use std::io;
use std::error;
use byteorder::{ReadBytesExt};

use rmpv::{Value};
use rmp_serde::{Serializer, Deserializer};
use rmpv::decode;
use serde::{Serialize, Deserialize};
use sha1::Sha1;
use base64;


pub fn decode_serde<'de, T, R>(r:R) -> io::Result<T>
    where T: Deserialize<'de>,R: io::Read
{
    Deserialize::deserialize(&mut  Deserializer::new(r)).map_err(map_err_to_io)
}


pub fn serialize_to_buf_mut<W: io::Write, S: Serialize>(wr: &mut W, v: &S) -> io::Result<()> {
    v.serialize(&mut Serializer::new(wr)).map_err(map_err_to_io)
}

pub fn serialize_to_vec_u8<S: Serialize>(v: &S) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    serialize_to_buf_mut(&mut buf, v)?;
    Ok(buf)
}

pub fn map_err_to_io<E>(e: E) -> io::Error
    where E: Into<Box<error::Error + Send + Sync>>
{
    error!("Error! {:?}",e.into());
    io::Error::new(io::ErrorKind::Other, "")
}

pub fn make_map_err_to_io() -> io::Error
{
    error!("make Error! ");
    io::Error::new(io::ErrorKind::Other, "Cant get key from map!")
}

pub fn search_key_in_msgpack_map(r: &mut &[u8], search_key: u64) -> io::Result<Vec<u8>> {
    r.read_u8()?;
    if r.is_empty() {
        Ok(Vec::new())
    } else {
        while !r.is_empty() {
            let key = decode::read_value(r).map_err(map_err_to_io)?;
            match key {
                Value::Integer(k) if k.is_u64() && k.as_u64().unwrap() == search_key => {
                    return Ok(Vec::from(*r));
                }
                _ => {}
            }
        }
        Err(io::Error::new(io::ErrorKind::Other, "Incorrect headers msg pack type!"))
    }
}


pub fn get_map_value(map: &Vec<(Value, Value)>, key: u64) -> io::Result<u64> {
    for row in map {
        if let &(Value::Integer(row_key), Value::Integer(val)) = row {
            match (row_key.as_u64(), val.as_u64()) {
                (Some(key64), Some(value64)) if key64 == key => {
                    return Ok(value64);
                }
                _ => {}
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::Other, "Not found header !"))
}

pub fn transform_u32_to_array_of_u8(x: u32) -> [u8; 4] {
    let b1: u8 = ((x >> 24) & 0xff) as u8;
    let b2: u8 = ((x >> 16) & 0xff) as u8;
    let b3: u8 = ((x >> 8) & 0xff) as u8;
    let b4: u8 = (x & 0xff) as u8;
    [b1, b2, b3, b4]
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