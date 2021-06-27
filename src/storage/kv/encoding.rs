use crate::{Error, Result};

use std::convert::TryInto;

pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(bytes.len() + 2);
    encoded.extend(
        bytes
            .iter()
            .flat_map(|b| match b {
                0x00 => vec![0x00, 0xff],
                b => vec![*b],
            })
            .chain(vec![0x00, 0x00]),
    );
    encoded
}

pub fn take_byte(bytes: &mut &[u8]) -> Result<u8> {
    if bytes.is_empty() {
        return Err(Error::Internal("Unexpected end of bytes".to_owned()));
    }
    let b = bytes[0];
    *bytes = &bytes[1..];
    Ok(b)
}

pub fn take_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    let mut iter = bytes.iter().enumerate();
    let taken = loop {
        match iter.next().map(|(_, b)| b) {
            Some(0x00) => match iter.next() {
                Some((i, 0x00)) => break i + 1,
                Some((_, 0xff)) => decoded.push(0x00),
                Some((_, b)) => {
                    return Err(Error::InvalidArgument(format!(
                        "Invalid byte escape {:?}",
                        b
                    )))
                }
                None => return Err(Error::InvalidArgument("Unexpected end of bytes".into())),
            },
            Some(b) => decoded.push(*b),
            None => return Err(Error::InvalidArgument("Unexpected end of bytes".into())),
        }
    };
    *bytes = &bytes[taken..];
    Ok(decoded)
}

pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

pub fn decode_u64(bytes: [u8; 8]) -> u64 {
    u64::from_be_bytes(bytes)
}

pub fn take_u64(bytes: &mut &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(Error::Internal(format!(
            "Unexpected buffer length {} < 8",
            bytes.len()
        )));
    }
    let n = decode_u64(bytes[0..8].try_into()?);
    *bytes = &bytes[8..];
    Ok(n)
}
