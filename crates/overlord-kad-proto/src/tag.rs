use std::io::{Read, Seek};

use binrw::{BinRead, BinReaderExt, BinResult, BinWrite, BinWriterExt, Endian};
use encoding_rs::WINDOWS_1252;

use crate::constants::tag_name;
use crate::error::ProtoError;
use crate::hash::Ed2kHash;

#[derive(Debug, Clone, PartialEq)]
pub enum TagName {
    Short(u8),
    Long(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TagValue {
    Hash(Ed2kHash),
    String(String),
    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),
    Float(f32),
    Bool(bool),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Tag {
    pub name: TagName,
    pub value: TagValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StringDecodeMode {
    LossyUtf8,
    SearchResult,
}

fn decode_string_bytes(bytes: &[u8], mode: StringDecodeMode) -> String {
    match mode {
        StringDecodeMode::LossyUtf8 => String::from_utf8_lossy(bytes).into_owned(),
        // eMule/aMule special-case SEARCH_RES string decoding: try UTF-8 first and
        // only then fall back to the local ANSI code page for legacy display-only data.
        // Reference:
        // - eMule srchybrid/kademlia/io/DataIO.cpp CDataIO::ReadStringUTF8(bool bOptACP)
        // - eMule srchybrid/kademlia/net/KademliaUDPListener.cpp Process_KADEMLIA2_SEARCH_RES
        // - aMule src/kademlia/net/KademliaUDPListener.cpp ProcessSearchResponse
        StringDecodeMode::SearchResult => match std::str::from_utf8(bytes) {
            Ok(s) => s.to_owned(),
            Err(_) => decode_legacy_search_result_string(bytes),
        },
    }
}

#[cfg(windows)]
fn decode_legacy_search_result_string(bytes: &[u8]) -> String {
    use windows_sys::Win32::Globalization::{CP_ACP, MultiByteToWideChar};

    if bytes.is_empty() {
        return String::new();
    }

    let src_len = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
    let wide_len =
        unsafe { MultiByteToWideChar(CP_ACP, 0, bytes.as_ptr(), src_len, std::ptr::null_mut(), 0) };
    if wide_len <= 0 {
        return WINDOWS_1252.decode(bytes).0.into_owned();
    }

    let mut wide = vec![0u16; wide_len as usize];
    let converted = unsafe {
        MultiByteToWideChar(
            CP_ACP,
            0,
            bytes.as_ptr(),
            src_len,
            wide.as_mut_ptr(),
            wide_len,
        )
    };
    if converted <= 0 {
        return WINDOWS_1252.decode(bytes).0.into_owned();
    }

    String::from_utf16_lossy(&wide[..converted as usize])
}

#[cfg(not(windows))]
fn decode_legacy_search_result_string(bytes: &[u8]) -> String {
    WINDOWS_1252.decode(bytes).0.into_owned()
}

impl Tag {
    pub fn new_short(name_byte: u8, value: TagValue) -> Self {
        Tag {
            name: TagName::Short(name_byte),
            value,
        }
    }

    pub fn new_long(name: impl Into<String>, value: TagValue) -> Self {
        Tag {
            name: TagName::Long(name.into()),
            value,
        }
    }

    pub fn filename(name: impl Into<String>) -> Self {
        Tag::new_short(tag_name::FILENAME, TagValue::String(name.into()))
    }

    pub fn filesize(size: u64) -> Self {
        Tag::new_short(tag_name::FILESIZE, TagValue::U64(size))
    }

    pub fn filetype(t: impl Into<String>) -> Self {
        Tag::new_short(tag_name::FILETYPE, TagValue::String(t.into()))
    }

    pub fn sources(n: u32) -> Self {
        Tag::new_short(tag_name::SOURCES, TagValue::U32(n))
    }
}

/// Map TagValue to its raw type byte (without the 0x80 name flag).
fn value_type_byte(v: &TagValue) -> u8 {
    match v {
        TagValue::Hash(_) => 0x01,
        TagValue::String(_) => 0x02,
        TagValue::U32(_) => 0x03,
        TagValue::Float(_) => 0x04,
        TagValue::Bool(_) => 0x05,
        TagValue::Blob(_) => 0x07,
        TagValue::U16(_) => 0x08,
        TagValue::U8(_) => 0x09,
        TagValue::U64(_) => 0x0B,
    }
}

impl BinRead for Tag {
    type Args<'a> = ();

    fn read_options<R: Read + Seek>(reader: &mut R, endian: Endian, _args: ()) -> BinResult<Self> {
        Self::read_with_mode(reader, endian, StringDecodeMode::LossyUtf8)
    }
}

impl Tag {
    pub(crate) fn read_with_mode<R: Read + Seek>(
        reader: &mut R,
        endian: Endian,
        mode: StringDecodeMode,
    ) -> BinResult<Self> {
        let type_byte: u8 = reader.read_type(endian)?;
        let short_name = (type_byte & 0x80) != 0;
        let tag_type = type_byte & 0x7F;

        let name = if short_name {
            let name_byte: u8 = reader.read_type(endian)?;
            TagName::Short(name_byte)
        } else {
            let name_len: u16 = reader.read_type(endian)?;
            let mut name_bytes = vec![0u8; name_len as usize];
            reader.read_exact(&mut name_bytes).map_err(|e| {
                let _pos = reader.stream_position().unwrap_or(0);
                binrw::Error::Io(std::io::Error::new(e.kind(), e.to_string()))
            })?;
            // eMule often writes single-byte numeric IDs as a 1-byte "long" name
            // instead of using the 0x80 short-name flag. Normalize to Short.
            if name_bytes.len() == 1 {
                TagName::Short(name_bytes[0])
            } else {
                TagName::Long(decode_string_bytes(&name_bytes, mode))
            }
        };

        let value = match tag_type {
            0x01 => {
                // Hash16
                let h = Ed2kHash::read_options(reader, endian, ())?;
                TagValue::Hash(h)
            }
            0x02 => {
                // String: u16 length + bytes
                let str_len: u16 = reader.read_type(endian)?;
                let mut str_bytes = vec![0u8; str_len as usize];
                reader
                    .read_exact(&mut str_bytes)
                    .map_err(|e| binrw::Error::Io(std::io::Error::new(e.kind(), e.to_string())))?;
                TagValue::String(decode_string_bytes(&str_bytes, mode))
            }
            0x03 => {
                let v: u32 = reader.read_type(endian)?;
                TagValue::U32(v)
            }
            0x04 => {
                let v: f32 = reader.read_type(endian)?;
                TagValue::Float(v)
            }
            0x05 => {
                let v: u8 = reader.read_type(endian)?;
                TagValue::Bool(v != 0)
            }
            0x06 => {
                // BOOLARRAY: u16 len + skip bytes => store as Blob
                let arr_len: u16 = reader.read_type(endian)?;
                let byte_count = (arr_len as usize).div_ceil(8);
                let mut data = vec![0u8; byte_count];
                reader
                    .read_exact(&mut data)
                    .map_err(|e| binrw::Error::Io(std::io::Error::new(e.kind(), e.to_string())))?;
                TagValue::Blob(data)
            }
            0x07 => {
                // BLOB: u32 len + bytes
                let blob_len: u32 = reader.read_type(endian)?;
                let mut data = vec![0u8; blob_len as usize];
                reader
                    .read_exact(&mut data)
                    .map_err(|e| binrw::Error::Io(std::io::Error::new(e.kind(), e.to_string())))?;
                TagValue::Blob(data)
            }
            0x08 => {
                let v: u16 = reader.read_type(endian)?;
                TagValue::U16(v)
            }
            0x09 => {
                let v: u8 = reader.read_type(endian)?;
                TagValue::U8(v)
            }
            0x0A => {
                // BSOB: u8 len + skip bytes => store as Blob
                let bsob_len: u8 = reader.read_type(endian)?;
                let mut data = vec![0u8; bsob_len as usize];
                reader
                    .read_exact(&mut data)
                    .map_err(|e| binrw::Error::Io(std::io::Error::new(e.kind(), e.to_string())))?;
                TagValue::Blob(data)
            }
            0x0B => {
                let v: u64 = reader.read_type(endian)?;
                TagValue::U64(v)
            }
            other => {
                let pos = reader.stream_position().unwrap_or(0);
                return Err(binrw::Error::Custom {
                    pos,
                    err: Box::new(ProtoError::UnknownTagType(other)),
                });
            }
        };

        Ok(Tag { name, value })
    }
}

impl BinWrite for Tag {
    type Args<'a> = ();

    fn write_options<W: std::io::Write + std::io::Seek>(
        &self,
        writer: &mut W,
        endian: Endian,
        _args: (),
    ) -> BinResult<()> {
        let raw_type = value_type_byte(&self.value);
        let short = matches!(&self.name, TagName::Short(_));
        let type_byte: u8 = if short { raw_type | 0x80 } else { raw_type };

        writer.write_type(&type_byte, endian)?;

        match &self.name {
            TagName::Short(b) => {
                writer.write_type(b, endian)?;
            }
            TagName::Long(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as u16;
                writer.write_type(&len, endian)?;
                writer.write_all(bytes).map_err(binrw::Error::Io)?;
            }
        }

        match &self.value {
            TagValue::Hash(h) => {
                h.write_options(writer, endian, ())?;
            }
            TagValue::String(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as u16;
                writer.write_type(&len, endian)?;
                writer.write_all(bytes).map_err(binrw::Error::Io)?;
            }
            TagValue::U32(v) => {
                writer.write_type(v, endian)?;
            }
            TagValue::Float(v) => {
                writer.write_type(v, endian)?;
            }
            TagValue::Bool(v) => {
                let b: u8 = if *v { 1 } else { 0 };
                writer.write_type(&b, endian)?;
            }
            TagValue::Blob(data) => {
                let len = data.len() as u32;
                writer.write_type(&len, endian)?;
                writer.write_all(data).map_err(binrw::Error::Io)?;
            }
            TagValue::U16(v) => {
                writer.write_type(v, endian)?;
            }
            TagValue::U8(v) => {
                writer.write_type(v, endian)?;
            }
            TagValue::U64(v) => {
                writer.write_type(v, endian)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binrw::{BinRead, BinWrite};
    use std::io::Cursor;

    fn roundtrip(tag: &Tag) -> Tag {
        let mut buf = Cursor::new(Vec::new());
        tag.write_le(&mut buf).unwrap();
        buf.set_position(0);
        Tag::read_le(&mut buf).unwrap()
    }

    #[test]
    fn test_short_name_string() {
        let t = Tag::filename("hello.txt");
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_short_name_u64() {
        let t = Tag::filesize(1234567890);
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_short_name_u32() {
        let t = Tag::sources(42);
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_long_name_string() {
        let t = Tag::new_long("my-tag", TagValue::String("value".to_string()));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_hash_value() {
        let h = Ed2kHash::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let t = Tag::new_short(0x01, TagValue::Hash(h));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_u16_value() {
        let t = Tag::new_short(0x22, TagValue::U16(65000));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_u8_value() {
        let t = Tag::new_short(0x20, TagValue::U8(7));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_float_value() {
        let t = Tag::new_short(0x10, TagValue::Float(std::f32::consts::PI));
        let t2 = roundtrip(&t);
        // Float comparison needs tolerance
        if let (TagValue::Float(a), TagValue::Float(b)) = (&t.value, &t2.value) {
            assert!((a - b).abs() < 1e-5);
        } else {
            panic!("expected float");
        }
    }

    #[test]
    fn test_bool_value() {
        let t_true = Tag::new_short(0x05, TagValue::Bool(true));
        let t_false = Tag::new_short(0x05, TagValue::Bool(false));
        assert_eq!(roundtrip(&t_true), t_true);
        assert_eq!(roundtrip(&t_false), t_false);
    }

    #[test]
    fn test_blob_value() {
        let t = Tag::new_short(0x07, TagValue::Blob(vec![0xAA, 0xBB, 0xCC]));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_long_name_u32() {
        let t = Tag::new_long("bitrate", TagValue::U32(320));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }

    #[test]
    fn test_long_name_u64() {
        let t = Tag::new_long("filesize", TagValue::U64(u64::MAX));
        let t2 = roundtrip(&t);
        assert_eq!(t, t2);
    }
}
