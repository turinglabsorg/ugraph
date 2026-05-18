use num_bigint::{BigInt, BigUint};
use serde::Serialize;
use thiserror::Error;

use crate::AbiEventInput;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("invalid hex value: {0}")]
    InvalidHex(String),
    #[error("missing indexed topic for ABI input {0}")]
    MissingTopic(String),
    #[error("missing ABI data word at index {0}")]
    MissingDataWord(usize),
    #[error("invalid ABI dynamic offset {0}")]
    InvalidDynamicOffset(usize),
    #[error("invalid ABI dynamic length {0}")]
    InvalidDynamicLength(usize),
    #[error("unsupported ABI type: {0}")]
    UnsupportedType(String),
}

#[derive(Clone, Debug, Serialize)]
pub struct DecodedEventParam {
    pub name: Option<String>,
    pub kind: String,
    pub indexed: bool,
    pub value: DecodedValue,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "value")]
pub enum DecodedValue {
    Address(String),
    Bool(bool),
    Bytes(String),
    Int(String),
    String(String),
    TopicHash(String),
    Uint(String),
}

pub fn decode_event_params(
    inputs: &[AbiEventInput],
    topics: &[String],
    data: &str,
) -> Result<Vec<DecodedEventParam>, DecodeError> {
    let data = decode_hex(data)?;
    let mut indexed_topic_index = 1;
    let mut data_word_index = 0;
    let mut params = Vec::with_capacity(inputs.len());

    for input in inputs {
        if input.indexed {
            let topic = topics
                .get(indexed_topic_index)
                .ok_or_else(|| DecodeError::MissingTopic(input.kind.clone()))?;
            indexed_topic_index += 1;
            let topic_word = decode_topic(topic)?;
            params.push(DecodedEventParam {
                name: input.name.clone(),
                kind: input.kind.clone(),
                indexed: true,
                value: decode_indexed_word(&input.kind, &topic_word, topic)?,
            });
        } else {
            let word = data_word(&data, data_word_index)?;
            let value = if is_dynamic_type(&input.kind) {
                decode_dynamic_value(&input.kind, &data, word)?
            } else {
                decode_static_word(&input.kind, word)?
            };
            data_word_index += 1;
            params.push(DecodedEventParam {
                name: input.name.clone(),
                kind: input.kind.clone(),
                indexed: false,
                value,
            });
        }
    }

    Ok(params)
}

fn decode_indexed_word(kind: &str, word: &[u8], topic: &str) -> Result<DecodedValue, DecodeError> {
    if is_dynamic_type(kind) {
        Ok(DecodedValue::TopicHash(topic.to_lowercase()))
    } else {
        decode_static_word(kind, word)
    }
}

fn decode_static_word(kind: &str, word: &[u8]) -> Result<DecodedValue, DecodeError> {
    if kind == "address" {
        return Ok(DecodedValue::Address(format!(
            "0x{}",
            hex::encode(&word[12..32])
        )));
    }
    if kind == "bool" {
        return Ok(DecodedValue::Bool(word[31] != 0));
    }
    if kind.starts_with("uint") {
        return Ok(DecodedValue::Uint(BigUint::from_bytes_be(word).to_string()));
    }
    if kind.starts_with("int") {
        return Ok(DecodedValue::Int(
            BigInt::from_signed_bytes_be(word).to_string(),
        ));
    }
    if kind == "bytes32" {
        return Ok(DecodedValue::Bytes(format!("0x{}", hex::encode(word))));
    }
    if let Some(size) = fixed_bytes_size(kind) {
        return Ok(DecodedValue::Bytes(format!(
            "0x{}",
            hex::encode(&word[..size])
        )));
    }
    Err(DecodeError::UnsupportedType(kind.to_string()))
}

fn decode_dynamic_value(
    kind: &str,
    data: &[u8],
    offset_word: &[u8],
) -> Result<DecodedValue, DecodeError> {
    let offset = usize_from_word(offset_word)?;
    let length_word = data
        .get(offset..offset + 32)
        .ok_or(DecodeError::InvalidDynamicOffset(offset))?;
    let length = usize_from_word(length_word)?;
    let start = offset + 32;
    let end = start + length;
    let bytes = data
        .get(start..end)
        .ok_or(DecodeError::InvalidDynamicLength(length))?;
    match kind {
        "string" => Ok(DecodedValue::String(
            String::from_utf8_lossy(bytes).to_string(),
        )),
        "bytes" => Ok(DecodedValue::Bytes(format!("0x{}", hex::encode(bytes)))),
        _ => Err(DecodeError::UnsupportedType(kind.to_string())),
    }
}

fn data_word(data: &[u8], index: usize) -> Result<&[u8], DecodeError> {
    let start = index * 32;
    data.get(start..start + 32)
        .ok_or(DecodeError::MissingDataWord(index))
}

fn decode_topic(topic: &str) -> Result<Vec<u8>, DecodeError> {
    let bytes = decode_hex(topic)?;
    if bytes.len() == 32 {
        Ok(bytes)
    } else {
        Err(DecodeError::InvalidHex(topic.to_string()))
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>, DecodeError> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    hex::decode(trimmed).map_err(|_| DecodeError::InvalidHex(value.to_string()))
}

fn usize_from_word(word: &[u8]) -> Result<usize, DecodeError> {
    let value = BigUint::from_bytes_be(word);
    value
        .try_into()
        .map_err(|_| DecodeError::InvalidDynamicOffset(usize::MAX))
}

fn is_dynamic_type(kind: &str) -> bool {
    kind == "string" || kind == "bytes"
}

fn fixed_bytes_size(kind: &str) -> Option<usize> {
    let suffix = kind.strip_prefix("bytes")?;
    if suffix.is_empty() {
        return None;
    }
    let size = suffix.parse::<usize>().ok()?;
    (1..=32).contains(&size).then_some(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_transfer_event_params() -> Result<(), DecodeError> {
        let inputs = vec![
            AbiEventInput {
                name: Some("from".to_string()),
                kind: "address".to_string(),
                indexed: true,
            },
            AbiEventInput {
                name: Some("to".to_string()),
                kind: "address".to_string(),
                indexed: true,
            },
            AbiEventInput {
                name: Some("value".to_string()),
                kind: "uint256".to_string(),
                indexed: false,
            },
        ];
        let params = decode_event_params(
            &inputs,
            &[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef".to_string(),
                "0x0000000000000000000000001111111111111111111111111111111111111111".to_string(),
                "0x0000000000000000000000002222222222222222222222222222222222222222".to_string(),
            ],
            "0x000000000000000000000000000000000000000000000000000000000000002a",
        )?;

        assert_eq!(params.len(), 3);
        assert!(matches!(
            &params[0].value,
            DecodedValue::Address(value) if value == "0x1111111111111111111111111111111111111111"
        ));
        assert!(matches!(&params[2].value, DecodedValue::Uint(value) if value == "42"));
        Ok(())
    }

    #[test]
    fn decodes_dynamic_string_param() -> Result<(), DecodeError> {
        let inputs = vec![AbiEventInput {
            name: Some("uri".to_string()),
            kind: "string".to_string(),
            indexed: false,
        }];
        let params = decode_event_params(
            &inputs,
            &["0x0".to_string()],
            concat!(
                "0x",
                "0000000000000000000000000000000000000000000000000000000000000020",
                "0000000000000000000000000000000000000000000000000000000000000005",
                "68656c6c6f000000000000000000000000000000000000000000000000000000"
            ),
        )?;

        assert!(matches!(&params[0].value, DecodedValue::String(value) if value == "hello"));
        Ok(())
    }
}
