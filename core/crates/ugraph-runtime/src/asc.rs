use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::Duration,
};

use num_bigint::{BigInt, BigUint, Sign};
use serde::Deserialize;
use serde_json::json;
use tiny_keccak::{Hasher, Keccak};
use ugraph_core::{DecodedEventParam, DecodedValue, MatchedLog};
use wasmtime::{Caller, Extern, Instance, Memory, Store, TypedFunc, Val, ValType};

use crate::RuntimeHostState;

const TYPE_STRING: i32 = 0;
const TYPE_ARRAY_BUFFER: i32 = 1;
const TYPE_UINT8_ARRAY: i32 = 6;
const TYPE_BIG_DECIMAL: i32 = 12;
const TYPE_ARRAY_ETHEREUM_VALUE: i32 = 15;
const TYPE_ARRAY_STORE_VALUE: i32 = 16;
const TYPE_ARRAY_JSON_VALUE: i32 = 17;
const TYPE_ARRAY_EVENT_PARAM: i32 = 19;
const TYPE_ARRAY_TYPED_MAP_ENTRY_STRING_JSON_VALUE: i32 = 20;
const TYPE_ARRAY_TYPED_MAP_ENTRY_STRING_STORE_VALUE: i32 = 21;
const TYPE_EVENT_PARAM: i32 = 23;
const TYPE_WRAPPED_BOOL: i32 = 28;
const TYPE_WRAPPED_JSON_VALUE: i32 = 29;
const TYPE_ETHEREUM_TRANSACTION: i32 = 24;
const TYPE_ETHEREUM_BLOCK: i32 = 25;
const TYPE_ETHEREUM_VALUE: i32 = 30;
const TYPE_STORE_VALUE: i32 = 31;
const TYPE_JSON_VALUE: i32 = 32;
const TYPE_ETHEREUM_EVENT: i32 = 33;
const TYPE_TYPED_MAP_ENTRY_STRING_JSON_VALUE: i32 = 35;
const TYPE_TYPED_MAP_ENTRY_STRING_STORE_VALUE: i32 = 34;
const TYPE_TYPED_MAP_STRING_JSON_VALUE: i32 = 37;
const TYPE_TYPED_MAP_STRING_STORE_VALUE: i32 = 36;
const TYPE_RESULT_JSON_VALUE_BOOL: i32 = 40;

const ETH_VALUE_ADDRESS: u32 = 0;
const ETH_VALUE_FIXED_BYTES: u32 = 1;
const ETH_VALUE_BYTES: u32 = 2;
const ETH_VALUE_INT: u32 = 3;
const ETH_VALUE_UINT: u32 = 4;
const ETH_VALUE_BOOL: u32 = 5;
const ETH_VALUE_STRING: u32 = 6;

const STORE_VALUE_STRING: u32 = 0;
const STORE_VALUE_INT: u32 = 1;
const STORE_VALUE_BIG_DECIMAL: u32 = 2;
const STORE_VALUE_BOOL: u32 = 3;
const STORE_VALUE_ARRAY: u32 = 4;
const STORE_VALUE_NULL: u32 = 5;
const STORE_VALUE_BYTES: u32 = 6;
const STORE_VALUE_BIG_INT: u32 = 7;
const STORE_VALUE_INT8: u32 = 8;
const STORE_VALUE_TIMESTAMP: u32 = 9;
const JSON_VALUE_NULL: u32 = 0;
const JSON_VALUE_BOOL: u32 = 1;
const JSON_VALUE_NUMBER: u32 = 2;
const JSON_VALUE_STRING: u32 = 3;
const JSON_VALUE_ARRAY: u32 = 4;
const JSON_VALUE_OBJECT: u32 = 5;
const BIG_DECIMAL_PRECISION: usize = 34;
const BIG_DECIMAL_DIVISION_SCALE: u32 = 34;
const MAX_DECIMAL_SCALE: usize = 10_000;
const DEFAULT_IPFS_GATEWAY: &str = "https://ipfs.io/ipfs/";
const DEFAULT_IPFS_TIMEOUT_SECS: u64 = 60;
const DEFAULT_MAX_IPFS_FILE_BYTES: u64 = 25 * 1024 * 1024;

static POW10_CACHE: OnceLock<Mutex<BTreeMap<usize, BigInt>>> = OnceLock::new();
static HTTP_CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();

pub type EntityData = BTreeMap<String, StoreValue>;
pub type EntityStore = BTreeMap<(String, String), EntityData>;
pub type EthereumCallCache = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", content = "value")]
pub enum StoreValue {
    String(String),
    Int(i32),
    BigDecimal { digits: String, exp: String },
    Bool(bool),
    Array(Vec<StoreValue>),
    Null,
    Bytes(String),
    BigInt(String),
    Int8(i64),
    Timestamp(i64),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoreSetCall {
    pub entity: Option<String>,
    pub id: Option<String>,
    pub data: Option<EntityData>,
    pub validation_errors: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DataSourceCreateCall {
    pub name: Option<String>,
    pub params: Vec<String>,
    pub context: EntityData,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EthereumCallReport {
    pub contract_name: String,
    pub contract_address: String,
    pub function_name: String,
    pub function_signature: String,
    pub block_number: Option<u64>,
    pub reverted: bool,
    pub output_types: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HandlerExecutionReport {
    pub wasm_path: String,
    pub handler: String,
    pub event_ptr: i32,
    pub call_counts: BTreeMap<String, usize>,
    pub store_sets: Vec<StoreSetCall>,
    pub data_source_creates: Vec<DataSourceCreateCall>,
    pub ethereum_calls: Vec<EthereumCallReport>,
}

#[derive(Debug, Clone)]
enum EthereumValue {
    Address(String),
    Bool(bool),
    Bytes(String),
    Int(String),
    String(String),
    Uint(String),
    Array(Vec<EthereumValue>),
    Tuple(Vec<EthereumValue>),
}

#[derive(Debug, Clone)]
struct SmartContractCall {
    contract_name: String,
    contract_address: String,
    function_name: String,
    function_signature: String,
    function_params: Vec<EthereumValue>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

pub(crate) fn handle_host_call(
    mut caller: Caller<'_, RuntimeHostState>,
    import_name: &str,
    params: &[Val],
    results: &mut [Val],
    result_types: &[ValType],
) -> Result<(), wasmtime::Error> {
    *caller
        .data_mut()
        .call_counts
        .entry(import_name.to_string())
        .or_default() += 1;

    match import_name {
        "abort" => Err(wasmtime::Error::msg("mapping aborted")),
        "typeConversion.bytesToHex" => {
            let ptr = i32_param(params, 0)?;
            let bytes = read_uint8_array_from_caller(&mut caller, ptr)?;
            let output = format!("0x{}", hex::encode(bytes));
            set_i32_result(results, 0, alloc_string_from_caller(&mut caller, &output)?);
            Ok(())
        }
        "typeConversion.bytesToString" => {
            let ptr = i32_param(params, 0)?;
            let bytes = read_uint8_array_from_caller(&mut caller, ptr)?;
            let output = graph_bytes_to_string(bytes);
            set_i32_result(results, 0, alloc_string_from_caller(&mut caller, &output)?);
            Ok(())
        }
        "typeConversion.stringToH160" => {
            let ptr = i32_param(params, 0)?;
            let value = read_string_from_caller(&mut caller, ptr)?;
            set_i32_result(
                results,
                0,
                alloc_uint8_array_from_caller(&mut caller, &hex_bytes_20(&value)?)?,
            );
            Ok(())
        }
        "typeConversion.bigIntToHex" => {
            let ptr = i32_param(params, 0)?;
            let value = read_bigint_from_caller(&mut caller, ptr)?;
            let output = if value < BigInt::from(0) {
                format!("-0x{}", (-value).to_str_radix(16))
            } else {
                format!("0x{}", value.to_str_radix(16))
            };
            set_i32_result(results, 0, alloc_string_from_caller(&mut caller, &output)?);
            Ok(())
        }
        "typeConversion.bigIntToString" => {
            let ptr = i32_param(params, 0)?;
            let value = read_bigint_from_caller(&mut caller, ptr)?;
            set_i32_result(
                results,
                0,
                alloc_string_from_caller(&mut caller, &value.to_string())?,
            );
            Ok(())
        }
        "bigInt.plus" => {
            let left = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigint_from_caller(&mut caller, i32_param(params, 1)?)?;
            set_i32_result(
                results,
                0,
                alloc_bigint_from_caller(&mut caller, &(left + right))?,
            );
            Ok(())
        }
        "bigInt.minus" => {
            let left = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigint_from_caller(&mut caller, i32_param(params, 1)?)?;
            set_i32_result(
                results,
                0,
                alloc_bigint_from_caller(&mut caller, &(left - right))?,
            );
            Ok(())
        }
        "bigInt.times" => {
            let left = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigint_from_caller(&mut caller, i32_param(params, 1)?)?;
            set_i32_result(
                results,
                0,
                alloc_bigint_from_caller(&mut caller, &(left * right))?,
            );
            Ok(())
        }
        "bigInt.dividedBy" => {
            let left = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigint_from_caller(&mut caller, i32_param(params, 1)?)?;
            if right == BigInt::from(0) {
                return Err(wasmtime::Error::msg("BigInt division by zero"));
            }
            set_i32_result(
                results,
                0,
                alloc_bigint_from_caller(&mut caller, &(left / right))?,
            );
            Ok(())
        }
        "bigInt.dividedByDecimal" => {
            let left = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            let (digits, exp) = divide_big_decimals((left, BigInt::from(0)), right)?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigInt.pow" => {
            let base = read_bigint_from_caller(&mut caller, i32_param(params, 0)?)?;
            let exponent = i32_param(params, 1)?;
            let exponent = u32::try_from(exponent)
                .map_err(|_| wasmtime::Error::msg("BigInt exponent cannot be negative"))?;
            set_i32_result(
                results,
                0,
                alloc_bigint_from_caller(&mut caller, &base.pow(exponent))?,
            );
            Ok(())
        }
        "bigDecimal.fromString" => {
            let value = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let (digits, exp) = parse_big_decimal_string(&value)?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigDecimal.plus" => {
            let left = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            let (digits, exp) = add_big_decimals(left, right)?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigDecimal.minus" => {
            let left = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            let (right_digits, right_exp) =
                read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            let (digits, exp) = add_big_decimals(left, (-right_digits, right_exp))?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigDecimal.times" => {
            let left = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            let (digits, exp) = multiply_big_decimals(left, right)?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigDecimal.dividedBy" => {
            let left = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            let (digits, exp) = divide_big_decimals(left, right)?;
            set_i32_result(
                results,
                0,
                alloc_bigdecimal_from_caller(&mut caller, &digits, &exp)?,
            );
            Ok(())
        }
        "bigDecimal.equals" => {
            let left = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            let right = read_bigdecimal_from_caller(&mut caller, i32_param(params, 1)?)?;
            set_i32_result(results, 0, i32::from(big_decimals_equal(left, right)?));
            Ok(())
        }
        "crypto.keccak256" => {
            let bytes = read_uint8_array_from_caller(&mut caller, i32_param(params, 0)?)?;
            let mut hasher = Keccak::v256();
            let mut output = [0_u8; 32];
            hasher.update(&bytes);
            hasher.finalize(&mut output);
            set_i32_result(
                results,
                0,
                alloc_uint8_array_from_caller(&mut caller, &output)?,
            );
            Ok(())
        }
        "json.fromBytes" => {
            let bytes = read_uint8_array_from_caller(&mut caller, i32_param(params, 0)?)?;
            let value = parse_json_bytes(&bytes)?;
            let ptr = alloc_json_value_from_caller(&mut caller, &value)?;
            set_i32_result(results, 0, ptr);
            Ok(())
        }
        "json.try_fromBytes" => {
            let bytes = read_uint8_array_from_caller(&mut caller, i32_param(params, 0)?)?;
            let ptr = match parse_json_bytes(&bytes) {
                Ok(value) => {
                    let value_ptr = alloc_json_value_from_caller(&mut caller, &value)?;
                    alloc_json_result_from_caller(&mut caller, Ok(value_ptr))?
                }
                Err(_) => alloc_json_result_from_caller(&mut caller, Err(true))?,
            };
            set_i32_result(results, 0, ptr);
            Ok(())
        }
        "json.toI64" => {
            let value = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let parsed = parse_json_integer_string(&value)?
                .parse::<i64>()
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            set_i64_result(results, 0, parsed);
            Ok(())
        }
        "json.toU64" => {
            let value = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let parsed = parse_json_integer_string(&value)?
                .parse::<u64>()
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            set_i64_result(results, 0, parsed as i64);
            Ok(())
        }
        "json.toF64" => {
            let value = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let parsed = value
                .parse::<f64>()
                .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
            set_f64_result(results, 0, parsed);
            Ok(())
        }
        "json.toBigInt" => {
            let value = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let parsed = parse_json_integer_string(&value)?;
            set_i32_result(
                results,
                0,
                alloc_bigint_string_from_caller(&mut caller, &parsed)?,
            );
            Ok(())
        }
        "bigDecimal.toString" => {
            let (digits, exp) = read_bigdecimal_from_caller(&mut caller, i32_param(params, 0)?)?;
            set_i32_result(
                results,
                0,
                alloc_string_from_caller(&mut caller, &format_big_decimal(&digits, &exp)?)?,
            );
            Ok(())
        }
        "ethereum.call" => {
            let call = read_smart_contract_call_from_caller(&mut caller, i32_param(params, 0)?)?;
            let rpc_url = caller.data().rpc_url.clone();
            let block_number = caller.data().block_number;
            let (values, report) = execute_ethereum_call(
                rpc_url.as_deref(),
                block_number,
                &call,
                &mut caller.data_mut().ethereum_call_cache,
            )?;
            caller.data_mut().ethereum_calls.push(report);
            let ptr = match values {
                Some(values) => alloc_ethereum_value_array_from_caller(&mut caller, &values)?,
                None => 0,
            };
            set_i32_result(results, 0, ptr);
            Ok(())
        }
        "ipfs.cat" | "ipfs.getBlock" => {
            let path = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let ptr = match fetch_ipfs_bytes(&path) {
                Ok(Some(bytes)) => alloc_uint8_array_from_caller(&mut caller, &bytes)?,
                Ok(None) | Err(_) => 0,
            };
            set_i32_result(results, 0, ptr);
            Ok(())
        }
        "ipfs.map" => {
            let path = read_string_from_caller(&mut caller, i32_param(params, 0)?)?;
            let callback = read_string_from_caller(&mut caller, i32_param(params, 1)?)?;
            let user_data_ptr = i32_param(params, 2)?;
            let flags = read_string_array_from_caller(&mut caller, i32_param(params, 3)?)?;
            let Some(bytes) = fetch_ipfs_bytes(&path)? else {
                return Ok(());
            };
            let callback_func =
                caller_export_func(&mut caller, &callback)?.typed::<(i32, i32), ()>(&mut caller)?;
            if ipfs_map_uses_json(&flags) {
                for value in ipfs_json_values_from_bytes(&bytes)? {
                    let value_ptr = alloc_json_value_from_caller(&mut caller, &value)?;
                    callback_func.call(&mut caller, (value_ptr, user_data_ptr))?;
                }
            } else {
                let value_ptr = alloc_uint8_array_from_caller(&mut caller, &bytes)?;
                callback_func.call(&mut caller, (value_ptr, user_data_ptr))?;
            }
            Ok(())
        }
        "store.get" | "store.get_in_block" => {
            let entity = params
                .first()
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let id = params
                .get(1)
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let data = match (&entity, &id) {
                (Some(entity), Some(id)) => caller
                    .data()
                    .store
                    .get(&(entity.clone(), id.clone()))
                    .cloned(),
                _ => None,
            };
            let ptr = match data {
                Some(data) => alloc_entity_from_caller(&mut caller, &data)?,
                None => 0,
            };
            set_i32_result(results, 0, ptr);
            Ok(())
        }
        "store.set" => {
            let entity = params
                .first()
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let id = params
                .get(1)
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let data = params
                .get(2)
                .and_then(Val::i32)
                .map(|ptr| read_entity_from_caller(&mut caller, ptr))
                .transpose()?;
            if let (Some(entity), Some(id), Some(data)) = (&entity, &id, &data) {
                caller
                    .data_mut()
                    .store
                    .insert((entity.clone(), id.clone()), data.clone());
            }
            caller.data_mut().store_sets.push(StoreSetCall {
                entity,
                id,
                data,
                validation_errors: Vec::new(),
            });
            Ok(())
        }
        "store.remove" => {
            let entity = params
                .first()
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let id = params
                .get(1)
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            if let (Some(entity), Some(id)) = (&entity, &id) {
                caller
                    .data_mut()
                    .store
                    .remove(&(entity.clone(), id.clone()));
            }
            Ok(())
        }
        "dataSource.create" | "dataSource.createWithContext" => {
            let name = params
                .first()
                .and_then(Val::i32)
                .map(|ptr| read_string_from_caller(&mut caller, ptr))
                .transpose()?;
            let create_params = params
                .get(1)
                .and_then(Val::i32)
                .map(|ptr| read_string_array_from_caller(&mut caller, ptr))
                .transpose()?
                .unwrap_or_default();
            let context = if import_name == "dataSource.createWithContext" {
                params
                    .get(2)
                    .and_then(Val::i32)
                    .map(|ptr| read_entity_from_caller(&mut caller, ptr))
                    .transpose()?
                    .unwrap_or_default()
            } else {
                EntityData::new()
            };
            caller
                .data_mut()
                .data_source_creates
                .push(DataSourceCreateCall {
                    name,
                    params: create_params,
                    context,
                });
            Ok(())
        }
        "dataSource.address" => {
            let address = caller
                .data()
                .data_source_address
                .clone()
                .unwrap_or_default();
            set_i32_result(
                results,
                0,
                alloc_uint8_array_from_caller(&mut caller, &hex_bytes_20(&address)?)?,
            );
            Ok(())
        }
        "dataSource.network" => {
            let network = caller
                .data()
                .data_source_network
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            set_i32_result(results, 0, alloc_string_from_caller(&mut caller, &network)?);
            Ok(())
        }
        "dataSource.context" => {
            let context = caller.data().data_source_context.clone();
            set_i32_result(results, 0, alloc_entity_from_caller(&mut caller, &context)?);
            Ok(())
        }
        "ethereum.hasCode" => {
            let address = params
                .first()
                .and_then(Val::i32)
                .map(|ptr| read_uint8_array_from_caller(&mut caller, ptr))
                .transpose()?
                .map(|bytes| format!("0x{}", hex::encode(bytes)))
                .unwrap_or_default();
            let has_code = match caller.data().rpc_url.as_deref() {
                Some(rpc_url) if !address.is_empty() => {
                    eth_get_code(rpc_url, &address, caller.data().block_number)
                        .map(|code| code != "0x" && code != "0x0")
                        .unwrap_or(false)
                }
                _ => false,
            };
            set_i32_result(results, 0, i32::from(has_code));
            Ok(())
        }
        "log.log" => Ok(()),
        _ => {
            for (result, ty) in results.iter_mut().zip(result_types) {
                *result = Val::default_for_ty(ty).ok_or_else(|| {
                    wasmtime::Error::msg(format!(
                        "cannot synthesize a default result for host import result type {ty}"
                    ))
                })?;
            }
            Ok(())
        }
    }
}

pub(crate) struct AscAllocator<'a> {
    store: &'a mut Store<RuntimeHostState>,
    memory: Memory,
    new_func: TypedFunc<(i32, i32), i32>,
    id_of_type: TypedFunc<i32, i32>,
    type_ids: BTreeMap<i32, i32>,
}

impl<'a> AscAllocator<'a> {
    pub(crate) fn new(
        store: &'a mut Store<RuntimeHostState>,
        instance: &'a Instance,
    ) -> Result<Self, wasmtime::Error> {
        let memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| wasmtime::Error::msg("memory export not found"))?;
        let new_func = instance.get_typed_func::<(i32, i32), i32>(&mut *store, "__new")?;
        let id_of_type = instance.get_typed_func::<i32, i32>(&mut *store, "id_of_type")?;
        Ok(Self {
            store,
            memory,
            new_func,
            id_of_type,
            type_ids: BTreeMap::new(),
        })
    }

    pub(crate) fn alloc_event(&mut self, log: &MatchedLog) -> Result<i32, wasmtime::Error> {
        let address = self.alloc_uint8_array(&hex_bytes_20(&log.address)?)?;
        let log_index = self.alloc_bigint_u64(log.log_index.unwrap_or_default())?;
        let transaction_log_index = self.alloc_bigint_u64(log.log_index.unwrap_or_default())?;
        let block = self.alloc_block(log)?;
        let transaction = self.alloc_transaction(log)?;
        let params = self.alloc_event_params(&log.params)?;

        let mut bytes = Vec::with_capacity(32);
        push_i32(&mut bytes, address);
        push_i32(&mut bytes, log_index);
        push_i32(&mut bytes, transaction_log_index);
        push_i32(&mut bytes, 0);
        push_i32(&mut bytes, block);
        push_i32(&mut bytes, transaction);
        push_i32(&mut bytes, params);
        push_i32(&mut bytes, 0);
        self.alloc_obj(TYPE_ETHEREUM_EVENT, &bytes)
    }

    fn alloc_block(&mut self, log: &MatchedLog) -> Result<i32, wasmtime::Error> {
        let hash = self.alloc_uint8_array(&hex_bytes_32(
            log.block_hash.as_deref().unwrap_or_default(),
        )?)?;
        let zero_hash = self.alloc_uint8_array(&[0; 32])?;
        let zero_address = self.alloc_uint8_array(&[0; 20])?;
        let number = self.alloc_bigint_u64(log.block_number.unwrap_or_default())?;
        let zero = self.alloc_bigint_u64(0)?;

        let mut bytes = Vec::with_capacity(60);
        push_i32(&mut bytes, hash);
        push_i32(&mut bytes, zero_hash);
        push_i32(&mut bytes, zero_hash);
        push_i32(&mut bytes, zero_address);
        push_i32(&mut bytes, zero_hash);
        push_i32(&mut bytes, zero_hash);
        push_i32(&mut bytes, zero_hash);
        push_i32(&mut bytes, number);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, 0);
        push_i32(&mut bytes, 0);
        self.alloc_obj(TYPE_ETHEREUM_BLOCK, &bytes)
    }

    fn alloc_transaction(&mut self, log: &MatchedLog) -> Result<i32, wasmtime::Error> {
        let hash = self.alloc_uint8_array(&hex_bytes_32(
            log.transaction_hash.as_deref().unwrap_or_default(),
        )?)?;
        let index = self.alloc_bigint_u64(log.transaction_index.unwrap_or_default())?;
        let from = self.alloc_uint8_array(&[0; 20])?;
        let to = self.alloc_uint8_array(&hex_bytes_20(&log.address)?)?;
        let zero = self.alloc_bigint_u64(0)?;
        let input = self.alloc_uint8_array(&[])?;

        let mut bytes = Vec::with_capacity(36);
        push_i32(&mut bytes, hash);
        push_i32(&mut bytes, index);
        push_i32(&mut bytes, from);
        push_i32(&mut bytes, to);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, zero);
        push_i32(&mut bytes, input);
        push_i32(&mut bytes, zero);
        self.alloc_obj(TYPE_ETHEREUM_TRANSACTION, &bytes)
    }

    fn alloc_event_params(&mut self, params: &[DecodedEventParam]) -> Result<i32, wasmtime::Error> {
        let mut pointers = Vec::with_capacity(params.len());
        for param in params {
            let name = self.alloc_string(param.name.as_deref().unwrap_or_default())?;
            let value = self.alloc_ethereum_value(&param.kind, &param.value)?;
            let mut bytes = Vec::with_capacity(8);
            push_i32(&mut bytes, name);
            push_i32(&mut bytes, value);
            pointers.push(self.alloc_obj(TYPE_EVENT_PARAM, &bytes)?);
        }
        self.alloc_array_of_ptrs(TYPE_ARRAY_EVENT_PARAM, &pointers)
    }

    fn alloc_ethereum_value(
        &mut self,
        kind: &str,
        value: &DecodedValue,
    ) -> Result<i32, wasmtime::Error> {
        let (tag, payload) = match value {
            DecodedValue::Address(value) => (
                ETH_VALUE_ADDRESS,
                self.alloc_uint8_array(&hex_bytes_20(value)?)?,
            ),
            DecodedValue::Bool(value) => (ETH_VALUE_BOOL, i32::from(*value)),
            DecodedValue::Bytes(value) => {
                let bytes = hex_bytes(value)?;
                let tag = if fixed_bytes_size(kind).is_some() {
                    ETH_VALUE_FIXED_BYTES
                } else {
                    ETH_VALUE_BYTES
                };
                (tag, self.alloc_uint8_array(&bytes)?)
            }
            DecodedValue::Int(value) => (ETH_VALUE_INT, self.alloc_bigint_decimal(value, true)?),
            DecodedValue::String(value) => (ETH_VALUE_STRING, self.alloc_string(value)?),
            DecodedValue::TopicHash(value) => (
                ETH_VALUE_FIXED_BYTES,
                self.alloc_uint8_array(&hex_bytes_32(value)?)?,
            ),
            DecodedValue::Uint(value) => (ETH_VALUE_UINT, self.alloc_bigint_decimal(value, false)?),
        };
        let mut bytes = Vec::with_capacity(16);
        push_u32(&mut bytes, tag);
        push_u32(&mut bytes, 0);
        push_u64(&mut bytes, payload as u64);
        self.alloc_obj(TYPE_ETHEREUM_VALUE, &bytes)
    }

    fn alloc_string(&mut self, value: &str) -> Result<i32, wasmtime::Error> {
        let mut bytes = Vec::new();
        for unit in value.encode_utf16() {
            bytes.extend(unit.to_le_bytes());
        }
        self.alloc_obj(TYPE_STRING, &bytes)
    }

    fn alloc_bigint_u64(&mut self, value: u64) -> Result<i32, wasmtime::Error> {
        let bigint = BigInt::from(value);
        self.alloc_uint8_array(&bigint.to_signed_bytes_le())
    }

    fn alloc_bigint_decimal(&mut self, value: &str, signed: bool) -> Result<i32, wasmtime::Error> {
        let bytes = if signed {
            let bigint = BigInt::parse_bytes(value.as_bytes(), 10)
                .ok_or_else(|| wasmtime::Error::msg(format!("invalid signed integer {value}")))?;
            bigint.to_signed_bytes_le()
        } else {
            let bigint = BigUint::parse_bytes(value.as_bytes(), 10)
                .ok_or_else(|| wasmtime::Error::msg(format!("invalid unsigned integer {value}")))?;
            BigInt::from(bigint).to_signed_bytes_le()
        };
        self.alloc_uint8_array(&bytes)
    }

    fn alloc_uint8_array(&mut self, value: &[u8]) -> Result<i32, wasmtime::Error> {
        let buffer = self.alloc_obj(TYPE_ARRAY_BUFFER, value)?;
        let mut bytes = Vec::with_capacity(12);
        push_i32(&mut bytes, buffer);
        push_i32(&mut bytes, buffer);
        push_i32(&mut bytes, value.len() as i32);
        self.alloc_obj(TYPE_UINT8_ARRAY, &bytes)
    }

    fn alloc_array_of_ptrs(
        &mut self,
        type_index: i32,
        pointers: &[i32],
    ) -> Result<i32, wasmtime::Error> {
        let mut content = Vec::with_capacity(pointers.len() * 4);
        for ptr in pointers {
            push_i32(&mut content, *ptr);
        }
        let buffer = self.alloc_obj(TYPE_ARRAY_BUFFER, &content)?;
        let mut bytes = Vec::with_capacity(16);
        push_i32(&mut bytes, buffer);
        push_i32(&mut bytes, buffer);
        push_i32(&mut bytes, content.len() as i32);
        push_i32(&mut bytes, pointers.len() as i32);
        self.alloc_obj(type_index, &bytes)
    }

    fn alloc_obj(&mut self, type_index: i32, bytes: &[u8]) -> Result<i32, wasmtime::Error> {
        let type_id = self.type_id(type_index)?;
        let ptr = self
            .new_func
            .call(&mut *self.store, (bytes.len() as i32, type_id))?;
        self.memory
            .write(&mut *self.store, ptr as usize, bytes)
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        Ok(ptr)
    }

    fn type_id(&mut self, type_index: i32) -> Result<i32, wasmtime::Error> {
        if let Some(value) = self.type_ids.get(&type_index) {
            return Ok(*value);
        }
        let value = self.id_of_type.call(&mut *self.store, type_index)?;
        self.type_ids.insert(type_index, value);
        Ok(value)
    }
}

pub(crate) fn alloc_string_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &str,
) -> Result<i32, wasmtime::Error> {
    let mut bytes = Vec::new();
    for unit in value.encode_utf16() {
        bytes.extend(unit.to_le_bytes());
    }
    alloc_obj_from_caller(caller, TYPE_STRING, &bytes)
}

fn alloc_uint8_array_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &[u8],
) -> Result<i32, wasmtime::Error> {
    let buffer = alloc_obj_from_caller(caller, TYPE_ARRAY_BUFFER, value)?;
    let mut bytes = Vec::with_capacity(12);
    push_i32(&mut bytes, buffer);
    push_i32(&mut bytes, buffer);
    push_i32(&mut bytes, value.len() as i32);
    alloc_obj_from_caller(caller, TYPE_UINT8_ARRAY, &bytes)
}

fn alloc_bigint_string_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &str,
) -> Result<i32, wasmtime::Error> {
    let bigint = BigInt::parse_bytes(value.as_bytes(), 10)
        .ok_or_else(|| wasmtime::Error::msg(format!("invalid BigInt value {value}")))?;
    alloc_bigint_from_caller(caller, &bigint)
}

fn alloc_bigint_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &BigInt,
) -> Result<i32, wasmtime::Error> {
    alloc_uint8_array_from_caller(caller, &value.to_signed_bytes_le())
}

fn alloc_bigdecimal_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    digits: &BigInt,
    exp: &BigInt,
) -> Result<i32, wasmtime::Error> {
    let digits = alloc_bigint_from_caller(caller, digits)?;
    let exp = alloc_bigint_from_caller(caller, exp)?;
    let mut bytes = Vec::with_capacity(8);
    push_i32(&mut bytes, digits);
    push_i32(&mut bytes, exp);
    alloc_obj_from_caller(caller, TYPE_BIG_DECIMAL, &bytes)
}

fn alloc_array_of_ptrs_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    type_index: i32,
    pointers: &[i32],
) -> Result<i32, wasmtime::Error> {
    let mut content = Vec::with_capacity(pointers.len() * 4);
    for ptr in pointers {
        push_i32(&mut content, *ptr);
    }
    let buffer = alloc_obj_from_caller(caller, TYPE_ARRAY_BUFFER, &content)?;
    let mut bytes = Vec::with_capacity(16);
    push_i32(&mut bytes, buffer);
    push_i32(&mut bytes, buffer);
    push_i32(&mut bytes, content.len() as i32);
    push_i32(&mut bytes, pointers.len() as i32);
    alloc_obj_from_caller(caller, type_index, &bytes)
}

fn alloc_entity_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    data: &EntityData,
) -> Result<i32, wasmtime::Error> {
    let mut entries = Vec::with_capacity(data.len());
    for (key, value) in data {
        let key_ptr = alloc_string_from_caller(caller, key)?;
        let value_ptr = alloc_store_value_from_caller(caller, value)?;
        let mut bytes = Vec::with_capacity(8);
        push_i32(&mut bytes, key_ptr);
        push_i32(&mut bytes, value_ptr);
        entries.push(alloc_obj_from_caller(
            caller,
            TYPE_TYPED_MAP_ENTRY_STRING_STORE_VALUE,
            &bytes,
        )?);
    }
    let entries_ptr = alloc_array_of_ptrs_from_caller(
        caller,
        TYPE_ARRAY_TYPED_MAP_ENTRY_STRING_STORE_VALUE,
        &entries,
    )?;
    let mut bytes = Vec::with_capacity(4);
    push_i32(&mut bytes, entries_ptr);
    alloc_obj_from_caller(caller, TYPE_TYPED_MAP_STRING_STORE_VALUE, &bytes)
}

fn alloc_store_value_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &StoreValue,
) -> Result<i32, wasmtime::Error> {
    let (kind, payload) = match value {
        StoreValue::String(value) => (
            STORE_VALUE_STRING,
            alloc_string_from_caller(caller, value)? as u64,
        ),
        StoreValue::Int(value) => (STORE_VALUE_INT, *value as u32 as u64),
        StoreValue::BigDecimal { digits, exp } => {
            let digits = alloc_bigint_string_from_caller(caller, digits)?;
            let exp = alloc_bigint_string_from_caller(caller, exp)?;
            let mut bytes = Vec::with_capacity(8);
            push_i32(&mut bytes, digits);
            push_i32(&mut bytes, exp);
            (
                STORE_VALUE_BIG_DECIMAL,
                alloc_obj_from_caller(caller, TYPE_BIG_DECIMAL, &bytes)? as u64,
            )
        }
        StoreValue::Bool(value) => (STORE_VALUE_BOOL, u64::from(*value)),
        StoreValue::Array(values) => {
            let mut pointers = Vec::with_capacity(values.len());
            for value in values {
                pointers.push(alloc_store_value_from_caller(caller, value)?);
            }
            (
                STORE_VALUE_ARRAY,
                alloc_array_of_ptrs_from_caller(caller, TYPE_ARRAY_STORE_VALUE, &pointers)? as u64,
            )
        }
        StoreValue::Null => (STORE_VALUE_NULL, 0),
        StoreValue::Bytes(value) => (
            STORE_VALUE_BYTES,
            alloc_uint8_array_from_caller(caller, &hex_bytes(value)?)? as u64,
        ),
        StoreValue::BigInt(value) => (
            STORE_VALUE_BIG_INT,
            alloc_bigint_string_from_caller(caller, value)? as u64,
        ),
        StoreValue::Int8(value) => (STORE_VALUE_INT8, *value as u64),
        StoreValue::Timestamp(value) => (STORE_VALUE_TIMESTAMP, *value as u64),
    };

    let mut bytes = Vec::with_capacity(16);
    push_u32(&mut bytes, kind);
    push_u32(&mut bytes, 0);
    push_u64(&mut bytes, payload);
    alloc_obj_from_caller(caller, TYPE_STORE_VALUE, &bytes)
}

fn alloc_json_value_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &serde_json::Value,
) -> Result<i32, wasmtime::Error> {
    let (kind, payload) = match value {
        serde_json::Value::Null => (JSON_VALUE_NULL, 0),
        serde_json::Value::Bool(value) => (JSON_VALUE_BOOL, u64::from(*value)),
        serde_json::Value::Number(value) => (
            JSON_VALUE_NUMBER,
            alloc_string_from_caller(caller, &value.to_string())? as u64,
        ),
        serde_json::Value::String(value) => (
            JSON_VALUE_STRING,
            alloc_string_from_caller(caller, value)? as u64,
        ),
        serde_json::Value::Array(values) => {
            let mut pointers = Vec::with_capacity(values.len());
            for value in values {
                pointers.push(alloc_json_value_from_caller(caller, value)?);
            }
            (
                JSON_VALUE_ARRAY,
                alloc_array_of_ptrs_from_caller(caller, TYPE_ARRAY_JSON_VALUE, &pointers)? as u64,
            )
        }
        serde_json::Value::Object(values) => {
            let mut entries = Vec::with_capacity(values.len());
            for (key, value) in values {
                let key_ptr = alloc_string_from_caller(caller, key)?;
                let value_ptr = alloc_json_value_from_caller(caller, value)?;
                let mut bytes = Vec::with_capacity(8);
                push_i32(&mut bytes, key_ptr);
                push_i32(&mut bytes, value_ptr);
                entries.push(alloc_obj_from_caller(
                    caller,
                    TYPE_TYPED_MAP_ENTRY_STRING_JSON_VALUE,
                    &bytes,
                )?);
            }
            let entries_ptr = alloc_array_of_ptrs_from_caller(
                caller,
                TYPE_ARRAY_TYPED_MAP_ENTRY_STRING_JSON_VALUE,
                &entries,
            )?;
            let mut bytes = Vec::with_capacity(4);
            push_i32(&mut bytes, entries_ptr);
            (
                JSON_VALUE_OBJECT,
                alloc_obj_from_caller(caller, TYPE_TYPED_MAP_STRING_JSON_VALUE, &bytes)? as u64,
            )
        }
    };

    let mut bytes = Vec::with_capacity(16);
    push_u32(&mut bytes, kind);
    push_u32(&mut bytes, 0);
    push_u64(&mut bytes, payload);
    alloc_obj_from_caller(caller, TYPE_JSON_VALUE, &bytes)
}

fn alloc_json_result_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: Result<i32, bool>,
) -> Result<i32, wasmtime::Error> {
    let (value_ptr, error_ptr) = match value {
        Ok(value_ptr) => {
            let mut bytes = Vec::with_capacity(4);
            push_i32(&mut bytes, value_ptr);
            (
                alloc_obj_from_caller(caller, TYPE_WRAPPED_JSON_VALUE, &bytes)?,
                0,
            )
        }
        Err(error) => {
            let bytes = [u8::from(error)];
            (0, alloc_obj_from_caller(caller, TYPE_WRAPPED_BOOL, &bytes)?)
        }
    };
    let mut bytes = Vec::with_capacity(8);
    push_i32(&mut bytes, value_ptr);
    push_i32(&mut bytes, error_ptr);
    alloc_obj_from_caller(caller, TYPE_RESULT_JSON_VALUE_BOOL, &bytes)
}

fn alloc_ethereum_value_array_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    values: &[EthereumValue],
) -> Result<i32, wasmtime::Error> {
    let mut pointers = Vec::with_capacity(values.len());
    for value in values {
        pointers.push(alloc_ethereum_value_from_caller(caller, value)?);
    }
    alloc_array_of_ptrs_from_caller(caller, TYPE_ARRAY_ETHEREUM_VALUE, &pointers)
}

fn alloc_ethereum_value_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    value: &EthereumValue,
) -> Result<i32, wasmtime::Error> {
    let (kind, payload) = match value {
        EthereumValue::Address(value) => (
            ETH_VALUE_ADDRESS,
            alloc_uint8_array_from_caller(caller, &hex_bytes_20(value)?)? as u64,
        ),
        EthereumValue::Bool(value) => (ETH_VALUE_BOOL, u64::from(*value)),
        EthereumValue::Bytes(value) => (
            ETH_VALUE_BYTES,
            alloc_uint8_array_from_caller(caller, &hex_bytes(value)?)? as u64,
        ),
        EthereumValue::Int(value) => (
            ETH_VALUE_INT,
            alloc_bigint_string_from_caller(caller, value)? as u64,
        ),
        EthereumValue::String(value) => (
            ETH_VALUE_STRING,
            alloc_string_from_caller(caller, value)? as u64,
        ),
        EthereumValue::Uint(value) => (
            ETH_VALUE_UINT,
            alloc_bigint_string_from_caller(caller, value)? as u64,
        ),
        EthereumValue::Array(values) | EthereumValue::Tuple(values) => {
            return Err(wasmtime::Error::msg(format!(
                "allocating array/tuple Ethereum values with {} entries is not implemented yet",
                values.len()
            )));
        }
    };

    let mut bytes = Vec::with_capacity(16);
    push_u32(&mut bytes, kind);
    push_u32(&mut bytes, 0);
    push_u64(&mut bytes, payload);
    alloc_obj_from_caller(caller, TYPE_ETHEREUM_VALUE, &bytes)
}

fn alloc_obj_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    type_index: i32,
    bytes: &[u8],
) -> Result<i32, wasmtime::Error> {
    let type_id = type_id_from_caller(caller, type_index)?;
    let new_func = caller_export_func(caller, "__new")?.typed::<(i32, i32), i32>(&mut *caller)?;
    let ptr = new_func.call(&mut *caller, (bytes.len() as i32, type_id))?;
    let memory = caller_memory(caller)?;
    memory
        .write(&mut *caller, ptr as usize, bytes)
        .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
    Ok(ptr)
}

fn type_id_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    type_index: i32,
) -> Result<i32, wasmtime::Error> {
    if let Some(value) = caller.data().type_ids.get(&type_index) {
        return Ok(*value);
    }
    let func = caller_export_func(caller, "id_of_type")?.typed::<i32, i32>(&mut *caller)?;
    let value = func.call(&mut *caller, type_index)?;
    caller.data_mut().type_ids.insert(type_index, value);
    Ok(value)
}

fn read_uint8_array_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<Vec<u8>, wasmtime::Error> {
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    read_uint8_array_from_data(data, ptr)
}

fn read_uint8_array_from_data(data: &[u8], ptr: i32) -> Result<Vec<u8>, wasmtime::Error> {
    if ptr == 0 {
        return Ok(Vec::new());
    }
    let ptr = ptr_to_usize(ptr)?;
    let data_start = read_u32(data, ptr + 4)?;
    let byte_length = read_u32(data, ptr + 8)?;
    let data_start_usize = data_start as usize;
    let byte_length_usize = byte_length as usize;
    data.get(data_start_usize..data_start_usize + byte_length_usize)
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| wasmtime::Error::msg("Uint8Array access out of bounds"))
}

fn read_string_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<String, wasmtime::Error> {
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    read_string_from_data(data, ptr)
}

fn read_string_from_data(data: &[u8], ptr: i32) -> Result<String, wasmtime::Error> {
    if ptr == 0 {
        return Ok(String::new());
    }
    let ptr = ptr_to_usize(ptr)?;
    let byte_length = read_u32(
        data,
        ptr.checked_sub(4)
            .ok_or_else(|| wasmtime::Error::msg("invalid string pointer"))?,
    )? as usize;
    let bytes = data
        .get(ptr..ptr + byte_length)
        .ok_or_else(|| wasmtime::Error::msg("string access out of bounds"))?;
    let mut units = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        units.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    String::from_utf16(&units).map_err(|err| wasmtime::Error::msg(err.to_string()))
}

fn read_entity_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<EntityData, wasmtime::Error> {
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    read_entity_from_data(data, ptr)
}

fn read_string_array_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<Vec<String>, wasmtime::Error> {
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    let pointers = read_array_ptrs_from_data(data, ptr)?;
    pointers
        .into_iter()
        .map(|ptr| read_string_from_data(data, ptr))
        .collect()
}

fn read_smart_contract_call_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<SmartContractCall, wasmtime::Error> {
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    read_smart_contract_call_from_data(data, ptr)
}

fn read_smart_contract_call_from_data(
    data: &[u8],
    ptr: i32,
) -> Result<SmartContractCall, wasmtime::Error> {
    let ptr = ptr_to_usize(ptr)?;
    let contract_name = read_string_from_data(data, read_i32(data, ptr)?)?;
    let contract_address = format!(
        "0x{}",
        hex::encode(read_uint8_array_from_data(data, read_i32(data, ptr + 4)?)?)
    );
    let function_name = read_string_from_data(data, read_i32(data, ptr + 8)?)?;
    let function_signature = read_string_from_data(data, read_i32(data, ptr + 12)?)?;
    let param_ptrs = read_array_ptrs_from_data(data, read_i32(data, ptr + 16)?)?;
    let mut function_params = Vec::with_capacity(param_ptrs.len());
    for param_ptr in param_ptrs {
        function_params.push(read_ethereum_value_from_data(data, param_ptr)?);
    }
    Ok(SmartContractCall {
        contract_name,
        contract_address,
        function_name,
        function_signature,
        function_params,
    })
}

fn read_ethereum_value_from_data(data: &[u8], ptr: i32) -> Result<EthereumValue, wasmtime::Error> {
    if ptr == 0 {
        return Err(wasmtime::Error::msg("Ethereum value pointer is null"));
    }
    let ptr = ptr_to_usize(ptr)?;
    let kind = read_u32(data, ptr)?;
    let payload = read_u64(data, ptr + 8)?;
    match kind {
        ETH_VALUE_ADDRESS => Ok(EthereumValue::Address(format!(
            "0x{}",
            hex::encode(read_uint8_array_from_data(data, payload_to_ptr(payload)?)?)
        ))),
        ETH_VALUE_FIXED_BYTES | ETH_VALUE_BYTES => Ok(EthereumValue::Bytes(format!(
            "0x{}",
            hex::encode(read_uint8_array_from_data(data, payload_to_ptr(payload)?)?)
        ))),
        ETH_VALUE_INT => Ok(EthereumValue::Int(read_bigint_string_from_data(
            data,
            payload_to_ptr(payload)?,
        )?)),
        ETH_VALUE_UINT => Ok(EthereumValue::Uint(read_bigint_string_from_data(
            data,
            payload_to_ptr(payload)?,
        )?)),
        ETH_VALUE_BOOL => Ok(EthereumValue::Bool(payload != 0)),
        ETH_VALUE_STRING => Ok(EthereumValue::String(read_string_from_data(
            data,
            payload_to_ptr(payload)?,
        )?)),
        7..=9 => {
            let value_ptrs = read_array_ptrs_from_data(data, payload_to_ptr(payload)?)?;
            let mut values = Vec::with_capacity(value_ptrs.len());
            for value_ptr in value_ptrs {
                values.push(read_ethereum_value_from_data(data, value_ptr)?);
            }
            if kind == 9 {
                Ok(EthereumValue::Tuple(values))
            } else {
                Ok(EthereumValue::Array(values))
            }
        }
        _ => Err(wasmtime::Error::msg(format!(
            "unsupported Ethereum value kind {kind}"
        ))),
    }
}

fn read_entity_from_data(data: &[u8], ptr: i32) -> Result<EntityData, wasmtime::Error> {
    if ptr == 0 {
        return Ok(EntityData::new());
    }
    let ptr = ptr_to_usize(ptr)?;
    let entries_ptr = read_i32(data, ptr)?;
    let entries = read_array_ptrs_from_data(data, entries_ptr)?;
    let mut entity = EntityData::new();
    for entry_ptr in entries {
        if entry_ptr == 0 {
            continue;
        }
        let entry_offset = ptr_to_usize(entry_ptr)?;
        let key_ptr = read_i32(data, entry_offset)?;
        let value_ptr = read_i32(data, entry_offset + 4)?;
        let key = read_string_from_data(data, key_ptr)?;
        let value = read_store_value_from_data(data, value_ptr)?;
        entity.insert(key, value);
    }
    Ok(entity)
}

fn read_store_value_from_data(data: &[u8], ptr: i32) -> Result<StoreValue, wasmtime::Error> {
    if ptr == 0 {
        return Ok(StoreValue::Null);
    }
    let ptr = ptr_to_usize(ptr)?;
    let kind = read_u32(data, ptr)?;
    let payload = read_u64(data, ptr + 8)?;
    match kind {
        STORE_VALUE_STRING => Ok(StoreValue::String(read_string_from_data(
            data,
            payload_to_ptr(payload)?,
        )?)),
        STORE_VALUE_INT => Ok(StoreValue::Int(payload as u32 as i32)),
        STORE_VALUE_BIG_DECIMAL => {
            let decimal_ptr = ptr_to_usize(payload_to_ptr(payload)?)?;
            let digits_ptr = read_i32(data, decimal_ptr)?;
            let exp_ptr = read_i32(data, decimal_ptr + 4)?;
            Ok(StoreValue::BigDecimal {
                digits: read_bigint_string_from_data(data, digits_ptr)?,
                exp: read_bigint_string_from_data(data, exp_ptr)?,
            })
        }
        STORE_VALUE_BOOL => Ok(StoreValue::Bool(payload != 0)),
        STORE_VALUE_ARRAY => {
            let value_ptrs = read_array_ptrs_from_data(data, payload_to_ptr(payload)?)?;
            let mut values = Vec::with_capacity(value_ptrs.len());
            for value_ptr in value_ptrs {
                values.push(read_store_value_from_data(data, value_ptr)?);
            }
            Ok(StoreValue::Array(values))
        }
        STORE_VALUE_NULL => Ok(StoreValue::Null),
        STORE_VALUE_BYTES => Ok(StoreValue::Bytes(format!(
            "0x{}",
            hex::encode(read_uint8_array_from_data(data, payload_to_ptr(payload)?)?)
        ))),
        STORE_VALUE_BIG_INT => Ok(StoreValue::BigInt(read_bigint_string_from_data(
            data,
            payload_to_ptr(payload)?,
        )?)),
        STORE_VALUE_INT8 => Ok(StoreValue::Int8(payload as i64)),
        STORE_VALUE_TIMESTAMP => Ok(StoreValue::Timestamp(payload as i64)),
        _ => Err(wasmtime::Error::msg(format!(
            "unsupported StoreValue kind {kind}"
        ))),
    }
}

fn read_array_ptrs_from_data(data: &[u8], ptr: i32) -> Result<Vec<i32>, wasmtime::Error> {
    if ptr == 0 {
        return Ok(Vec::new());
    }
    let ptr = ptr_to_usize(ptr)?;
    let data_start = read_u32(data, ptr + 4)? as usize;
    let length = read_i32(data, ptr + 12)?;
    if length < 0 {
        return Err(wasmtime::Error::msg("array length is negative"));
    }
    let byte_length = read_u32(data, ptr + 8)? as usize;
    let length = length as usize;
    if length * 4 > byte_length {
        return Err(wasmtime::Error::msg(
            "array pointer length exceeds byte length",
        ));
    }

    let mut pointers = Vec::with_capacity(length);
    for index in 0..length {
        pointers.push(read_i32(data, data_start + index * 4)?);
    }
    Ok(pointers)
}

fn read_bigint_string_from_data(data: &[u8], ptr: i32) -> Result<String, wasmtime::Error> {
    let bytes = read_uint8_array_from_data(data, ptr)?;
    Ok(BigInt::from_signed_bytes_le(&bytes).to_string())
}

fn graph_bytes_to_string(mut bytes: Vec<u8>) -> String {
    while bytes.last() == Some(&0) {
        bytes.pop();
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_json_bytes(bytes: &[u8]) -> Result<serde_json::Value, wasmtime::Error> {
    serde_json::from_slice(bytes).map_err(|err| wasmtime::Error::msg(err.to_string()))
}

fn parse_json_integer_string(value: &str) -> Result<String, wasmtime::Error> {
    if value.contains(['.', 'e', 'E']) {
        return Err(wasmtime::Error::msg(format!(
            "JSON number `{value}` is not an integer"
        )));
    }
    Ok(value.to_string())
}

fn ipfs_map_uses_json(flags: &[String]) -> bool {
    flags.iter().any(|flag| flag == "json")
}

fn ipfs_json_values_from_bytes(bytes: &[u8]) -> Result<Vec<serde_json::Value>, wasmtime::Error> {
    let text = std::str::from_utf8(bytes).map_err(|err| wasmtime::Error::msg(err.to_string()))?;
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(|err| wasmtime::Error::msg(err.to_string())))
        .collect()
}

fn read_bigint_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<BigInt, wasmtime::Error> {
    Ok(BigInt::from_signed_bytes_le(&read_uint8_array_from_caller(
        caller, ptr,
    )?))
}

fn read_bigdecimal_from_caller(
    caller: &mut Caller<'_, RuntimeHostState>,
    ptr: i32,
) -> Result<(BigInt, BigInt), wasmtime::Error> {
    if ptr == 0 {
        return Ok((BigInt::from(0), BigInt::from(0)));
    }
    let memory = caller_memory(caller)?;
    let data = memory.data(&mut *caller);
    let ptr = ptr_to_usize(ptr)?;
    let digits_ptr = read_i32(data, ptr)?;
    let exp_ptr = read_i32(data, ptr + 4)?;
    Ok((
        BigInt::from_signed_bytes_le(&read_uint8_array_from_data(data, digits_ptr)?),
        BigInt::from_signed_bytes_le(&read_uint8_array_from_data(data, exp_ptr)?),
    ))
}

fn format_big_decimal(digits: &BigInt, exp: &BigInt) -> Result<String, wasmtime::Error> {
    let exp = exp
        .to_string()
        .parse::<i64>()
        .map_err(|_| wasmtime::Error::msg("BigDecimal exponent is out of range"))?;
    if exp == 0 {
        return Ok(digits.to_string());
    }

    let negative = digits.sign() == Sign::Minus;
    let magnitude = if negative {
        -digits.clone()
    } else {
        digits.clone()
    };
    let mut text = magnitude.to_string();
    if exp > 0 {
        text.push_str(&"0".repeat(exp as usize));
    } else {
        let places = (-exp) as usize;
        if places >= text.len() {
            let zeros = "0".repeat(places - text.len());
            text = format!("0.{zeros}{text}");
        } else {
            let split = text.len() - places;
            text.insert(split, '.');
        }
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if negative {
        text.insert(0, '-');
    }
    Ok(text)
}

fn parse_big_decimal_string(value: &str) -> Result<(BigInt, BigInt), wasmtime::Error> {
    let value = value.trim();
    if value.is_empty() {
        return Err(wasmtime::Error::msg("BigDecimal string is empty"));
    }

    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent = value[index + 1..]
                .parse::<i64>()
                .map_err(|_| wasmtime::Error::msg(format!("invalid BigDecimal `{value}`")))?;
            (&value[..index], exponent)
        }
        None => (value, 0),
    };

    let negative = mantissa.starts_with('-');
    let mantissa = mantissa
        .strip_prefix('-')
        .or_else(|| mantissa.strip_prefix('+'))
        .unwrap_or(mantissa);
    let (whole, fractional) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fractional.is_empty() {
        return Err(wasmtime::Error::msg(format!(
            "invalid BigDecimal `{value}`"
        )));
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fractional.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(wasmtime::Error::msg(format!(
            "invalid BigDecimal `{value}`"
        )));
    }

    let raw_digits = format!("{whole}{fractional}");
    let digits = raw_digits.trim_start_matches('0');
    if digits.is_empty() {
        return Ok((BigInt::from(0), BigInt::from(0)));
    }

    let mut digits = BigInt::parse_bytes(digits.as_bytes(), 10)
        .ok_or_else(|| wasmtime::Error::msg(format!("invalid BigDecimal `{value}`")))?;
    if negative {
        digits = -digits;
    }
    let exp = exponent
        .checked_sub(i64::try_from(fractional.len()).map_err(|_| {
            wasmtime::Error::msg(format!(
                "BigDecimal `{value}` has too many fractional digits"
            ))
        })?)
        .ok_or_else(|| {
            wasmtime::Error::msg(format!("BigDecimal exponent overflow for `{value}`"))
        })?;
    normalize_big_decimal(digits, BigInt::from(exp))
}

fn add_big_decimals(
    left: (BigInt, BigInt),
    right: (BigInt, BigInt),
) -> Result<(BigInt, BigInt), wasmtime::Error> {
    if left.0 == BigInt::from(0) {
        return normalize_big_decimal(right.0, right.1);
    }
    if right.0 == BigInt::from(0) {
        return normalize_big_decimal(left.0, left.1);
    }
    let (left_digits, left_exp) = normalize_big_decimal(left.0, left.1)?;
    let (right_digits, right_exp) = normalize_big_decimal(right.0, right.1)?;
    if let Some(value) =
        significant_operand_for_large_gap((&left_digits, &left_exp), (&right_digits, &right_exp))?
    {
        return normalize_big_decimal(value.0, value.1);
    }
    let (left_digits, left_exp) = decimal_parts_to_i64((left_digits, left_exp))?;
    let (right_digits, right_exp) = decimal_parts_to_i64((right_digits, right_exp))?;
    let exp = left_exp.min(right_exp);
    let left_digits = decimal_shift(left_digits, left_exp - exp)?;
    let right_digits = decimal_shift(right_digits, right_exp - exp)?;
    normalize_big_decimal(left_digits + right_digits, BigInt::from(exp))
}

fn multiply_big_decimals(
    left: (BigInt, BigInt),
    right: (BigInt, BigInt),
) -> Result<(BigInt, BigInt), wasmtime::Error> {
    let (left_digits, left_exp) = decimal_parts_to_i64(left)?;
    let (right_digits, right_exp) = decimal_parts_to_i64(right)?;
    let exp = left_exp
        .checked_add(right_exp)
        .ok_or_else(|| wasmtime::Error::msg("BigDecimal exponent overflow"))?;
    normalize_big_decimal(left_digits * right_digits, BigInt::from(exp))
}

fn divide_big_decimals(
    left: (BigInt, BigInt),
    right: (BigInt, BigInt),
) -> Result<(BigInt, BigInt), wasmtime::Error> {
    let (left_digits, left_exp) = decimal_parts_to_i64(left)?;
    let (right_digits, right_exp) = decimal_parts_to_i64(right)?;
    if right_digits == BigInt::from(0) {
        return Err(wasmtime::Error::msg("BigDecimal division by zero"));
    }
    let scaled_left = left_digits * pow10(BIG_DECIMAL_DIVISION_SCALE as usize)?;
    let exp = left_exp
        .checked_sub(right_exp)
        .and_then(|exp| exp.checked_sub(i64::from(BIG_DECIMAL_DIVISION_SCALE)))
        .ok_or_else(|| wasmtime::Error::msg("BigDecimal exponent overflow"))?;
    normalize_big_decimal(scaled_left / right_digits, BigInt::from(exp))
}

fn big_decimals_equal(
    left: (BigInt, BigInt),
    right: (BigInt, BigInt),
) -> Result<bool, wasmtime::Error> {
    let left = normalize_big_decimal(left.0, left.1)?;
    let right = normalize_big_decimal(right.0, right.1)?;
    Ok(left == right)
}

fn decimal_parts_to_i64(value: (BigInt, BigInt)) -> Result<(BigInt, i64), wasmtime::Error> {
    let exp = value
        .1
        .to_string()
        .parse::<i64>()
        .map_err(|_| wasmtime::Error::msg("BigDecimal exponent is out of range"))?;
    Ok((value.0, exp))
}

fn decimal_shift(digits: BigInt, shift: i64) -> Result<BigInt, wasmtime::Error> {
    if shift < 0 {
        return Err(wasmtime::Error::msg("BigDecimal negative scale shift"));
    }
    Ok(digits
        * pow10(
            usize::try_from(shift)
                .map_err(|_| wasmtime::Error::msg("BigDecimal scale is out of range"))?,
        )?)
}

fn normalize_big_decimal(
    mut digits: BigInt,
    mut exp: BigInt,
) -> Result<(BigInt, BigInt), wasmtime::Error> {
    if digits == BigInt::from(0) {
        return Ok((BigInt::from(0), BigInt::from(0)));
    }
    trim_trailing_decimal_zeros(&mut digits, &mut exp);
    round_big_decimal_precision(&mut digits, &mut exp)?;
    trim_trailing_decimal_zeros(&mut digits, &mut exp);
    Ok((digits, exp))
}

fn trim_trailing_decimal_zeros(digits: &mut BigInt, exp: &mut BigInt) {
    let ten = BigInt::from(10);
    while (&*digits % &ten) == BigInt::from(0) {
        *digits /= &ten;
        *exp += 1;
    }
}

fn round_big_decimal_precision(
    digits: &mut BigInt,
    exp: &mut BigInt,
) -> Result<(), wasmtime::Error> {
    let negative = digits.sign() == Sign::Minus;
    let magnitude = if negative {
        -digits.clone()
    } else {
        digits.clone()
    };
    let text = magnitude.to_string();
    if text.len() <= BIG_DECIMAL_PRECISION {
        return Ok(());
    }

    let extra_digits = text.len() - BIG_DECIMAL_PRECISION;
    let mut rounded = BigInt::parse_bytes(&text.as_bytes()[..BIG_DECIMAL_PRECISION], 10)
        .ok_or_else(|| wasmtime::Error::msg("invalid BigDecimal digits"))?;
    if text.as_bytes()[BIG_DECIMAL_PRECISION] >= b'5' {
        rounded += 1;
    }
    if rounded.to_string().len() > BIG_DECIMAL_PRECISION {
        rounded /= 10;
        *exp += 1;
    }
    if negative {
        rounded = -rounded;
    }
    *digits = rounded;
    *exp += BigInt::from(extra_digits);
    Ok(())
}

fn significant_operand_for_large_gap(
    left: (&BigInt, &BigInt),
    right: (&BigInt, &BigInt),
) -> Result<Option<(BigInt, BigInt)>, wasmtime::Error> {
    let left_order = decimal_order(left.0, left.1)?;
    let right_order = decimal_order(right.0, right.1)?;
    let gap = (left_order - right_order).abs();
    if gap <= BIG_DECIMAL_PRECISION as i64 {
        return Ok(None);
    }
    if left_order > right_order {
        Ok(Some((left.0.clone(), left.1.clone())))
    } else {
        Ok(Some((right.0.clone(), right.1.clone())))
    }
}

fn decimal_order(digits: &BigInt, exp: &BigInt) -> Result<i64, wasmtime::Error> {
    let exp = exp
        .to_string()
        .parse::<i64>()
        .map_err(|_| wasmtime::Error::msg("BigDecimal exponent is out of range"))?;
    let digits = if digits < &BigInt::from(0) {
        -digits.clone()
    } else {
        digits.clone()
    };
    let digits = digits.to_string().len();
    i64::try_from(digits)
        .ok()
        .and_then(|digits| digits.checked_add(exp))
        .ok_or_else(|| wasmtime::Error::msg("BigDecimal order overflow"))
}

fn pow10(scale: usize) -> Result<BigInt, wasmtime::Error> {
    if scale > MAX_DECIMAL_SCALE {
        return Err(wasmtime::Error::msg(format!(
            "BigDecimal scale {scale} exceeds limit {MAX_DECIMAL_SCALE}"
        )));
    }
    let cache = POW10_CACHE.get_or_init(|| {
        let mut cache = BTreeMap::new();
        cache.insert(0, BigInt::from(1));
        Mutex::new(cache)
    });
    if let Some(value) = cache
        .lock()
        .map_err(|_| wasmtime::Error::msg("BigDecimal pow10 cache is poisoned"))?
        .get(&scale)
        .cloned()
    {
        return Ok(value);
    }
    let value = BigInt::from(10).pow(scale as u32);
    cache
        .lock()
        .map_err(|_| wasmtime::Error::msg("BigDecimal pow10 cache is poisoned"))?
        .insert(scale, value.clone());
    Ok(value)
}

fn execute_ethereum_call(
    rpc_url: Option<&str>,
    block_number: Option<u64>,
    call: &SmartContractCall,
    cache: &mut EthereumCallCache,
) -> Result<(Option<Vec<EthereumValue>>, EthereumCallReport), wasmtime::Error> {
    let output_types = parse_call_output_types(&call.function_signature)?;
    let mut report = EthereumCallReport {
        contract_name: call.contract_name.clone(),
        contract_address: call.contract_address.clone(),
        function_name: call.function_name.clone(),
        function_signature: call.function_signature.clone(),
        block_number,
        reverted: false,
        output_types: output_types.clone(),
        error: None,
    };

    let Some(rpc_url) = rpc_url else {
        report.reverted = true;
        report.error = Some("RPC URL not configured".to_string());
        return Ok((None, report));
    };
    let (selector_signature, input_types) = parse_call_input_signature(&call.function_signature)?;
    if input_types.len() != call.function_params.len() {
        report.reverted = true;
        report.error = Some(format!(
            "call param count mismatch: signature has {}, call has {}",
            input_types.len(),
            call.function_params.len()
        ));
        return Ok((None, report));
    }
    let calldata = encode_call_data(&selector_signature, &input_types, &call.function_params)?;
    let block = block_number
        .map(|block| format!("0x{block:x}"))
        .unwrap_or_else(|| "latest".to_string());
    let cache_key = format!("{rpc_url}|{}|{calldata}|{block}", call.contract_address);
    let response = match cache.get(&cache_key) {
        Some(result) => Ok(result.clone()),
        None => eth_call(rpc_url, &call.contract_address, &calldata, &block).inspect(|result| {
            cache.insert(cache_key, result.clone());
        }),
    };
    let result = match response {
        Ok(result) => result,
        Err(err) => {
            report.reverted = true;
            report.error = Some(err);
            return Ok((None, report));
        }
    };
    match decode_call_outputs(&output_types, &result) {
        Ok(values) => Ok((Some(values), report)),
        Err(err) => {
            report.reverted = true;
            report.error = Some(err.to_string());
            Ok((None, report))
        }
    }
}

fn eth_call(rpc_url: &str, to: &str, data: &str, block: &str) -> Result<String, String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": to,
                "data": data,
            },
            block,
        ],
    });
    let response = HTTP_CLIENT
        .get_or_init(reqwest::blocking::Client::new)
        .post(rpc_url)
        .json(&body)
        .send()
        .map_err(|err| err.to_string())?
        .error_for_status()
        .map_err(|err| err.to_string())?
        .json::<JsonRpcResponse<String>>()
        .map_err(|err| err.to_string())?;
    if let Some(error) = response.error {
        return Err(format!("rpc error {}: {}", error.code, error.message));
    }
    response
        .result
        .ok_or_else(|| "rpc response did not include a result".to_string())
}

fn eth_get_code(rpc_url: &str, address: &str, block_number: Option<u64>) -> Result<String, String> {
    let block = block_number
        .map(|block| format!("0x{block:x}"))
        .unwrap_or_else(|| "latest".to_string());
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getCode",
        "params": [address, block],
    });
    let response = reqwest::blocking::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .map_err(|err| err.to_string())?
        .error_for_status()
        .map_err(|err| err.to_string())?
        .json::<JsonRpcResponse<String>>()
        .map_err(|err| err.to_string())?;
    if let Some(error) = response.error {
        return Err(format!("rpc error {}: {}", error.code, error.message));
    }
    response
        .result
        .ok_or_else(|| "rpc response did not include a result".to_string())
}

fn fetch_ipfs_bytes(path: &str) -> Result<Option<Vec<u8>>, wasmtime::Error> {
    let url = ipfs_gateway_url(path);
    let timeout = ipfs_timeout();
    let max_bytes = max_ipfs_file_bytes();
    let response = HTTP_CLIENT
        .get_or_init(reqwest::blocking::Client::new)
        .get(&url)
        .timeout(timeout)
        .send()
        .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let response = response
        .error_for_status()
        .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes)
    {
        return Err(wasmtime::Error::msg(format!(
            "IPFS response exceeds {max_bytes} bytes"
        )));
    }
    let bytes = response
        .bytes()
        .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(wasmtime::Error::msg(format!(
            "IPFS response exceeds {max_bytes} bytes"
        )));
    }
    Ok(Some(bytes.to_vec()))
}

fn ipfs_gateway_url(path: &str) -> String {
    let path = path.trim();
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }
    let path = normalize_ipfs_path(path);
    let gateway =
        std::env::var("UGRAPH_IPFS_GATEWAY").unwrap_or_else(|_| DEFAULT_IPFS_GATEWAY.to_string());
    if gateway.contains("{path}") {
        return gateway.replace("{path}", &path);
    }
    let separator = if gateway.ends_with('/') { "" } else { "/" };
    format!("{gateway}{separator}{path}")
}

fn normalize_ipfs_path(path: &str) -> String {
    let path = path.trim();
    let path = path.strip_prefix("ipfs://").unwrap_or(path);
    let path = path.strip_prefix("/ipfs/").unwrap_or(path);
    path.trim_start_matches('/').to_string()
}

fn ipfs_timeout() -> Duration {
    let seconds = std::env::var("UGRAPH_IPFS_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IPFS_TIMEOUT_SECS);
    Duration::from_secs(seconds)
}

fn max_ipfs_file_bytes() -> u64 {
    std::env::var("UGRAPH_MAX_IPFS_FILE_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MAX_IPFS_FILE_BYTES)
}

fn encode_call_data(
    selector_signature: &str,
    input_types: &[String],
    params: &[EthereumValue],
) -> Result<String, wasmtime::Error> {
    let mut bytes = function_selector(selector_signature).to_vec();
    for (kind, value) in input_types.iter().zip(params) {
        bytes.extend(encode_static_abi_word(kind, value)?);
    }
    Ok(format!("0x{}", hex::encode(bytes)))
}

fn decode_call_outputs(
    output_types: &[String],
    data: &str,
) -> Result<Vec<EthereumValue>, wasmtime::Error> {
    let bytes = hex_bytes(data)?;
    let mut values = Vec::with_capacity(output_types.len());
    for (index, kind) in output_types.iter().enumerate() {
        let offset = index * 32;
        let word = bytes
            .get(offset..offset + 32)
            .ok_or_else(|| wasmtime::Error::msg(format!("missing call output word {index}")))?;
        if is_dynamic_abi_type(kind) {
            values.push(decode_dynamic_abi_output(kind, &bytes, word)?);
        } else {
            values.push(decode_static_abi_word(kind, word)?);
        }
    }
    Ok(values)
}

fn encode_static_abi_word(kind: &str, value: &EthereumValue) -> Result<[u8; 32], wasmtime::Error> {
    let mut out = [0_u8; 32];
    match (kind, value) {
        ("address", EthereumValue::Address(value)) => {
            let bytes = hex_bytes_20(value)?;
            out[12..32].copy_from_slice(&bytes);
            Ok(out)
        }
        ("bool", EthereumValue::Bool(value)) => {
            out[31] = u8::from(*value);
            Ok(out)
        }
        (kind, EthereumValue::Uint(value)) if kind.starts_with("uint") => {
            let value = BigUint::parse_bytes(value.as_bytes(), 10)
                .ok_or_else(|| wasmtime::Error::msg(format!("invalid uint value {value}")))?;
            let bytes = value.to_bytes_be();
            if bytes.len() > 32 {
                return Err(wasmtime::Error::msg("uint value exceeds 32 bytes"));
            }
            out[32 - bytes.len()..32].copy_from_slice(&bytes);
            Ok(out)
        }
        (kind, EthereumValue::Int(value)) if kind.starts_with("int") => {
            let value = BigInt::parse_bytes(value.as_bytes(), 10)
                .ok_or_else(|| wasmtime::Error::msg(format!("invalid int value {value}")))?;
            let fill = if value < BigInt::from(0) { 0xff } else { 0x00 };
            out.fill(fill);
            let bytes = value.to_signed_bytes_be();
            if bytes.len() > 32 {
                return Err(wasmtime::Error::msg("int value exceeds 32 bytes"));
            }
            out[32 - bytes.len()..32].copy_from_slice(&bytes);
            Ok(out)
        }
        (kind, EthereumValue::Bytes(value)) if fixed_bytes_size(kind).is_some() => {
            let size = fixed_bytes_size(kind).expect("checked");
            let bytes = hex_bytes(value)?;
            if bytes.len() != size {
                return Err(wasmtime::Error::msg(format!(
                    "expected {size} bytes for {kind}, got {}",
                    bytes.len()
                )));
            }
            out[..size].copy_from_slice(&bytes);
            Ok(out)
        }
        _ => Err(wasmtime::Error::msg(format!(
            "unsupported static ABI input {kind}"
        ))),
    }
}

fn decode_static_abi_word(kind: &str, word: &[u8]) -> Result<EthereumValue, wasmtime::Error> {
    if word.len() != 32 {
        return Err(wasmtime::Error::msg("ABI word must be 32 bytes"));
    }
    if kind == "address" {
        return Ok(EthereumValue::Address(format!(
            "0x{}",
            hex::encode(&word[12..32])
        )));
    }
    if kind == "bool" {
        return Ok(EthereumValue::Bool(word[31] != 0));
    }
    if kind.starts_with("uint") {
        return Ok(EthereumValue::Uint(
            BigUint::from_bytes_be(word).to_string(),
        ));
    }
    if kind.starts_with("int") {
        return Ok(EthereumValue::Int(
            BigInt::from_signed_bytes_be(word).to_string(),
        ));
    }
    if fixed_bytes_size(kind).is_some() {
        let size = fixed_bytes_size(kind).expect("checked");
        return Ok(EthereumValue::Bytes(format!(
            "0x{}",
            hex::encode(&word[..size])
        )));
    }
    Err(wasmtime::Error::msg(format!(
        "unsupported static ABI output {kind}"
    )))
}

fn decode_dynamic_abi_output(
    kind: &str,
    data: &[u8],
    offset_word: &[u8],
) -> Result<EthereumValue, wasmtime::Error> {
    let offset = abi_word_as_usize(offset_word)?;
    let bytes = decode_dynamic_abi_bytes(data, offset)?;
    match kind {
        "string" => Ok(EthereumValue::String(
            String::from_utf8(bytes).map_err(|err| wasmtime::Error::msg(err.to_string()))?,
        )),
        "bytes" => Ok(EthereumValue::Bytes(format!("0x{}", hex::encode(bytes)))),
        _ => Err(wasmtime::Error::msg(format!(
            "unsupported dynamic ABI output {kind}"
        ))),
    }
}

fn decode_dynamic_abi_bytes(data: &[u8], offset: usize) -> Result<Vec<u8>, wasmtime::Error> {
    let length_word = data
        .get(offset..offset + 32)
        .ok_or_else(|| wasmtime::Error::msg("dynamic ABI output length is out of bounds"))?;
    let length = abi_word_as_usize(length_word)?;
    let start = offset + 32;
    data.get(start..start + length)
        .map(|bytes| bytes.to_vec())
        .ok_or_else(|| wasmtime::Error::msg("dynamic ABI output data is out of bounds"))
}

fn abi_word_as_usize(word: &[u8]) -> Result<usize, wasmtime::Error> {
    if word.len() != 32 {
        return Err(wasmtime::Error::msg("ABI word must be 32 bytes"));
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        return Err(wasmtime::Error::msg("ABI word does not fit in usize"));
    }
    let mut value = [0_u8; 8];
    value.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(value))
        .map_err(|_| wasmtime::Error::msg("ABI word does not fit in usize"))
}

fn is_dynamic_abi_type(kind: &str) -> bool {
    matches!(kind, "string" | "bytes")
}

fn parse_call_input_signature(signature: &str) -> Result<(String, Vec<String>), wasmtime::Error> {
    let selector_signature = signature
        .split_once(':')
        .map(|(input, _)| input)
        .unwrap_or(signature)
        .to_string();
    let inputs = parse_signature_types(&selector_signature)?;
    Ok((selector_signature, inputs))
}

fn parse_call_output_types(signature: &str) -> Result<Vec<String>, wasmtime::Error> {
    let Some((_, output)) = signature.split_once(':') else {
        return Ok(Vec::new());
    };
    parse_signature_types(output)
}

fn parse_signature_types(signature: &str) -> Result<Vec<String>, wasmtime::Error> {
    let Some(open) = signature.find('(') else {
        return Ok(Vec::new());
    };
    let Some(close) = signature.rfind(')') else {
        return Err(wasmtime::Error::msg(format!(
            "invalid call signature {signature}"
        )));
    };
    let inner = signature[open + 1..close].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    Ok(split_top_level(inner)
        .into_iter()
        .map(str::to_string)
        .collect())
}

fn split_top_level(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0_i32;
    let mut start = 0;
    for (index, ch) in value.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(value[start..].trim());
    parts
}

fn function_selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak::v256();
    let mut out = [0_u8; 32];
    hasher.update(signature.as_bytes());
    hasher.finalize(&mut out);
    [out[0], out[1], out[2], out[3]]
}

fn caller_memory(caller: &mut Caller<'_, RuntimeHostState>) -> Result<Memory, wasmtime::Error> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| wasmtime::Error::msg("memory export not found"))
}

fn caller_export_func(
    caller: &mut Caller<'_, RuntimeHostState>,
    name: &str,
) -> Result<wasmtime::Func, wasmtime::Error> {
    caller
        .get_export(name)
        .and_then(Extern::into_func)
        .ok_or_else(|| wasmtime::Error::msg(format!("{name} export not found")))
}

fn i32_param(params: &[Val], index: usize) -> Result<i32, wasmtime::Error> {
    params
        .get(index)
        .and_then(Val::i32)
        .ok_or_else(|| wasmtime::Error::msg(format!("missing i32 param {index}")))
}

fn set_i32_result(results: &mut [Val], index: usize, value: i32) {
    if let Some(result) = results.get_mut(index) {
        *result = Val::I32(value);
    }
}

fn set_i64_result(results: &mut [Val], index: usize, value: i64) {
    if let Some(result) = results.get_mut(index) {
        *result = Val::I64(value);
    }
}

fn set_f64_result(results: &mut [Val], index: usize, value: f64) {
    if let Some(result) = results.get_mut(index) {
        *result = Val::F64(value.to_bits());
    }
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32, wasmtime::Error> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| wasmtime::Error::msg("u32 read out of bounds"))?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_i32(data: &[u8], offset: usize) -> Result<i32, wasmtime::Error> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| wasmtime::Error::msg("i32 read out of bounds"))?;
    Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64, wasmtime::Error> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| wasmtime::Error::msg("u64 read out of bounds"))?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn ptr_to_usize(ptr: i32) -> Result<usize, wasmtime::Error> {
    usize::try_from(ptr).map_err(|_| wasmtime::Error::msg(format!("invalid pointer {ptr}")))
}

fn payload_to_ptr(payload: u64) -> Result<i32, wasmtime::Error> {
    i32::try_from(payload)
        .map_err(|_| wasmtime::Error::msg(format!("invalid pointer payload {payload}")))
}

fn push_i32(out: &mut Vec<u8>, value: i32) {
    out.extend(value.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend(value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend(value.to_le_bytes());
}

fn hex_bytes(value: &str) -> Result<Vec<u8>, wasmtime::Error> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    hex::decode(trimmed).map_err(|err| wasmtime::Error::msg(err.to_string()))
}

fn hex_bytes_20(value: &str) -> Result<[u8; 20], wasmtime::Error> {
    let bytes = hex_bytes(value)?;
    bytes
        .try_into()
        .map_err(|_| wasmtime::Error::msg(format!("expected 20-byte hex value: {value}")))
}

fn hex_bytes_32(value: &str) -> Result<[u8; 32], wasmtime::Error> {
    let bytes = hex_bytes(value)?;
    if bytes.is_empty() {
        return Ok([0; 32]);
    }
    bytes
        .try_into()
        .map_err(|_| wasmtime::Error::msg(format!("expected 32-byte hex value: {value}")))
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
    use std::{
        io::{ErrorKind, Read, Write},
        net::TcpListener,
        thread,
    };

    #[test]
    fn encodes_generic_static_eth_call_data() -> Result<(), wasmtime::Error> {
        let (selector_signature, inputs) =
            parse_call_input_signature("balanceOf(address):(uint256)")?;
        let calldata = encode_call_data(
            &selector_signature,
            &inputs,
            &[EthereumValue::Address(
                "0x9b7898cd64741d7a5503e8092f04fef2106c2291".to_string(),
            )],
        )?;

        assert_eq!(
            calldata,
            "0x70a082310000000000000000000000009b7898cd64741d7a5503e8092f04fef2106c2291"
        );
        assert_eq!(
            parse_call_output_types("treasury():(address)")?,
            ["address"]
        );
        Ok(())
    }

    #[test]
    fn decodes_generic_static_eth_call_output() -> Result<(), wasmtime::Error> {
        let values = decode_call_outputs(
            &["address".to_string()],
            "0x0000000000000000000000009b7898cd64741d7a5503e8092f04fef2106c2291",
        )?;

        assert!(matches!(
            values.first(),
            Some(EthereumValue::Address(value))
                if value == "0x9b7898cd64741d7a5503e8092f04fef2106c2291"
        ));
        Ok(())
    }

    #[test]
    fn decodes_dynamic_string_eth_call_output() -> Result<(), wasmtime::Error> {
        let values = decode_call_outputs(
            &["string".to_string()],
            "0x0000000000000000000000000000000000000000000000000000000000000020\
               0000000000000000000000000000000000000000000000000000000000000003\
               554e490000000000000000000000000000000000000000000000000000000000",
        )?;

        assert!(matches!(
            values.first(),
            Some(EthereumValue::String(value)) if value == "UNI"
        ));
        Ok(())
    }

    #[test]
    fn parses_and_formats_big_decimal_values() -> Result<(), wasmtime::Error> {
        let (digits, exp) = parse_big_decimal_string("-0012.3400")?;
        assert_eq!(digits.to_string(), "-1234");
        assert_eq!(exp.to_string(), "-2");
        assert_eq!(format_big_decimal(&digits, &exp)?, "-12.34");

        let (digits, exp) = parse_big_decimal_string("1.23e4")?;
        assert_eq!(digits.to_string(), "123");
        assert_eq!(exp.to_string(), "2");
        assert_eq!(format_big_decimal(&digits, &exp)?, "12300");
        Ok(())
    }

    #[test]
    fn computes_big_decimal_arithmetic() -> Result<(), wasmtime::Error> {
        let left = parse_big_decimal_string("1.25")?;
        let right = parse_big_decimal_string("2")?;

        let (digits, exp) = add_big_decimals(left.clone(), right.clone())?;
        assert_eq!(format_big_decimal(&digits, &exp)?, "3.25");

        let (digits, exp) = multiply_big_decimals(left.clone(), right.clone())?;
        assert_eq!(format_big_decimal(&digits, &exp)?, "2.5");

        let quotient = divide_big_decimals(right.clone(), left)?;
        assert_eq!(format_big_decimal(&quotient.0, &quotient.1)?, "1.6");

        let (digits, exp) = divide_big_decimals((BigInt::from(10), BigInt::from(0)), right)?;
        assert_eq!(format_big_decimal(&digits, &exp)?, "5");

        assert!(big_decimals_equal(
            parse_big_decimal_string("1.600")?,
            quotient
        )?);
        Ok(())
    }

    #[test]
    fn clamps_big_decimal_to_decimal128_precision() -> Result<(), wasmtime::Error> {
        let (digits, exp) = parse_big_decimal_string("123456789012345678901234567890123456789")?;

        assert_eq!(digits.to_string().len(), BIG_DECIMAL_PRECISION);
        assert_eq!(digits.to_string(), "1234567890123456789012345678901235");
        assert_eq!(exp.to_string(), "5");
        Ok(())
    }

    #[test]
    fn adds_large_exponent_gap_without_rescaling() -> Result<(), wasmtime::Error> {
        let (digits, exp) = add_big_decimals(
            parse_big_decimal_string("1e1000")?,
            parse_big_decimal_string("1")?,
        )?;

        assert_eq!(digits.to_string(), "1");
        assert_eq!(exp.to_string(), "1000");
        Ok(())
    }

    #[test]
    fn subtracts_zero_without_rescaling_huge_decimals() -> Result<(), wasmtime::Error> {
        let value = (BigInt::from(10001), BigInt::from(-202320));
        let zero = (BigInt::from(0), BigInt::from(0));
        let (digits, exp) = add_big_decimals(value, zero)?;

        assert_eq!(digits.to_string(), "10001");
        assert_eq!(exp.to_string(), "-202320");
        Ok(())
    }

    #[test]
    fn converts_string_address_to_h160_bytes() -> Result<(), wasmtime::Error> {
        assert_eq!(
            hex_bytes_20("0x0000000000000000000000000000000000000001")?,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        Ok(())
    }

    #[test]
    fn trims_trailing_null_bytes_in_graph_string_conversion() {
        assert_eq!(graph_bytes_to_string(b"POOL\0\0\0".to_vec()), "POOL");
        assert_eq!(graph_bytes_to_string(b"PO\0OL\0".to_vec()), "PO\0OL");
    }

    #[test]
    fn normalizes_ipfs_paths_for_gateway_fetches() {
        assert_eq!(
            normalize_ipfs_path("ipfs://bafybeigdyrzt/path/file.json"),
            "bafybeigdyrzt/path/file.json"
        );
        assert_eq!(
            normalize_ipfs_path("/ipfs/QmHash/metadata.json"),
            "QmHash/metadata.json"
        );
        assert_eq!(normalize_ipfs_path("//QmHash"), "QmHash");
    }

    #[test]
    fn extracts_ipfs_json_map_values_line_by_line() -> Result<(), wasmtime::Error> {
        let array_values = ipfs_json_values_from_bytes(br#"[{"id":1},{"id":2}]"#)?;
        assert_eq!(array_values.len(), 1);
        assert!(array_values[0].is_array());

        let line_values = ipfs_json_values_from_bytes(b"{\"id\":1}\n{\"id\":2}\n")?;
        assert_eq!(line_values.len(), 2);
        Ok(())
    }

    #[test]
    fn fetches_ipfs_bytes_from_direct_url() -> Result<(), wasmtime::Error> {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == ErrorKind::PermissionDenied => return Ok(()),
            Err(err) => return Err(wasmtime::Error::msg(err.to_string())),
        };
        let address = listener
            .local_addr()
            .map_err(|err| wasmtime::Error::msg(err.to_string()))?;
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test server accepts connection");
            let mut request = [0; 1024];
            let bytes_read = stream
                .read(&mut request)
                .expect("test server reads request");
            assert!(bytes_read > 0);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                )
                .expect("test server writes response");
        });

        let bytes = fetch_ipfs_bytes(&format!("http://{address}/ipfs/test"))?
            .expect("IPFS test response exists");
        handle
            .join()
            .map_err(|_| wasmtime::Error::msg("test server thread panicked"))?;
        assert_eq!(bytes, br#"{"ok":true}"#);
        Ok(())
    }
}
