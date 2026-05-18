use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

use serde::Deserialize;
use serde_json::{json, Map, Number, Value};
use ugraph_core::EntityField;
use ugraph_runtime::StoreValue;

use crate::state::{materialize_historical_snapshot, EntitySnapshot, StoreSnapshot};

#[derive(Debug, Deserialize)]
pub struct GraphqlHttpRequest {
    pub query: String,
    #[serde(default)]
    pub variables: Value,
    #[serde(rename = "operationName")]
    pub _operation_name: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedField {
    response_name: String,
    name: String,
    args: BTreeMap<String, Value>,
    selection: Vec<ParsedField>,
}

type FragmentMap = BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FilterOp {
    Eq,
    Not,
    In,
    NotIn,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    NotContains,
    ContainsNoCase,
    NotContainsNoCase,
    StartsWith,
    StartsWithNoCase,
    EndsWith,
    EndsWithNoCase,
}

#[cfg(test)]
pub fn execute_graphql(snapshot: &StoreSnapshot, query: &str) -> Value {
    execute_graphql_with_variables(snapshot, query, &Value::Null)
}

#[cfg(test)]
pub fn execute_graphql_with_variables(
    snapshot: &StoreSnapshot,
    query: &str,
    variables: &Value,
) -> Value {
    execute_graphql_with_operation(snapshot, query, variables, None)
}

pub fn execute_graphql_with_operation(
    snapshot: &StoreSnapshot,
    query: &str,
    variables: &Value,
    operation_name: Option<&str>,
) -> Value {
    match execute_query(snapshot, query, variables, operation_name) {
        Ok(data) => json!({ "data": data }),
        Err(message) => json!({ "errors": [{ "message": message }] }),
    }
}

pub fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

pub fn query_needs_history(query: &str, variables: &Value, operation_name: Option<&str>) -> bool {
    let query = strip_comments(query);
    let Ok(fragments) = extract_fragments(&query) else {
        return false;
    };
    let Ok(body) = operation_body(&query, operation_name) else {
        return false;
    };
    let Ok(fields) = parse_fields(&body, variables, &fragments) else {
        return false;
    };
    fields_have_block_arg(&fields)
}

fn fields_have_block_arg(fields: &[ParsedField]) -> bool {
    fields
        .iter()
        .any(|field| field.args.contains_key("block") || fields_have_block_arg(&field.selection))
}

fn execute_query(
    snapshot: &StoreSnapshot,
    query: &str,
    variables: &Value,
    operation_name: Option<&str>,
) -> Result<Value, String> {
    let query = strip_comments(query);
    let fragments = extract_fragments(&query)?;
    let body = operation_body(&query, operation_name)?;
    let fields = parse_fields(&body, variables, &fragments)?;
    validate_query_fields(snapshot, &fields)?;
    let mut data = Map::new();

    for field in fields {
        let value = execute_root_field(snapshot, &field)?;
        data.insert(field.response_name, value);
    }

    Ok(Value::Object(data))
}

fn validate_query_fields(snapshot: &StoreSnapshot, fields: &[ParsedField]) -> Result<(), String> {
    for field in fields {
        match field.name.as_str() {
            "__typename" | "__schema" | "__type" => {}
            "_meta" => validate_meta_fields("_Meta_", &field.selection)?,
            _ => {
                let Some(entity_name) = entity_name_for_query(snapshot, &field.name) else {
                    return Err(format!("unknown query field `{}`", field.name));
                };
                validate_entity_fields(snapshot, &entity_name, &field.selection)?;
            }
        }
    }
    Ok(())
}

fn validate_meta_fields(type_name: &str, fields: &[ParsedField]) -> Result<(), String> {
    for field in fields {
        match (type_name, field.name.as_str()) {
            (_, "__typename") => {}
            ("_Meta_", "block") => validate_meta_fields("_Block_", &field.selection)?,
            ("_Meta_", "hasIndexingErrors") | ("_Block_", "hash" | "number") => {
                if !field.selection.is_empty() {
                    return Err(format!(
                        "Field `{type_name}.{}` must not have a selection",
                        field.name
                    ));
                }
            }
            _ => return Err(format!("Type `{type_name}` has no field `{}`", field.name)),
        }
    }
    Ok(())
}

fn validate_entity_fields(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    fields: &[ParsedField],
) -> Result<(), String> {
    let Some(entity_type) = snapshot.schema.entities.get(entity_name) else {
        return Ok(());
    };
    for field in fields {
        if field.name == "__typename" {
            continue;
        }
        let Some(schema_field) = entity_type.fields.get(&field.name) else {
            return Err(format!(
                "Type `{entity_name}` has no field `{}`",
                field.name
            ));
        };
        if is_entity_type(snapshot, &schema_field.kind) {
            validate_entity_fields(snapshot, &schema_field.kind, &field.selection)?;
        } else if !field.selection.is_empty() {
            return Err(format!(
                "Field `{entity_name}.{}` must not have a selection",
                field.name
            ));
        }
    }
    Ok(())
}

fn execute_root_field(snapshot: &StoreSnapshot, field: &ParsedField) -> Result<Value, String> {
    match field.name.as_str() {
        "__typename" => Ok(Value::String("Query".to_string())),
        "__schema" => Ok(project_json(
            &introspection_schema(snapshot),
            &field.selection,
        )),
        "__type" => {
            let Some(name) = arg_string(field, "name") else {
                return Ok(Value::Null);
            };
            Ok(introspection_type(snapshot, &name)
                .map(|value| project_json(&value, &field.selection))
                .unwrap_or(Value::Null))
        }
        "_meta" => {
            let selected = select_snapshot_for_field(snapshot, field)?;
            Ok(project_json(
                &meta_value(selected.as_ref()),
                &field.selection,
            ))
        }
        _ => {
            let Some(entity_name) = entity_name_for_query(snapshot, &field.name) else {
                return Err(format!("unknown query field `{}`", field.name));
            };
            let selected = select_snapshot_for_field(snapshot, field)?;
            let selected = selected.as_ref();
            if is_plural_query(snapshot, &field.name) {
                Ok(Value::Array(query_entities(selected, &entity_name, field)))
            } else {
                Ok(query_entity(selected, &entity_name, field).unwrap_or(Value::Null))
            }
        }
    }
}

fn select_snapshot_for_field<'a>(
    snapshot: &'a StoreSnapshot,
    field: &ParsedField,
) -> Result<Cow<'a, StoreSnapshot>, String> {
    let Some(block) = block_arg(field) else {
        return Ok(Cow::Borrowed(snapshot));
    };
    match block {
        RequestedBlock::Number(number) => {
            if number > snapshot.checkpoint.to_block {
                return Err(format!(
                    "requested block {number} is after indexed block {}",
                    snapshot.checkpoint.to_block
                ));
            }
            if snapshot.checkpoint.to_block <= number {
                return Ok(Cow::Owned(snapshot_for_number_selection(snapshot.clone())));
            }
            snapshot
                .history
                .iter()
                .rev()
                .find(|entry| entry.checkpoint.to_block <= number)
                .map(|entry| {
                    Cow::Owned(snapshot_for_number_selection(
                        materialize_historical_snapshot(snapshot, entry),
                    ))
                })
                .or_else(|| Some(Cow::Owned(empty_snapshot_at(snapshot, number, None))))
                .ok_or_else(|| format!("no snapshot found for block {number}"))
        }
        RequestedBlock::Hash(hash) => {
            if snapshot
                .checkpoint
                .block_hash
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(&hash))
            {
                return Ok(Cow::Borrowed(snapshot));
            }
            snapshot
                .history
                .iter()
                .find(|entry| {
                    entry
                        .checkpoint
                        .block_hash
                        .as_deref()
                        .is_some_and(|value| value.eq_ignore_ascii_case(&hash))
                })
                .map(|entry| Cow::Owned(materialize_historical_snapshot(snapshot, entry)))
                .ok_or_else(|| format!("unknown block hash `{hash}`"))
        }
    }
}

fn snapshot_for_number_selection(mut snapshot: StoreSnapshot) -> StoreSnapshot {
    snapshot.checkpoint.block_hash = None;
    snapshot
}

enum RequestedBlock {
    Number(u64),
    Hash(String),
}

fn block_arg(field: &ParsedField) -> Option<RequestedBlock> {
    let Value::Object(block) = field.args.get("block")? else {
        return None;
    };
    if let Some(number) = block.get("number").and_then(value_to_u64) {
        return Some(RequestedBlock::Number(number));
    }
    block
        .get("hash")
        .and_then(value_to_string)
        .map(RequestedBlock::Hash)
}

fn empty_snapshot_at(snapshot: &StoreSnapshot, block: u64, hash: Option<String>) -> StoreSnapshot {
    StoreSnapshot {
        version: snapshot.version,
        manifest: snapshot.manifest.clone(),
        checkpoint: crate::state::SyncCheckpoint {
            from_block: snapshot.checkpoint.from_block,
            to_block: block,
            block_hash: hash,
            scanned_logs: 0,
            executed_logs: 0,
            validation_errors: snapshot.checkpoint.validation_errors,
            complete: true,
        },
        schema: snapshot.schema.clone(),
        entities: Vec::new(),
        dynamic_sources: Vec::new(),
        processed_logs: Vec::new(),
        history: snapshot.history.clone(),
    }
}

fn query_entity(snapshot: &StoreSnapshot, entity_name: &str, field: &ParsedField) -> Option<Value> {
    let id = arg_string(field, "id")?;
    snapshot
        .entities
        .iter()
        .find(|entity| entity.entity == entity_name && ids_match(&entity.id, &id))
        .filter(|entity| {
            entity_matches_where(snapshot, entity_name, entity, field.args.get("where"))
        })
        .map(|entity| project_entity(snapshot, entity_name, entity, &field.selection))
}

fn query_entities(snapshot: &StoreSnapshot, entity_name: &str, field: &ParsedField) -> Vec<Value> {
    let entities = snapshot
        .entities
        .iter()
        .filter(|entity| entity.entity == entity_name)
        .filter(|entity| {
            entity_matches_where(snapshot, entity_name, entity, field.args.get("where"))
        })
        .collect::<Vec<_>>();
    apply_entity_args(snapshot, entity_name, entities, field)
        .into_iter()
        .map(|entity| project_entity(snapshot, entity_name, entity, &field.selection))
        .collect()
}

fn apply_entity_args<'a>(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    mut entities: Vec<&'a EntitySnapshot>,
    field: &ParsedField,
) -> Vec<&'a EntitySnapshot> {
    let order_by = arg_string(field, "orderBy").unwrap_or_else(|| "id".to_string());
    let desc =
        arg_string(field, "orderDirection").is_some_and(|value| value.eq_ignore_ascii_case("desc"));
    entities.sort_by(|left, right| {
        let ordering = compare_entity_order(snapshot, entity_name, left, right, &order_by);
        if desc {
            ordering.reverse()
        } else {
            ordering
        }
    });

    let skip = arg_usize(field, "skip").unwrap_or(0);
    let first = arg_usize(field, "first").unwrap_or(100);
    entities.into_iter().skip(skip).take(first).collect()
}

fn project_entity(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    selection: &[ParsedField],
) -> Value {
    let fields = if selection.is_empty() {
        entity
            .data
            .keys()
            .map(|name| ParsedField {
                response_name: name.clone(),
                name: name.clone(),
                args: BTreeMap::new(),
                selection: Vec::new(),
            })
            .collect::<Vec<_>>()
    } else {
        selection.to_vec()
    };

    let mut object = Map::new();
    for field in fields {
        let value = project_entity_field(snapshot, entity_name, entity, &field);
        object.insert(field.response_name, value);
    }
    Value::Object(object)
}

fn project_entity_field(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    field: &ParsedField,
) -> Value {
    if field.name == "__typename" {
        return Value::String(entity_name.to_string());
    }

    let Some(entity_type) = snapshot.schema.entities.get(entity_name) else {
        return Value::Null;
    };
    let Some(schema_field) = entity_type.fields.get(&field.name) else {
        return entity
            .data
            .get(&field.name)
            .map(store_value_to_json)
            .unwrap_or(Value::Null);
    };

    if schema_field.derived || is_entity_type(snapshot, &schema_field.kind) {
        return project_relation_field(snapshot, entity_name, entity, schema_field, field);
    }

    entity
        .data
        .get(&field.name)
        .map(store_value_to_json)
        .unwrap_or(Value::Null)
}

fn project_relation_field(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    schema_field: &EntityField,
    field: &ParsedField,
) -> Value {
    let related = related_entities(snapshot, entity_name, entity, schema_field);
    if schema_field.list {
        return Value::Array(
            apply_entity_args(snapshot, &schema_field.kind, related, field)
                .into_iter()
                .filter(|related| {
                    entity_matches_where(
                        snapshot,
                        &schema_field.kind,
                        related,
                        field.args.get("where"),
                    )
                })
                .map(|related| {
                    project_entity(snapshot, &schema_field.kind, related, &field.selection)
                })
                .collect(),
        );
    }

    related
        .into_iter()
        .find(|related| {
            entity_matches_where(
                snapshot,
                &schema_field.kind,
                related,
                field.args.get("where"),
            )
        })
        .map(|related| project_entity(snapshot, &schema_field.kind, related, &field.selection))
        .unwrap_or(Value::Null)
}

fn related_entities<'a>(
    snapshot: &'a StoreSnapshot,
    _entity_name: &str,
    entity: &EntitySnapshot,
    schema_field: &EntityField,
) -> Vec<&'a EntitySnapshot> {
    if schema_field.derived {
        let Some(derived_from) = schema_field.derived_from.as_deref() else {
            return Vec::new();
        };
        return snapshot
            .entities
            .iter()
            .filter(|candidate| candidate.entity == schema_field.kind)
            .filter(|candidate| {
                candidate
                    .data
                    .get(derived_from)
                    .is_some_and(|value| store_value_matches_id(value, &entity.id))
            })
            .collect();
    }

    let Some(value) = entity.data.get(&schema_field.name) else {
        return Vec::new();
    };
    entity_ids_from_store_value(value)
        .into_iter()
        .filter_map(|id| find_entity(snapshot, &schema_field.kind, &id))
        .collect()
}

fn find_entity<'a>(
    snapshot: &'a StoreSnapshot,
    entity_name: &str,
    id: &str,
) -> Option<&'a EntitySnapshot> {
    snapshot
        .entities
        .iter()
        .find(|entity| entity.entity == entity_name && ids_match(&entity.id, id))
}

fn entity_matches_where(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    where_value: Option<&Value>,
) -> bool {
    let Some(Value::Object(filters)) = where_value else {
        return true;
    };

    filters.iter().all(|(key, expected)| {
        if key == "and" {
            return match expected {
                Value::Array(items) => items
                    .iter()
                    .all(|item| entity_matches_where(snapshot, entity_name, entity, Some(item))),
                _ => entity_matches_where(snapshot, entity_name, entity, Some(expected)),
            };
        }
        if key == "or" {
            return match expected {
                Value::Array(items) => items
                    .iter()
                    .any(|item| entity_matches_where(snapshot, entity_name, entity, Some(item))),
                _ => entity_matches_where(snapshot, entity_name, entity, Some(expected)),
            };
        }
        if let Some(relation_name) = key.strip_suffix('_') {
            return relation_matches_where(snapshot, entity_name, entity, relation_name, expected);
        }

        let (field_name, op) = parse_filter_key(key);
        let actual = entity_field_json(entity, &field_name);
        filter_matches(actual.as_ref(), op, expected)
    })
}

fn relation_matches_where(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    relation_name: &str,
    expected: &Value,
) -> bool {
    let Some(entity_type) = snapshot.schema.entities.get(entity_name) else {
        return false;
    };
    let Some(schema_field) = entity_type.fields.get(relation_name) else {
        return false;
    };
    related_entities(snapshot, entity_name, entity, schema_field)
        .into_iter()
        .any(|related| entity_matches_where(snapshot, &schema_field.kind, related, Some(expected)))
}

fn parse_filter_key(key: &str) -> (String, FilterOp) {
    for (suffix, op) in [
        ("_not_contains_nocase", FilterOp::NotContainsNoCase),
        ("_contains_nocase", FilterOp::ContainsNoCase),
        ("_starts_with_nocase", FilterOp::StartsWithNoCase),
        ("_ends_with_nocase", FilterOp::EndsWithNoCase),
        ("_not_contains", FilterOp::NotContains),
        ("_contains", FilterOp::Contains),
        ("_starts_with", FilterOp::StartsWith),
        ("_ends_with", FilterOp::EndsWith),
        ("_not_in", FilterOp::NotIn),
        ("_gte", FilterOp::Gte),
        ("_lte", FilterOp::Lte),
        ("_gt", FilterOp::Gt),
        ("_lt", FilterOp::Lt),
        ("_not", FilterOp::Not),
        ("_in", FilterOp::In),
    ] {
        if let Some(base) = key.strip_suffix(suffix) {
            return (base.to_string(), op);
        }
    }
    (key.to_string(), FilterOp::Eq)
}

fn filter_matches(actual: Option<&Value>, op: FilterOp, expected: &Value) -> bool {
    match op {
        FilterOp::Eq => actual.is_some_and(|actual| values_equal(actual, expected)),
        FilterOp::Not => !actual.is_some_and(|actual| values_equal(actual, expected)),
        FilterOp::In => expected.as_array().is_some_and(|items| {
            actual.is_some_and(|actual| items.iter().any(|item| values_equal(actual, item)))
        }),
        FilterOp::NotIn => expected.as_array().is_none_or(|items| {
            !actual.is_some_and(|actual| items.iter().any(|item| values_equal(actual, item)))
        }),
        FilterOp::Gt => {
            compare_optional(actual, expected).is_some_and(|ord| ord == Ordering::Greater)
        }
        FilterOp::Gte => compare_optional(actual, expected)
            .is_some_and(|ord| matches!(ord, Ordering::Greater | Ordering::Equal)),
        FilterOp::Lt => compare_optional(actual, expected).is_some_and(|ord| ord == Ordering::Less),
        FilterOp::Lte => compare_optional(actual, expected)
            .is_some_and(|ord| matches!(ord, Ordering::Less | Ordering::Equal)),
        FilterOp::Contains => contains_value(actual, expected, false),
        FilterOp::NotContains => !contains_value(actual, expected, false),
        FilterOp::ContainsNoCase => contains_value(actual, expected, true),
        FilterOp::NotContainsNoCase => !contains_value(actual, expected, true),
        FilterOp::StartsWith => string_predicate(
            actual,
            expected,
            |left, right| left.starts_with(right),
            false,
        ),
        FilterOp::StartsWithNoCase => string_predicate(
            actual,
            expected,
            |left, right| left.starts_with(right),
            true,
        ),
        FilterOp::EndsWith => {
            string_predicate(actual, expected, |left, right| left.ends_with(right), false)
        }
        FilterOp::EndsWithNoCase => {
            string_predicate(actual, expected, |left, right| left.ends_with(right), true)
        }
    }
}

fn contains_value(actual: Option<&Value>, expected: &Value, nocase: bool) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    match actual {
        Value::Array(values) => match expected {
            Value::Array(expected_values) => expected_values
                .iter()
                .all(|expected| values.iter().any(|value| values_equal(value, expected))),
            _ => values.iter().any(|value| values_equal(value, expected)),
        },
        _ => string_predicate(
            Some(actual),
            expected,
            |left, right| left.contains(right),
            nocase,
        ),
    }
}

fn string_predicate(
    actual: Option<&Value>,
    expected: &Value,
    predicate: impl Fn(&str, &str) -> bool,
    nocase: bool,
) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    let (Some(mut left), Some(mut right)) = (value_to_string(actual), value_to_string(expected))
    else {
        return false;
    };
    if nocase {
        left = left.to_ascii_lowercase();
        right = right.to_ascii_lowercase();
    }
    predicate(&left, &right)
}

fn compare_optional(actual: Option<&Value>, expected: &Value) -> Option<Ordering> {
    compare_json_values(actual?, expected)
}

fn compare_entity_order(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    left: &EntitySnapshot,
    right: &EntitySnapshot,
    order_by: &str,
) -> Ordering {
    let left_value = nested_order_value(snapshot, entity_name, left, order_by)
        .or_else(|| entity_field_json(left, order_by));
    let right_value = nested_order_value(snapshot, entity_name, right, order_by)
        .or_else(|| entity_field_json(right, order_by));
    match (left_value.as_ref(), right_value.as_ref()) {
        (Some(left), Some(right)) => compare_json_values(left, right).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn nested_order_value(
    snapshot: &StoreSnapshot,
    entity_name: &str,
    entity: &EntitySnapshot,
    order_by: &str,
) -> Option<Value> {
    let (relation_name, nested_field) = order_by.split_once("__")?;
    let entity_type = snapshot.schema.entities.get(entity_name)?;
    let schema_field = entity_type.fields.get(relation_name)?;
    let related = related_entities(snapshot, entity_name, entity, schema_field)
        .into_iter()
        .next()?;
    nested_order_value(snapshot, &schema_field.kind, related, nested_field)
        .or_else(|| entity_field_json(related, nested_field))
}

fn entity_field_json(entity: &EntitySnapshot, field_name: &str) -> Option<Value> {
    if field_name == "id" {
        return Some(Value::String(entity.id.clone()));
    }
    entity.data.get(field_name).map(store_value_to_json)
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => {
            left == right || hexish(left) && hexish(right) && left.eq_ignore_ascii_case(right)
        }
        (Value::Number(left), Value::Number(right)) => left == right,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Null, Value::Null) => true,
        _ => value_to_string(left)
            .zip(value_to_string(right))
            .is_some_and(|(left, right)| left == right),
    }
}

fn compare_json_values(left: &Value, right: &Value) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (decimal_string(left), decimal_string(right)) {
        return Some(compare_decimal_strings(&left, &right));
    }
    value_to_string(left)
        .zip(value_to_string(right))
        .map(|(left, right)| left.cmp(&right))
}

fn decimal_string(value: &Value) -> Option<String> {
    match value {
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) if is_decimal(value) => Some(value.clone()),
        _ => None,
    }
}

fn compare_decimal_strings(left: &str, right: &str) -> Ordering {
    let (left_negative, left_digits) = normalize_decimal(left);
    let (right_negative, right_digits) = normalize_decimal(right);
    match (left_negative, right_negative) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => compare_unsigned_decimal(&left_digits, &right_digits),
        (true, true) => compare_unsigned_decimal(&right_digits, &left_digits),
    }
}

fn compare_unsigned_decimal(left: &str, right: &str) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn normalize_decimal(value: &str) -> (bool, String) {
    let negative = value.starts_with('-');
    let digits = value
        .trim_start_matches('-')
        .trim_start_matches('0')
        .to_string();
    (
        negative && !digits.is_empty(),
        if digits.is_empty() {
            "0".to_string()
        } else {
            digits
        },
    )
}

fn is_decimal(value: &str) -> bool {
    let value = value.strip_prefix('-').unwrap_or(value);
    !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit())
}

fn entity_ids_from_store_value(value: &StoreValue) -> Vec<String> {
    match value {
        StoreValue::Array(values) => values.iter().filter_map(store_value_id).collect(),
        _ => store_value_id(value).into_iter().collect(),
    }
}

fn store_value_id(value: &StoreValue) -> Option<String> {
    match value {
        StoreValue::String(value) | StoreValue::Bytes(value) | StoreValue::BigInt(value) => {
            Some(value.clone())
        }
        StoreValue::Int(value) => Some(value.to_string()),
        StoreValue::Int8(value) | StoreValue::Timestamp(value) => Some(value.to_string()),
        _ => None,
    }
}

fn store_value_matches_id(value: &StoreValue, id: &str) -> bool {
    match value {
        StoreValue::Array(values) => values.iter().any(|value| store_value_matches_id(value, id)),
        _ => store_value_id(value).is_some_and(|value| ids_match(&value, id)),
    }
}

fn is_entity_type(snapshot: &StoreSnapshot, name: &str) -> bool {
    snapshot.schema.entities.contains_key(name)
}

fn entity_name_for_query(snapshot: &StoreSnapshot, query_name: &str) -> Option<String> {
    snapshot.schema.entities.keys().find_map(|name| {
        let singular = lower_first(name);
        let plural = plural_query_name(name);
        if query_name == singular || query_name == plural {
            Some(name.clone())
        } else {
            None
        }
    })
}

fn is_plural_query(snapshot: &StoreSnapshot, query_name: &str) -> bool {
    snapshot
        .schema
        .entities
        .keys()
        .any(|name| query_name == plural_query_name(name))
}

fn meta_value(snapshot: &StoreSnapshot) -> Value {
    let hash = snapshot
        .checkpoint
        .block_hash
        .as_ref()
        .map(|hash| Value::String(hash.clone()))
        .unwrap_or(Value::Null);
    json!({
        "block": {
            "number": snapshot.checkpoint.to_block,
            "hash": hash
        },
        "hasIndexingErrors": snapshot.checkpoint.validation_errors > 0
    })
}

fn introspection_schema(snapshot: &StoreSnapshot) -> Value {
    let mut types = vec![
        scalar_type("ID"),
        scalar_type("String"),
        scalar_type("Boolean"),
        scalar_type("Int"),
        scalar_type("Bytes"),
        scalar_type("BigInt"),
        scalar_type("BigDecimal"),
        meta_type(),
        meta_block_type(),
        block_height_input_type(),
        order_direction_enum_type(),
    ];
    types.push(query_type(snapshot));
    types.extend(
        snapshot
            .schema
            .entities
            .keys()
            .map(|entity| order_by_enum_type(snapshot, entity)),
    );
    types.extend(snapshot.schema.entities.values().map(|entity| {
        json!({
            "kind": "OBJECT",
            "name": entity.name,
            "description": Value::Null,
            "fields": entity.fields.values().map(|field| {
                json!({
                    "name": field.name,
                    "description": Value::Null,
                    "args": entity_field_args(snapshot, field),
                    "type": graph_type_ref(snapshot, &field.kind, field.list, field.required),
                    "isDeprecated": false,
                    "deprecationReason": Value::Null
                })
            }).collect::<Vec<_>>(),
            "inputFields": Value::Null,
            "interfaces": [],
            "enumValues": Value::Null,
            "possibleTypes": Value::Null
        })
    }));
    types.extend(
        snapshot
            .schema
            .entities
            .values()
            .map(|entity| filter_input_type(snapshot, &entity.name)),
    );
    json!({
        "queryType": query_type(snapshot),
        "mutationType": Value::Null,
        "subscriptionType": Value::Null,
        "types": types,
        "directives": standard_directives()
    })
}

fn introspection_type(snapshot: &StoreSnapshot, name: &str) -> Option<Value> {
    if name == "Query" {
        return Some(query_type(snapshot));
    }
    if matches!(
        name,
        "ID" | "String" | "Boolean" | "Int" | "Bytes" | "BigInt" | "BigDecimal"
    ) {
        return Some(scalar_type(name));
    }
    if name == "_Meta_" {
        return Some(meta_type());
    }
    if name == "_Block_" {
        return Some(meta_block_type());
    }
    if name == "_Block_height" {
        return Some(block_height_input_type());
    }
    if name == "OrderDirection" {
        return Some(order_direction_enum_type());
    }
    if let Some(entity_name) = name.strip_suffix("_orderBy") {
        if snapshot.schema.entities.contains_key(entity_name) {
            return Some(order_by_enum_type(snapshot, entity_name));
        }
    }
    if let Some(entity_name) = name.strip_suffix("_filter") {
        if snapshot.schema.entities.contains_key(entity_name) {
            return Some(filter_input_type(snapshot, entity_name));
        }
    }
    snapshot.schema.entities.get(name).map(|entity| {
        json!({
            "kind": "OBJECT",
            "name": entity.name,
            "description": Value::Null,
            "fields": entity.fields.values().map(|field| {
                json!({
                    "name": field.name,
                    "description": Value::Null,
                    "args": entity_field_args(snapshot, field),
                    "type": graph_type_ref(snapshot, &field.kind, field.list, field.required),
                    "isDeprecated": false,
                    "deprecationReason": Value::Null
                })
            }).collect::<Vec<_>>(),
            "inputFields": Value::Null,
            "interfaces": [],
            "enumValues": Value::Null,
            "possibleTypes": Value::Null
        })
    })
}

fn query_type(snapshot: &StoreSnapshot) -> Value {
    let mut fields = Vec::new();
    fields.push(json!({
        "name": "_meta",
        "description": Value::Null,
        "args": [argument("block", named_input_type_ref("_Block_height"), Value::Null)],
        "type": named_output_type_ref(snapshot, "_Meta_"),
        "isDeprecated": false,
        "deprecationReason": Value::Null
    }));
    for entity in snapshot.schema.entities.keys() {
        fields.push(json!({
            "name": lower_first(entity),
            "description": Value::Null,
            "args": [
                argument("id", non_null_type_ref(named_output_type_ref(snapshot, "ID")), Value::Null),
                argument("block", named_input_type_ref("_Block_height"), Value::Null)
            ],
            "type": named_output_type_ref(snapshot, entity),
            "isDeprecated": false,
            "deprecationReason": Value::Null
        }));
        fields.push(json!({
            "name": plural_query_name(entity),
            "description": Value::Null,
            "args": collection_args(entity, true),
            "type": non_null_type_ref(list_type_ref(non_null_type_ref(named_output_type_ref(snapshot, entity)))),
            "isDeprecated": false,
            "deprecationReason": Value::Null
        }));
    }
    json!({
        "kind": "OBJECT",
        "name": "Query",
        "description": Value::Null,
        "fields": fields,
        "inputFields": Value::Null,
        "interfaces": [],
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn entity_field_args(snapshot: &StoreSnapshot, field: &EntityField) -> Vec<Value> {
    if field.list && snapshot.schema.entities.contains_key(&field.kind) {
        collection_args(&field.kind, false)
    } else {
        Vec::new()
    }
}

fn collection_args(entity: &str, include_block: bool) -> Vec<Value> {
    let mut args = vec![
        argument(
            "first",
            named_output_type_ref_stub("SCALAR", "Int"),
            json!("100"),
        ),
        argument(
            "skip",
            named_output_type_ref_stub("SCALAR", "Int"),
            json!("0"),
        ),
        argument(
            "orderBy",
            named_enum_type_ref(format!("{entity}_orderBy")),
            json!("id"),
        ),
        argument(
            "orderDirection",
            named_enum_type_ref("OrderDirection"),
            json!("asc"),
        ),
        argument(
            "where",
            named_input_type_ref(format!("{entity}_filter")),
            Value::Null,
        ),
    ];
    if include_block {
        args.push(argument(
            "block",
            named_input_type_ref("_Block_height"),
            Value::Null,
        ));
    }
    args
}

fn argument(name: impl Into<String>, ty: Value, default_value: Value) -> Value {
    json!({
        "name": name.into(),
        "description": Value::Null,
        "type": ty,
        "defaultValue": default_value
    })
}

fn scalar_type(name: &str) -> Value {
    json!({
        "kind": "SCALAR",
        "name": name,
        "description": Value::Null,
        "fields": Value::Null,
        "inputFields": Value::Null,
        "interfaces": Value::Null,
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn meta_type() -> Value {
    json!({
        "kind": "OBJECT",
        "name": "_Meta_",
        "description": Value::Null,
        "fields": [
            { "name": "block", "description": Value::Null, "args": [], "type": non_null_type_ref(named_output_type_ref_stub("OBJECT", "_Block_")), "isDeprecated": false, "deprecationReason": Value::Null },
            { "name": "hasIndexingErrors", "description": Value::Null, "args": [], "type": non_null_type_ref(named_output_type_ref_stub("SCALAR", "Boolean")), "isDeprecated": false, "deprecationReason": Value::Null }
        ],
        "inputFields": Value::Null,
        "interfaces": [],
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn meta_block_type() -> Value {
    json!({
        "kind": "OBJECT",
        "name": "_Block_",
        "description": Value::Null,
        "fields": [
            { "name": "hash", "description": Value::Null, "args": [], "type": named_output_type_ref_stub("SCALAR", "Bytes"), "isDeprecated": false, "deprecationReason": Value::Null },
            { "name": "number", "description": Value::Null, "args": [], "type": non_null_type_ref(named_output_type_ref_stub("SCALAR", "Int")), "isDeprecated": false, "deprecationReason": Value::Null }
        ],
        "inputFields": Value::Null,
        "interfaces": [],
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn block_height_input_type() -> Value {
    json!({
        "kind": "INPUT_OBJECT",
        "name": "_Block_height",
        "description": Value::Null,
        "fields": Value::Null,
        "inputFields": [
            { "name": "hash", "description": Value::Null, "type": named_output_type_ref_stub("SCALAR", "Bytes"), "defaultValue": Value::Null },
            { "name": "number", "description": Value::Null, "type": named_output_type_ref_stub("SCALAR", "Int"), "defaultValue": Value::Null }
        ],
        "interfaces": Value::Null,
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn order_direction_enum_type() -> Value {
    enum_type("OrderDirection", ["asc", "desc"])
}

fn order_by_enum_type(snapshot: &StoreSnapshot, entity_name: &str) -> Value {
    let mut values = BTreeSet::new();
    values.insert("id".to_string());
    if let Some(entity) = snapshot.schema.entities.get(entity_name) {
        for field in entity.fields.values() {
            if !field.list {
                values.insert(field.name.clone());
            }
            if !field.list && is_entity_type(snapshot, &field.kind) {
                if let Some(related) = snapshot.schema.entities.get(&field.kind) {
                    for related_field in related.fields.values().filter(|field| !field.list) {
                        values.insert(format!("{}__{}", field.name, related_field.name));
                    }
                }
            }
        }
    }
    enum_type(format!("{entity_name}_orderBy"), values)
}

fn enum_type(
    name: impl Into<String>,
    values: impl IntoIterator<Item = impl Into<String>>,
) -> Value {
    json!({
        "kind": "ENUM",
        "name": name.into(),
        "description": Value::Null,
        "fields": Value::Null,
        "inputFields": Value::Null,
        "interfaces": Value::Null,
        "enumValues": values.into_iter().map(|name| {
            json!({
                "name": name.into(),
                "description": Value::Null,
                "isDeprecated": false,
                "deprecationReason": Value::Null
            })
        }).collect::<Vec<_>>(),
        "possibleTypes": Value::Null
    })
}

fn filter_input_type(snapshot: &StoreSnapshot, entity_name: &str) -> Value {
    let input_fields = snapshot
        .schema
        .entities
        .get(entity_name)
        .map(|entity| {
            let mut fields = vec![
                input_field(
                    "and",
                    list_type_ref(non_null_type_ref(named_input_type_ref(format!(
                        "{entity_name}_filter"
                    )))),
                ),
                input_field(
                    "or",
                    list_type_ref(non_null_type_ref(named_input_type_ref(format!(
                        "{entity_name}_filter"
                    )))),
                ),
            ];
            for field in entity.fields.values() {
                if field.list {
                    continue;
                }
                let ty = if is_entity_type(snapshot, &field.kind) {
                    named_output_type_ref(snapshot, "Bytes")
                } else {
                    named_output_type_ref(snapshot, &field.kind)
                };
                fields.push(input_field(&field.name, ty.clone()));
                fields.push(input_field(format!("{}_not", field.name), ty.clone()));
                fields.push(input_field(
                    format!("{}_in", field.name),
                    list_type_ref(ty.clone()),
                ));
                fields.push(input_field(
                    format!("{}_not_in", field.name),
                    list_type_ref(ty.clone()),
                ));
                fields.push(input_field(format!("{}_gt", field.name), ty.clone()));
                fields.push(input_field(format!("{}_gte", field.name), ty.clone()));
                fields.push(input_field(format!("{}_lt", field.name), ty.clone()));
                fields.push(input_field(format!("{}_lte", field.name), ty.clone()));
                fields.push(input_field(format!("{}_contains", field.name), ty.clone()));
                fields.push(input_field(
                    format!("{}_not_contains", field.name),
                    ty.clone(),
                ));
                fields.push(input_field(
                    format!("{}_contains_nocase", field.name),
                    ty.clone(),
                ));
                fields.push(input_field(
                    format!("{}_not_contains_nocase", field.name),
                    ty.clone(),
                ));
                fields.push(input_field(
                    format!("{}_starts_with", field.name),
                    ty.clone(),
                ));
                fields.push(input_field(
                    format!("{}_starts_with_nocase", field.name),
                    ty.clone(),
                ));
                fields.push(input_field(format!("{}_ends_with", field.name), ty.clone()));
                fields.push(input_field(
                    format!("{}_ends_with_nocase", field.name),
                    ty.clone(),
                ));
                if is_entity_type(snapshot, &field.kind) || field.derived {
                    fields.push(input_field(
                        format!("{}_", field.name),
                        named_input_type_ref(format!("{}_filter", field.kind)),
                    ));
                }
            }
            fields
        })
        .unwrap_or_default();
    json!({
        "kind": "INPUT_OBJECT",
        "name": format!("{entity_name}_filter"),
        "description": Value::Null,
        "fields": Value::Null,
        "inputFields": input_fields,
        "interfaces": Value::Null,
        "enumValues": Value::Null,
        "possibleTypes": Value::Null
    })
}

fn input_field(name: impl Into<String>, ty: Value) -> Value {
    json!({
        "name": name.into(),
        "description": Value::Null,
        "type": ty,
        "defaultValue": Value::Null
    })
}

fn graph_type_ref(snapshot: &StoreSnapshot, name: &str, list: bool, required: bool) -> Value {
    let inner = named_output_type_ref(snapshot, name);
    let value = if list { list_type_ref(inner) } else { inner };
    if required {
        non_null_type_ref(value)
    } else {
        value
    }
}

fn named_output_type_ref(snapshot: &StoreSnapshot, name: &str) -> Value {
    if is_scalar_type(name) {
        named_output_type_ref_stub("SCALAR", name)
    } else if matches!(name, "_Meta_" | "_Block_") || snapshot.schema.entities.contains_key(name) {
        named_output_type_ref_stub("OBJECT", name)
    } else {
        named_output_type_ref_stub("SCALAR", name)
    }
}

fn named_output_type_ref_stub(kind: &str, name: impl Into<String>) -> Value {
    json!({ "kind": kind, "name": name.into(), "ofType": Value::Null })
}

fn named_input_type_ref(name: impl Into<String>) -> Value {
    let name = name.into();
    let kind = if is_scalar_type(&name) {
        "SCALAR"
    } else {
        "INPUT_OBJECT"
    };
    json!({ "kind": kind, "name": name, "ofType": Value::Null })
}

fn named_enum_type_ref(name: impl Into<String>) -> Value {
    json!({ "kind": "ENUM", "name": name.into(), "ofType": Value::Null })
}

fn list_type_ref(of_type: Value) -> Value {
    json!({ "kind": "LIST", "name": Value::Null, "ofType": of_type })
}

fn non_null_type_ref(of_type: Value) -> Value {
    json!({ "kind": "NON_NULL", "name": Value::Null, "ofType": of_type })
}

fn is_scalar_type(name: &str) -> bool {
    matches!(
        name,
        "ID" | "String" | "Boolean" | "Int" | "Bytes" | "BigInt" | "BigDecimal"
    )
}

fn standard_directives() -> Vec<Value> {
    vec![
        directive(
            "include",
            ["FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"],
            vec![argument(
                "if",
                non_null_type_ref(named_output_type_ref_stub("SCALAR", "Boolean")),
                Value::Null,
            )],
        ),
        directive(
            "skip",
            ["FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"],
            vec![argument(
                "if",
                non_null_type_ref(named_output_type_ref_stub("SCALAR", "Boolean")),
                Value::Null,
            )],
        ),
        directive(
            "deprecated",
            [
                "FIELD_DEFINITION",
                "ARGUMENT_DEFINITION",
                "INPUT_FIELD_DEFINITION",
                "ENUM_VALUE",
            ],
            vec![argument(
                "reason",
                named_output_type_ref_stub("SCALAR", "String"),
                json!("No longer supported"),
            )],
        ),
        directive(
            "specifiedBy",
            ["SCALAR"],
            vec![argument(
                "url",
                non_null_type_ref(named_output_type_ref_stub("SCALAR", "String")),
                Value::Null,
            )],
        ),
    ]
}

fn directive(
    name: impl Into<String>,
    locations: impl IntoIterator<Item = impl Into<String>>,
    args: Vec<Value>,
) -> Value {
    json!({
        "name": name.into(),
        "description": Value::Null,
        "locations": locations.into_iter().map(Into::into).collect::<Vec<String>>(),
        "args": args,
        "isRepeatable": false
    })
}

fn project_json(value: &Value, selection: &[ParsedField]) -> Value {
    if selection.is_empty() {
        return value.clone();
    }
    match value {
        Value::Object(object) => {
            let mut projected = Map::new();
            for field in selection {
                if field.name == "__typename" {
                    projected.insert(
                        field.response_name.clone(),
                        Value::String("Object".to_string()),
                    );
                    continue;
                }
                projected.insert(
                    field.response_name.clone(),
                    object
                        .get(&field.name)
                        .map(|value| project_json(value, &field.selection))
                        .unwrap_or(Value::Null),
                );
            }
            Value::Object(projected)
        }
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| project_json(value, selection))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn operation_body(query: &str, operation_name: Option<&str>) -> Result<String, String> {
    if let Some(operation_name) = operation_name {
        return named_operation_body(query, operation_name);
    }
    if let Some(body) = first_operation_body(query)? {
        return Ok(body);
    }
    let start = query
        .find('{')
        .ok_or_else(|| "query is missing a selection set".to_string())?;
    let end = matching_delimiter(query, start, '{', '}')
        .ok_or_else(|| "query has unbalanced selection braces".to_string())?;
    Ok(query[start + 1..end].to_string())
}

fn first_operation_body(query: &str) -> Result<Option<String>, String> {
    let mut index = 0;
    while index < query.len() {
        skip_ignored(query, &mut index);
        if index >= query.len() {
            break;
        }
        if peek(query, index) == Some('{') {
            let end = matching_delimiter(query, index, '{', '}')
                .ok_or_else(|| "anonymous query has unbalanced selection braces".to_string())?;
            return Ok(Some(query[index + 1..end].to_string()));
        }
        let checkpoint = index;
        let Some(kind) = read_ident(query, &mut index) else {
            index += query[index..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(1);
            continue;
        };
        if kind == "fragment" {
            if let Some(open) = query[index..].find('{').map(|offset| index + offset) {
                if let Some(close) = matching_delimiter(query, open, '{', '}') {
                    index = close + 1;
                    continue;
                }
            }
            return Err("fragment has unbalanced braces".to_string());
        }
        if matches!(kind.as_str(), "query" | "mutation" | "subscription") {
            let start = query[index..]
                .find('{')
                .map(|offset| index + offset)
                .ok_or_else(|| format!("operation `{kind}` is missing a selection set"))?;
            let end = matching_delimiter(query, start, '{', '}')
                .ok_or_else(|| format!("operation `{kind}` has unbalanced selection braces"))?;
            return Ok(Some(query[start + 1..end].to_string()));
        }
        index = checkpoint + kind.len();
    }
    Ok(None)
}

fn named_operation_body(query: &str, operation_name: &str) -> Result<String, String> {
    let mut index = 0;
    while index < query.len() {
        skip_ignored(query, &mut index);
        if index >= query.len() {
            break;
        }
        let Some(kind) = read_ident(query, &mut index) else {
            index += query[index..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(1);
            continue;
        };
        if !matches!(kind.as_str(), "query" | "mutation" | "subscription") {
            continue;
        }
        skip_ignored(query, &mut index);
        let name = read_ident(query, &mut index);
        let Some(name) = name else {
            continue;
        };
        let start = query[index..]
            .find('{')
            .map(|offset| index + offset)
            .ok_or_else(|| format!("operation `{name}` is missing a selection set"))?;
        let end = matching_delimiter(query, start, '{', '}')
            .ok_or_else(|| format!("operation `{name}` has unbalanced selection braces"))?;
        if name == operation_name {
            return Ok(query[start + 1..end].to_string());
        }
        index = end + 1;
    }
    Err(format!("operation `{operation_name}` was not found"))
}

fn extract_fragments(query: &str) -> Result<FragmentMap, String> {
    let mut fragments = FragmentMap::new();
    let mut index = 0;
    while index < query.len() {
        skip_ignored(query, &mut index);
        if index >= query.len() {
            break;
        }
        let start = index;
        let Some(keyword) = read_ident(query, &mut index) else {
            index += query[index..]
                .chars()
                .next()
                .map(char::len_utf8)
                .unwrap_or(1);
            continue;
        };
        if keyword != "fragment" {
            continue;
        }
        skip_ignored(query, &mut index);
        let name = read_ident(query, &mut index)
            .ok_or_else(|| format!("expected fragment name at byte {index}"))?;
        skip_ignored(query, &mut index);
        let Some(on_keyword) = read_ident(query, &mut index) else {
            return Err(format!("expected `on` after fragment `{name}`"));
        };
        if on_keyword != "on" {
            return Err(format!("expected `on` after fragment `{name}`"));
        }
        skip_ignored(query, &mut index);
        let _type_name = read_ident(query, &mut index)
            .ok_or_else(|| format!("expected fragment type condition at byte {index}"))?;
        let open = query[index..]
            .find('{')
            .map(|offset| index + offset)
            .ok_or_else(|| format!("fragment `{name}` is missing a selection set"))?;
        let close = matching_delimiter(query, open, '{', '}')
            .ok_or_else(|| format!("fragment `{name}` has unbalanced braces"))?;
        fragments.insert(name, query[open + 1..close].to_string());
        index = close + 1;
        if index <= start {
            break;
        }
    }
    Ok(fragments)
}

fn parse_fields(
    selection: &str,
    variables: &Value,
    fragments: &FragmentMap,
) -> Result<Vec<ParsedField>, String> {
    let mut fields = Vec::new();
    let mut index = 0;
    while index < selection.len() {
        skip_ignored(selection, &mut index);
        if index >= selection.len() {
            break;
        }
        if peek(selection, index) == Some('}') {
            break;
        }
        if selection[index..].starts_with("...") {
            index += 3;
            skip_ignored(selection, &mut index);
            let Some(name) = read_ident(selection, &mut index) else {
                return Err(format!("expected fragment name at byte {index}"));
            };
            if name == "on" {
                skip_ignored(selection, &mut index);
                let _type_name = read_ident(selection, &mut index)
                    .ok_or_else(|| format!("expected inline fragment type at byte {index}"))?;
                let include = directives_include(selection, &mut index, variables)?;
                skip_ignored(selection, &mut index);
                if peek(selection, index) != Some('{') {
                    return Err("inline fragment is missing a selection set".to_string());
                }
                let end = matching_delimiter(selection, index, '{', '}')
                    .ok_or_else(|| "inline fragment has unbalanced braces".to_string())?;
                if include {
                    fields.extend(parse_fields(
                        &selection[index + 1..end],
                        variables,
                        fragments,
                    )?);
                }
                index = end + 1;
            } else if let Some(fragment) = fragments.get(&name) {
                if directives_include(selection, &mut index, variables)? {
                    fields.extend(parse_fields(fragment, variables, fragments)?);
                }
            } else {
                return Err(format!("unknown fragment `{name}`"));
            }
            continue;
        }
        let first = read_ident(selection, &mut index)
            .ok_or_else(|| format!("expected field at byte {index}"))?;
        skip_ignored(selection, &mut index);

        let (response_name, name) = if peek(selection, index) == Some(':') {
            index += 1;
            skip_ignored(selection, &mut index);
            let name = read_ident(selection, &mut index)
                .ok_or_else(|| format!("expected aliased field at byte {index}"))?;
            (first, name)
        } else {
            (first.clone(), first)
        };

        skip_ignored(selection, &mut index);
        let args = if peek(selection, index) == Some('(') {
            let end = matching_delimiter(selection, index, '(', ')')
                .ok_or_else(|| format!("unbalanced arguments for field `{name}`"))?;
            let args = parse_args(&selection[index + 1..end], variables)?;
            index = end + 1;
            args
        } else {
            BTreeMap::new()
        };

        skip_ignored(selection, &mut index);
        let include = directives_include(selection, &mut index, variables)?;
        skip_ignored(selection, &mut index);
        let nested = if peek(selection, index) == Some('{') {
            let end = matching_delimiter(selection, index, '{', '}')
                .ok_or_else(|| format!("unbalanced selection for field `{name}`"))?;
            let nested = parse_fields(&selection[index + 1..end], variables, fragments)?;
            index = end + 1;
            nested
        } else {
            Vec::new()
        };

        if include {
            fields.push(ParsedField {
                response_name,
                name,
                args,
                selection: nested,
            });
        }
    }
    Ok(fields)
}

fn parse_args(args: &str, variables: &Value) -> Result<BTreeMap<String, Value>, String> {
    let mut parsed = BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        skip_ignored(args, &mut index);
        if index >= args.len() {
            break;
        }
        let key = read_ident(args, &mut index)
            .ok_or_else(|| format!("expected argument name at byte {index}"))?;
        skip_ignored(args, &mut index);
        if peek(args, index) != Some(':') {
            return Err(format!("expected ':' after argument `{key}`"));
        }
        index += 1;
        let value = parse_value(args, &mut index, variables)?;
        parsed.insert(key, value);
        skip_ignored(args, &mut index);
    }
    Ok(parsed)
}

fn directives_include(input: &str, index: &mut usize, variables: &Value) -> Result<bool, String> {
    let mut include = true;
    loop {
        skip_ignored(input, index);
        if peek(input, *index) != Some('@') {
            break;
        }
        *index += 1;
        let name = read_ident(input, index)
            .ok_or_else(|| format!("expected directive name at byte {index}"))?;
        skip_ignored(input, index);
        if peek(input, *index) == Some('(') {
            let end = matching_delimiter(input, *index, '(', ')')
                .ok_or_else(|| "directive arguments have unbalanced parentheses".to_string())?;
            let args = parse_args(&input[*index + 1..end], variables)?;
            let if_arg = args.get("if").and_then(Value::as_bool);
            match name.as_str() {
                "skip" if if_arg == Some(true) => include = false,
                "include" if if_arg == Some(false) => include = false,
                _ => {}
            }
            *index = end + 1;
        }
    }
    Ok(include)
}

fn parse_value(input: &str, index: &mut usize, variables: &Value) -> Result<Value, String> {
    skip_ignored(input, index);
    let Some(ch) = peek(input, *index) else {
        return Err("expected value".to_string());
    };
    match ch {
        '"' => parse_string(input, index).map(Value::String),
        '\'' => parse_single_quoted_string(input, index).map(Value::String),
        '$' => {
            *index += 1;
            let name = read_ident(input, index)
                .ok_or_else(|| format!("expected variable name at byte {index}"))?;
            Ok(variables.get(&name).cloned().unwrap_or(Value::Null))
        }
        '[' => parse_array(input, index, variables),
        '{' => parse_object(input, index, variables),
        '-' | '0'..='9' => parse_number(input, index),
        _ => {
            let ident = read_ident(input, index)
                .ok_or_else(|| format!("expected value at byte {index}"))?;
            match ident.as_str() {
                "true" => Ok(Value::Bool(true)),
                "false" => Ok(Value::Bool(false)),
                "null" => Ok(Value::Null),
                _ => Ok(Value::String(ident)),
            }
        }
    }
}

fn parse_array(input: &str, index: &mut usize, variables: &Value) -> Result<Value, String> {
    *index += 1;
    let mut values = Vec::new();
    loop {
        skip_ignored(input, index);
        if peek(input, *index) == Some(']') {
            *index += 1;
            break;
        }
        values.push(parse_value(input, index, variables)?);
        skip_ignored(input, index);
    }
    Ok(Value::Array(values))
}

fn parse_object(input: &str, index: &mut usize, variables: &Value) -> Result<Value, String> {
    *index += 1;
    let mut object = Map::new();
    loop {
        skip_ignored(input, index);
        if peek(input, *index) == Some('}') {
            *index += 1;
            break;
        }
        let key = match peek(input, *index) {
            Some('"') => parse_string(input, index)?,
            Some('\'') => parse_single_quoted_string(input, index)?,
            _ => read_ident(input, index)
                .ok_or_else(|| format!("expected object key at byte {index}"))?,
        };
        skip_ignored(input, index);
        if peek(input, *index) != Some(':') {
            return Err(format!("expected ':' after object key `{key}`"));
        }
        *index += 1;
        let value = parse_value(input, index, variables)?;
        object.insert(key, value);
        skip_ignored(input, index);
    }
    Ok(Value::Object(object))
}

fn parse_number(input: &str, index: &mut usize) -> Result<Value, String> {
    let start = *index;
    if peek(input, *index) == Some('-') {
        *index += 1;
    }
    while *index < input.len() {
        let Some(ch) = peek(input, *index) else {
            break;
        };
        if ch.is_ascii_digit() || ch == '.' {
            *index += ch.len_utf8();
        } else {
            break;
        }
    }
    let raw = &input[start..*index];
    if raw.contains('.') {
        raw.parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| format!("invalid number `{raw}`"))
    } else {
        raw.parse::<i64>()
            .map(|value| Value::Number(Number::from(value)))
            .map_err(|_| format!("invalid number `{raw}`"))
    }
}

fn parse_string(input: &str, index: &mut usize) -> Result<String, String> {
    parse_quoted_string(input, index, '"')
}

fn parse_single_quoted_string(input: &str, index: &mut usize) -> Result<String, String> {
    parse_quoted_string(input, index, '\'')
}

fn parse_quoted_string(input: &str, index: &mut usize, quote: char) -> Result<String, String> {
    *index += quote.len_utf8();
    let mut output = String::new();
    let mut escaped = false;
    while *index < input.len() {
        let Some(ch) = peek(input, *index) else {
            break;
        };
        *index += ch.len_utf8();
        if escaped {
            output.push(match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                '"' => '"',
                '\'' => '\'',
                '\\' => '\\',
                other => other,
            });
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            return Ok(output);
        }
        output.push(ch);
    }
    Err("unterminated string literal".to_string())
}

fn read_ident(input: &str, index: &mut usize) -> Option<String> {
    let start = *index;
    while *index < input.len() {
        let ch = input[*index..].chars().next()?;
        if ch.is_ascii_alphanumeric() || ch == '_' {
            *index += ch.len_utf8();
        } else {
            break;
        }
    }
    if *index == start {
        None
    } else {
        Some(input[start..*index].to_string())
    }
}

fn skip_ignored(input: &str, index: &mut usize) {
    while *index < input.len() {
        let Some(ch) = input[*index..].chars().next() else {
            break;
        };
        if ch.is_whitespace() || ch == ',' {
            *index += ch.len_utf8();
        } else {
            break;
        }
    }
}

fn matching_delimiter(input: &str, start: usize, open: char, close: char) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_quote = '"';
    let mut escaped = false;
    for (index, ch) in input[start..].char_indices() {
        let absolute = start + index;
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == string_quote {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' | '\'' => {
                in_string = true;
                string_quote = ch;
            }
            _ if ch == open => depth += 1,
            _ if ch == close => {
                depth -= 1;
                if depth == 0 {
                    return Some(absolute);
                }
            }
            _ => {}
        }
    }
    None
}

fn peek(input: &str, index: usize) -> Option<char> {
    input[index..].chars().next()
}

fn strip_comments(query: &str) -> String {
    let mut output = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(ch) = chars.next() {
        if in_string {
            output.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                output.push(ch);
            }
            '#' => {
                for next in chars.by_ref() {
                    if next == '\n' {
                        output.push('\n');
                        break;
                    }
                }
            }
            _ => output.push(ch),
        }
    }
    output
}

fn arg_string(field: &ParsedField, name: &str) -> Option<String> {
    field.args.get(name).and_then(value_to_string)
}

fn arg_usize(field: &ParsedField, name: &str) -> Option<usize> {
    field.args.get(name).and_then(|value| match value {
        Value::Number(value) => value.as_u64().and_then(|value| usize::try_from(value).ok()),
        Value::String(value) => value.parse::<usize>().ok(),
        _ => None,
    })
}

fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn lower_first(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    format!(
        "{}{}",
        first.to_ascii_lowercase(),
        chars.collect::<String>()
    )
}

fn plural_query_name(entity: &str) -> String {
    let singular = lower_first(entity);
    if singular.ends_with('s') {
        format!("{singular}es")
    } else {
        format!("{singular}s")
    }
}

fn store_value_to_json(value: &StoreValue) -> Value {
    match value {
        StoreValue::String(value) => Value::String(value.clone()),
        StoreValue::Int(value) => json!(value),
        StoreValue::BigDecimal { digits, exp } => Value::String(format_big_decimal(digits, exp)),
        StoreValue::Bool(value) => Value::Bool(*value),
        StoreValue::Array(values) => Value::Array(values.iter().map(store_value_to_json).collect()),
        StoreValue::Null => Value::Null,
        StoreValue::Bytes(value) | StoreValue::BigInt(value) => Value::String(value.clone()),
        StoreValue::Int8(value) | StoreValue::Timestamp(value) => json!(value),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn format_big_decimal(digits: &str, exp: &str) -> String {
    let exp = exp.parse::<i64>().unwrap_or(0);
    if exp == 0 {
        return digits.to_string();
    }
    if exp > 0 {
        return format!("{}{}", digits, "0".repeat(exp as usize));
    }

    let negative = digits.starts_with('-');
    let digits = digits.trim_start_matches('-');
    let shift = (-exp) as usize;
    let value = if shift >= digits.len() {
        format!("0.{}{}", "0".repeat(shift - digits.len()), digits)
    } else {
        let split = digits.len() - shift;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{value}")
    } else {
        value
    }
}

fn ids_match(left: &str, right: &str) -> bool {
    left == right || left.eq_ignore_ascii_case(right)
}

fn hexish(value: &str) -> bool {
    value.starts_with("0x") || value.starts_with("0X")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{HistoricalSnapshot, StoreSnapshot, SyncCheckpoint};
    use ugraph_core::{EntityField, EntitySchema, EntityType};

    #[test]
    fn serves_single_entity_and_meta() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ _meta { block { number } } protocol(id: "0xabc") { id growToken } }"#,
        );
        assert_eq!(result["data"]["_meta"]["block"]["number"], 42);
        assert_eq!(result["data"]["protocol"]["id"], "0xabc");
        assert_eq!(result["data"]["protocol"]["growToken"], "0xdef");
    }

    #[test]
    fn serves_entity_lists_with_first_skip_and_where() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ protocols(first: 1, skip: 0, where: { name_contains: "bb" }) { id name } }"#,
        );
        assert_eq!(result["data"]["protocols"][0]["id"], "0xbbb");
    }

    #[test]
    fn serves_variables_nested_relations_and_derived_fields() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql_with_variables(
            &snapshot,
            r#"query($id: ID!) { protocol(id: $id) { id owner { id name } purchases(orderBy: amount, orderDirection: desc) { id amount } } }"#,
            &json!({ "id": "0xabc" }),
        );
        assert_eq!(result["data"]["protocol"]["owner"]["name"], "Alice");
        assert_eq!(result["data"]["protocol"]["purchases"][0]["amount"], "12");
    }

    #[test]
    fn detects_queries_that_need_historical_snapshots() {
        assert!(!query_needs_history(
            r#"{ purchases(first: 1) { id block } }"#,
            &Value::Null,
            None
        ));
        assert!(query_needs_history(
            r#"{ _meta(block: { number: 20 }) { block { number } } }"#,
            &Value::Null,
            None
        ));
        assert!(query_needs_history(
            r#"{ protocol(id: "0xabc", block: { number: 20 }) { id name } }"#,
            &Value::Null,
            None
        ));
    }

    #[test]
    fn rejects_unknown_entity_fields_like_graph_node() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ protocol(id: "0xabc") { id missingField } }"#,
        );

        assert_eq!(
            result["errors"][0]["message"],
            "Type `Protocol` has no field `missingField`"
        );
        assert!(result.get("data").is_none());
    }

    #[test]
    fn rejects_unknown_nested_relation_fields_like_graph_node() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ protocol(id: "0xabc") { owner { id email } } }"#,
        );

        assert_eq!(
            result["errors"][0]["message"],
            "Type `User` has no field `email`"
        );
    }

    #[test]
    fn serves_introspection_shape() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ __type(name: "Protocol") { name fields { name type { kind name ofType { name } } } } }"#,
        );
        assert_eq!(result["data"]["__type"]["name"], "Protocol");
        assert!(result["data"]["__type"]["fields"].is_array());
    }

    #[test]
    fn exposes_generated_introspection_types() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"
            {
              __schema {
                directives { name locations }
                queryType { fields { name } }
              }
              queryType: __type(name: "Query") {
                fields {
                  name
                  args { name type { kind name ofType { kind name } } }
                }
              }
              protocolType: __type(name: "Protocol") {
                fields {
                  name
                  args { name type { kind name } }
                }
              }
              orderDirection: __type(name: "OrderDirection") {
                kind
                enumValues { name }
              }
              protocolOrderBy: __type(name: "Protocol_orderBy") {
                kind
                enumValues { name }
              }
            }
            "#,
        );
        let directives = result["data"]["__schema"]["directives"]
            .as_array()
            .expect("directives");
        assert!(directives
            .iter()
            .any(|directive| directive["name"] == "include"));
        assert!(directives
            .iter()
            .any(|directive| directive["name"] == "skip"));

        let query_fields = result["data"]["queryType"]["fields"]
            .as_array()
            .expect("query fields");
        let protocols = query_fields
            .iter()
            .find(|field| field["name"] == "protocols")
            .expect("protocols query");
        assert!(protocols["args"]
            .as_array()
            .expect("protocols args")
            .iter()
            .any(|arg| arg["name"] == "orderBy" && arg["type"]["kind"] == "ENUM"));
        assert!(result["data"]["__schema"]["queryType"]["fields"]
            .as_array()
            .expect("schema query fields")
            .iter()
            .any(|field| field["name"] == "protocols"));

        let protocol_fields = result["data"]["protocolType"]["fields"]
            .as_array()
            .expect("protocol fields");
        let purchases = protocol_fields
            .iter()
            .find(|field| field["name"] == "purchases")
            .expect("purchases field");
        assert!(purchases["args"]
            .as_array()
            .expect("purchases args")
            .iter()
            .any(|arg| arg["name"] == "where"));

        assert_eq!(result["data"]["orderDirection"]["kind"], "ENUM");
        assert!(result["data"]["protocolOrderBy"]["enumValues"]
            .as_array()
            .expect("orderBy values")
            .iter()
            .any(|value| value["name"] == "owner__name"));
    }

    #[test]
    fn selects_named_operation() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql_with_operation(
            &snapshot,
            r#"query First { protocols { id } } query Second($id: ID!) { protocol(id: $id) { id name } }"#,
            &json!({ "id": "0xabc" }),
            Some("Second"),
        );
        assert_eq!(result["data"]["protocol"]["name"], "alpha");
    }

    #[test]
    fn expands_named_and_inline_fragments() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"
            fragment ProtocolFields on Protocol {
              id
              ... on Protocol {
                name
              }
            }
            query {
              protocol(id: "0xabc") {
                ...ProtocolFields
                purchases { id }
              }
            }
            "#,
        );
        assert_eq!(result["data"]["protocol"]["id"], "0xabc");
        assert_eq!(result["data"]["protocol"]["name"], "alpha");
        assert_eq!(result["data"]["protocol"]["purchases"][0]["id"], "0xaaa1");
    }

    #[test]
    fn handles_anonymous_query_after_fragments_and_hash_in_strings() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"
            fragment ProtocolFields on Protocol {
              id
              name
            }
            {
              protocol(id: "0xabc") {
                ...ProtocolFields
              }
              protocols(where: { name: "a#lpha" }) {
                id
              }
            }
            "#,
        );
        assert_eq!(result["data"]["protocol"]["name"], "alpha");
        assert!(result["data"]["protocols"]
            .as_array()
            .expect("protocols")
            .is_empty());
    }

    #[test]
    fn honors_include_and_skip_directives() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql_with_variables(
            &snapshot,
            r#"
            fragment HiddenFields on Protocol {
              growToken
            }
            query($showName: Boolean!, $showHidden: Boolean!) {
              protocol(id: "0xabc") {
                id
                name @include(if: $showName)
                growToken @skip(if: true)
                ...HiddenFields @include(if: $showHidden)
                ... on Protocol @skip(if: true) {
                  owner { id }
                }
              }
            }
            "#,
            &json!({
                "showName": true,
                "showHidden": false,
            }),
        );
        let protocol = &result["data"]["protocol"];
        assert_eq!(protocol["id"], "0xabc");
        assert_eq!(protocol["name"], "alpha");
        assert!(protocol.get("growToken").is_none());
        assert!(protocol.get("owner").is_none());
    }

    #[test]
    fn meta_uses_selected_historical_snapshot() {
        let snapshot = fixture_snapshot();
        let current = execute_graphql(&snapshot, r#"{ _meta { block { number hash } } }"#);
        assert_eq!(current["data"]["_meta"]["block"]["hash"], "0xblock");
        let historical = execute_graphql(
            &snapshot,
            r#"{ _meta(block: { number: 20 }) { block { number hash } } }"#,
        );
        assert_eq!(historical["data"]["_meta"]["block"]["number"], 20);
        assert!(historical["data"]["_meta"]["block"]["hash"].is_null());
    }

    #[test]
    fn root_entity_query_can_read_historical_block() {
        let snapshot = fixture_snapshot();
        let result = execute_graphql(
            &snapshot,
            r#"{ protocol(id: "0xabc", block: { number: 20 }) { id name } }"#,
        );
        assert_eq!(result["data"]["protocol"]["id"], "0xabc");
        assert_eq!(result["data"]["protocol"]["name"], "old-alpha");
    }

    fn fixture_snapshot() -> StoreSnapshot {
        let mut schema = EntitySchema::default();
        schema.entities.insert(
            "Protocol".to_string(),
            EntityType {
                name: "Protocol".to_string(),
                fields: [
                    field("id", "Bytes"),
                    field("growToken", "Bytes"),
                    field("name", "String"),
                    relation("owner", "User", false, false, None),
                    relation("purchases", "Purchase", true, true, Some("protocol")),
                ]
                .into_iter()
                .map(|field| (field.name.clone(), field))
                .collect(),
            },
        );
        schema.entities.insert(
            "User".to_string(),
            EntityType {
                name: "User".to_string(),
                fields: [field("id", "Bytes"), field("name", "String")]
                    .into_iter()
                    .map(|field| (field.name.clone(), field))
                    .collect(),
            },
        );
        schema.entities.insert(
            "Purchase".to_string(),
            EntityType {
                name: "Purchase".to_string(),
                fields: [
                    field("id", "Bytes"),
                    field("amount", "BigInt"),
                    relation("protocol", "Protocol", false, false, None),
                ]
                .into_iter()
                .map(|field| (field.name.clone(), field))
                .collect(),
            },
        );

        let mut first = BTreeMap::new();
        first.insert("id".to_string(), StoreValue::Bytes("0xabc".to_string()));
        first.insert(
            "growToken".to_string(),
            StoreValue::Bytes("0xdef".to_string()),
        );
        first.insert("name".to_string(), StoreValue::String("alpha".to_string()));
        first.insert("owner".to_string(), StoreValue::Bytes("0x111".to_string()));

        let mut second = BTreeMap::new();
        second.insert("id".to_string(), StoreValue::Bytes("0xbbb".to_string()));
        second.insert(
            "growToken".to_string(),
            StoreValue::Bytes("0xeee".to_string()),
        );
        second.insert("name".to_string(), StoreValue::String("bbb".to_string()));

        let mut user = BTreeMap::new();
        user.insert("id".to_string(), StoreValue::Bytes("0x111".to_string()));
        user.insert("name".to_string(), StoreValue::String("Alice".to_string()));

        let mut purchase_one = BTreeMap::new();
        purchase_one.insert("id".to_string(), StoreValue::Bytes("0xaaa1".to_string()));
        purchase_one.insert("amount".to_string(), StoreValue::BigInt("4".to_string()));
        purchase_one.insert(
            "protocol".to_string(),
            StoreValue::Bytes("0xabc".to_string()),
        );

        let mut purchase_two = BTreeMap::new();
        purchase_two.insert("id".to_string(), StoreValue::Bytes("0xaaa2".to_string()));
        purchase_two.insert("amount".to_string(), StoreValue::BigInt("12".to_string()));
        purchase_two.insert(
            "protocol".to_string(),
            StoreValue::Bytes("0xabc".to_string()),
        );

        let historical_protocol = EntitySnapshot {
            entity: "Protocol".to_string(),
            id: "0xabc".to_string(),
            data: [
                ("id".to_string(), StoreValue::Bytes("0xabc".to_string())),
                (
                    "name".to_string(),
                    StoreValue::String("old-alpha".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        };

        StoreSnapshot {
            version: 1,
            manifest: "subgraph.yaml".to_string(),
            checkpoint: SyncCheckpoint {
                from_block: Some(1),
                to_block: 42,
                block_hash: Some("0xblock".to_string()),
                scanned_logs: 2,
                executed_logs: 2,
                validation_errors: 0,
                complete: true,
            },
            schema,
            entities: vec![
                EntitySnapshot {
                    entity: "Protocol".to_string(),
                    id: "0xabc".to_string(),
                    data: first,
                },
                EntitySnapshot {
                    entity: "Protocol".to_string(),
                    id: "0xbbb".to_string(),
                    data: second,
                },
                EntitySnapshot {
                    entity: "User".to_string(),
                    id: "0x111".to_string(),
                    data: user,
                },
                EntitySnapshot {
                    entity: "Purchase".to_string(),
                    id: "0xaaa1".to_string(),
                    data: purchase_one,
                },
                EntitySnapshot {
                    entity: "Purchase".to_string(),
                    id: "0xaaa2".to_string(),
                    data: purchase_two,
                },
            ],
            dynamic_sources: Vec::new(),
            processed_logs: Vec::new(),
            history: vec![HistoricalSnapshot {
                checkpoint: SyncCheckpoint {
                    from_block: Some(1),
                    to_block: 20,
                    block_hash: Some("0xold".to_string()),
                    scanned_logs: 1,
                    executed_logs: 1,
                    validation_errors: 0,
                    complete: true,
                },
                entities: vec![historical_protocol],
                dynamic_sources: Vec::new(),
            }],
        }
    }

    fn field(name: &str, kind: &str) -> EntityField {
        relation(name, kind, false, false, None)
    }

    fn relation(
        name: &str,
        kind: &str,
        list: bool,
        derived: bool,
        derived_from: Option<&str>,
    ) -> EntityField {
        EntityField {
            name: name.to_string(),
            kind: kind.to_string(),
            list,
            required: false,
            derived,
            derived_from: derived_from.map(str::to_string),
        }
    }
}
