use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Manifest, ManifestError};

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error("failed to read schema {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EntitySchema {
    pub entities: BTreeMap<String, EntityType>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EntityType {
    pub name: String,
    pub fields: BTreeMap<String, EntityField>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EntityField {
    pub name: String,
    pub kind: String,
    pub list: bool,
    pub required: bool,
    pub derived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_from: Option<String>,
}

impl EntitySchema {
    pub fn load_for_manifest(manifest_path: impl AsRef<Path>) -> Result<Self, SchemaError> {
        let manifest_path = manifest_path.as_ref();
        let manifest = Manifest::load(manifest_path)?;
        let base = manifest_path.parent().unwrap_or_else(|| Path::new("."));
        let schema_path = base.join(&manifest.schema.file);
        let raw = fs::read_to_string(&schema_path).map_err(|source| SchemaError::Read {
            path: schema_path,
            source,
        })?;
        Ok(parse_entity_schema(&raw))
    }
}

pub fn parse_entity_schema(raw: &str) -> EntitySchema {
    let mut schema = EntitySchema::default();
    let mut current: Option<EntityType> = None;
    let mut last_field_name: Option<String> = None;

    for line in raw.lines().map(strip_comment).map(str::trim) {
        if line.is_empty() {
            continue;
        }
        if current.is_none() {
            if let Some(name) = entity_type_name(line) {
                current = Some(EntityType {
                    name: name.to_string(),
                    fields: BTreeMap::new(),
                });
                last_field_name = None;
            }
            continue;
        }

        if line.starts_with('}') {
            let entity = current.take().expect("current entity exists");
            schema.entities.insert(entity.name.clone(), entity);
            last_field_name = None;
            continue;
        }

        if line.starts_with("@derivedFrom") {
            if let (Some(entity), Some(field_name)) = (current.as_mut(), last_field_name.as_ref()) {
                if let Some(field) = entity.fields.get_mut(field_name) {
                    field.derived = true;
                    field.derived_from = parse_derived_from(line);
                }
            }
            continue;
        }

        if let Some(field) = parse_field(line) {
            if let Some(entity) = current.as_mut() {
                last_field_name = Some(field.name.clone());
                entity.fields.insert(field.name.clone(), field);
            }
        }
    }

    schema
}

fn entity_type_name(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("type ")?;
    if !rest.contains("@entity") {
        return None;
    }
    rest.split_whitespace().next()
}

fn parse_field(line: &str) -> Option<EntityField> {
    let (name, rest) = line.split_once(':')?;
    let name = name.trim();
    if name.is_empty() || name.starts_with('@') {
        return None;
    }
    let derived = rest.contains("@derivedFrom");
    let raw_kind = rest.split_whitespace().next().unwrap_or_default().trim();
    if raw_kind.is_empty() {
        return None;
    }
    let required = raw_kind.ends_with('!');
    let mut kind = raw_kind.trim_end_matches('!');
    let list = kind.starts_with('[') && kind.ends_with(']');
    if list {
        kind = kind
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim_end_matches('!');
    }
    Some(EntityField {
        name: name.to_string(),
        kind: kind.to_string(),
        list,
        required,
        derived,
        derived_from: parse_derived_from(rest),
    })
}

fn parse_derived_from(rest: &str) -> Option<String> {
    let directive = rest.split_once("@derivedFrom")?.1;
    let field_index = directive.find("field")?;
    let after_field = &directive[field_index + "field".len()..];
    let (_, after_colon) = after_field.split_once(':')?;
    let after_colon = after_colon.trim_start();
    if let Some(after_quote) = after_colon.strip_prefix('"') {
        return after_quote
            .split_once('"')
            .map(|(value, _)| value.to_string());
    }
    after_colon
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn strip_comment(line: &str) -> &str {
    line.split_once('#').map(|(line, _)| line).unwrap_or(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_growfi_schema_entities() -> Result<(), SchemaError> {
        let schema = EntitySchema::load_for_manifest("../../examples/growfi/subgraph.yaml")?;
        let protocol = schema.entities.get("Protocol").expect("Protocol entity");
        assert_eq!(protocol.fields["id"].kind, "Bytes");
        assert!(protocol.fields["id"].required);
        assert_eq!(protocol.fields["growTreasury"].kind, "Bytes");

        let campaign = schema.entities.get("Campaign").expect("Campaign entity");
        assert!(campaign.fields["acceptedTokens"].derived);
        assert_eq!(
            campaign.fields["acceptedTokens"].derived_from.as_deref(),
            Some("campaign")
        );
        assert!(campaign.fields["acceptedTokens"].list);
        Ok(())
    }

    #[test]
    fn parses_multiline_derived_from_directives() {
        let schema = parse_entity_schema(
            r#"
            type Pool @entity {
              id: ID!
              isolationModeTotalDebtUpdatedHistory: [IsolationModeTotalDebtUpdated!]!
                @derivedFrom(field: "pool")
            }
            "#,
        );

        let field = &schema.entities["Pool"].fields["isolationModeTotalDebtUpdatedHistory"];
        assert!(field.derived);
        assert_eq!(field.derived_from.as_deref(), Some("pool"));
    }
}
