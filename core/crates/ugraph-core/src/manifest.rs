use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_yaml::Value as YamlValue;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("failed to read manifest {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse manifest {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("schema file does not exist: {0}")]
    MissingSchema(PathBuf),
    #[error("mapping file does not exist: {0}")]
    MissingMapping(PathBuf),
    #[error("ABI file does not exist: {0}")]
    MissingAbi(PathBuf),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub spec_version: String,
    pub description: Option<String>,
    pub repository: Option<String>,
    #[serde(default)]
    pub features: Vec<String>,
    pub indexer_hints: Option<YamlValue>,
    pub graft: Option<YamlValue>,
    pub schema: SchemaRef,
    #[serde(default)]
    pub data_sources: Vec<DataSource>,
    #[serde(default)]
    pub templates: Vec<DataSource>,
}

impl Manifest {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        serde_yaml::from_str(&raw).map_err(|source| ManifestError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    pub fn validate_files(&self, manifest_path: impl AsRef<Path>) -> Result<(), ManifestError> {
        let base = manifest_path
            .as_ref()
            .parent()
            .unwrap_or_else(|| Path::new("."));

        let schema = base.join(&self.schema.file);
        if !schema.exists() {
            return Err(ManifestError::MissingSchema(schema));
        }

        for source in self.data_sources.iter().chain(self.templates.iter()) {
            let mapping = base.join(&source.mapping.file);
            if !mapping.exists() {
                return Err(ManifestError::MissingMapping(mapping));
            }
            for abi in &source.mapping.abis {
                let path = base.join(&abi.file);
                if !path.exists() {
                    return Err(ManifestError::MissingAbi(path));
                }
            }
        }

        Ok(())
    }

    pub fn static_source_count(&self) -> usize {
        self.data_sources.len()
    }

    pub fn template_count(&self) -> usize {
        self.templates.len()
    }

    pub fn event_handler_count(&self) -> usize {
        self.data_sources
            .iter()
            .chain(self.templates.iter())
            .map(|source| source.mapping.event_handlers.len())
            .sum()
    }

    pub fn handler_count(&self) -> usize {
        self.data_sources
            .iter()
            .chain(self.templates.iter())
            .map(|source| source.mapping.handler_count())
            .sum()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SchemaRef {
    pub file: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DataSource {
    pub kind: String,
    pub name: String,
    pub network: Option<String>,
    pub source: Option<Source>,
    pub context: Option<YamlValue>,
    pub mapping: Mapping,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub address: Option<String>,
    pub abi: Option<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub file: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Mapping {
    pub kind: String,
    pub api_version: Option<String>,
    pub language: String,
    pub file: String,
    #[serde(default)]
    pub entities: Vec<String>,
    #[serde(default)]
    pub abis: Vec<AbiRef>,
    #[serde(default)]
    pub event_handlers: Vec<EventHandler>,
    #[serde(default)]
    pub call_handlers: Vec<CallHandler>,
    #[serde(default)]
    pub block_handlers: Vec<BlockHandler>,
}

impl Mapping {
    pub fn handler_count(&self) -> usize {
        self.event_handlers.len() + self.call_handlers.len() + self.block_handlers.len()
    }

    pub fn handler_names(&self) -> Vec<&str> {
        let mut names = Vec::with_capacity(self.handler_count());
        names.extend(
            self.event_handlers
                .iter()
                .map(|handler| handler.handler.as_str()),
        );
        names.extend(
            self.call_handlers
                .iter()
                .map(|handler| handler.handler.as_str()),
        );
        names.extend(
            self.block_handlers
                .iter()
                .map(|handler| handler.handler.as_str()),
        );
        names
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AbiRef {
    pub name: String,
    pub file: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventHandler {
    pub event: String,
    pub handler: String,
    pub receipt: Option<bool>,
    pub topic0: Option<String>,
    pub calls: Option<YamlValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CallHandler {
    pub function: String,
    pub handler: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlockHandler {
    pub handler: String,
    pub filter: Option<YamlValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_standard_manifest_shape() -> anyhow::Result<()> {
        let path = Path::new("../../examples/growfi/subgraph.yaml");
        let manifest = Manifest::load(path)?;

        assert_eq!(manifest.spec_version, "1.0.0");
        assert!(manifest.static_source_count() > 0);
        assert!(manifest.template_count() > 0);
        assert!(manifest.event_handler_count() > 0);
        assert_eq!(manifest.handler_count(), manifest.event_handler_count());

        Ok(())
    }

    #[test]
    fn parses_modern_graph_manifest_handlers_and_hints() -> anyhow::Result<()> {
        let manifest: Manifest = serde_yaml::from_str(
            r#"
specVersion: 1.3.0
description: Modern manifest fixture
features:
  - grafting
indexerHints:
  prune: auto
graft:
  base: QmBase
  block: 100
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: Gravity
    network: sepolia
    source:
      address: "0x0000000000000000000000000000000000000001"
      abi: Gravity
      startBlock: 1
      endBlock: 2
    context:
      enabled:
        type: Bool
        data: true
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.9
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - Gravatar
      abis:
        - name: Gravity
          file: ./abis/Gravity.json
      eventHandlers:
        - event: NewGravatar(uint256,address,string,string)
          handler: handleNewGravatar
          receipt: true
          calls:
            owner: Gravity[event.address].owner()
      callHandlers:
        - function: createGravatar(string,string)
          handler: handleCreateGravatar
      blockHandlers:
        - handler: handleBlock
        - handler: handleBlockWithCall
          filter:
            kind: call
"#,
        )?;

        assert_eq!(manifest.spec_version, "1.3.0");
        assert_eq!(manifest.features, ["grafting"]);
        assert!(manifest.indexer_hints.is_some());
        assert!(manifest.graft.is_some());
        assert_eq!(manifest.event_handler_count(), 1);
        assert_eq!(manifest.handler_count(), 4);
        assert_eq!(
            manifest.data_sources[0].mapping.handler_names(),
            [
                "handleNewGravatar",
                "handleCreateGravatar",
                "handleBlock",
                "handleBlockWithCall"
            ]
        );

        Ok(())
    }
}
