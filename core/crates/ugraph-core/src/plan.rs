use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

use crate::{check_manifest_abi_events, AbiError, AbiEventInput, Manifest, ManifestError};

#[derive(Debug, Error)]
pub enum PlanError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Abi(#[from] AbiError),
    #[error("manifest ABI event check failed")]
    AbiEvents,
}

#[derive(Clone, Debug, Serialize)]
pub struct IndexingPlan {
    pub manifest: PathBuf,
    pub sources: Vec<SourcePlan>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourcePlan {
    pub name: String,
    pub template: bool,
    pub dynamic: bool,
    pub template_name: Option<String>,
    pub params: Vec<String>,
    pub kind: String,
    pub network: Option<String>,
    pub address: Option<String>,
    pub abi: Option<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub triggers: Vec<EventTriggerPlan>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventTriggerPlan {
    pub event: String,
    pub handler: String,
    pub signature: String,
    pub topic0: String,
    pub inputs: Vec<AbiEventInput>,
}

pub fn build_indexing_plan(manifest_path: impl AsRef<Path>) -> Result<IndexingPlan, PlanError> {
    let manifest_path = manifest_path.as_ref();
    let manifest = Manifest::load(manifest_path)?;
    manifest.validate_files(manifest_path)?;
    let abi_events = check_manifest_abi_events(manifest_path)?;
    if !abi_events.ok {
        return Err(PlanError::AbiEvents);
    }

    let mut sources = Vec::new();
    for source in &manifest.data_sources {
        sources.push(SourcePlan {
            name: source.name.clone(),
            template: false,
            dynamic: false,
            template_name: None,
            params: Vec::new(),
            kind: source.kind.clone(),
            network: source.network.clone(),
            address: source
                .source
                .as_ref()
                .and_then(|source| source.address.clone()),
            abi: source.source.as_ref().and_then(|source| source.abi.clone()),
            start_block: source.source.as_ref().and_then(|source| source.start_block),
            end_block: source.source.as_ref().and_then(|source| source.end_block),
            triggers: abi_events
                .events
                .iter()
                .filter(|event| !event.template && event.data_source == source.name)
                .map(|event| EventTriggerPlan {
                    event: event.event.clone(),
                    handler: event.handler.clone(),
                    signature: event.normalized_signature.clone(),
                    topic0: event
                        .topic0
                        .clone()
                        .expect("checked ABI events have topic0"),
                    inputs: event.inputs.clone(),
                })
                .collect(),
        });
    }
    for template in &manifest.templates {
        sources.push(SourcePlan {
            name: template.name.clone(),
            template: true,
            dynamic: false,
            template_name: None,
            params: Vec::new(),
            kind: template.kind.clone(),
            network: template.network.clone(),
            address: None,
            abi: template
                .source
                .as_ref()
                .and_then(|source| source.abi.clone()),
            start_block: None,
            end_block: template.source.as_ref().and_then(|source| source.end_block),
            triggers: abi_events
                .events
                .iter()
                .filter(|event| event.template && event.data_source == template.name)
                .map(|event| EventTriggerPlan {
                    event: event.event.clone(),
                    handler: event.handler.clone(),
                    signature: event.normalized_signature.clone(),
                    topic0: event
                        .topic0
                        .clone()
                        .expect("checked ABI events have topic0"),
                    inputs: event.inputs.clone(),
                })
                .collect(),
        });
    }

    Ok(IndexingPlan {
        manifest: manifest_path.to_path_buf(),
        sources,
    })
}

pub fn instantiate_dynamic_source(
    plan: &IndexingPlan,
    template_name: &str,
    params: &[String],
    creation_block: u64,
) -> Option<SourcePlan> {
    let template = plan
        .sources
        .iter()
        .find(|source| source.template && source.name == template_name)?;
    let address = params.first()?.clone();
    Some(SourcePlan {
        name: template.name.clone(),
        template: false,
        dynamic: true,
        template_name: Some(template.name.clone()),
        params: params.to_vec(),
        kind: template.kind.clone(),
        network: template.network.clone(),
        address: Some(address),
        abi: template.abi.clone(),
        start_block: Some(creation_block),
        end_block: template.end_block,
        triggers: template.triggers.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_growfi_indexing_plan() -> Result<(), PlanError> {
        let plan = build_indexing_plan("../../examples/growfi/subgraph.yaml")?;
        assert_eq!(plan.sources.len(), 10);
        assert_eq!(
            plan.sources
                .iter()
                .map(|source| source.triggers.len())
                .sum::<usize>(),
            85
        );
        assert!(plan.sources.iter().any(|source| {
            source.name == "CampaignFactory"
                && source.address.as_deref() == Some("0xa4DEd8Ab35e89bCAF1f7DFeb7aB2c1ED533b3f05")
        }));
        Ok(())
    }

    #[test]
    fn instantiates_growfi_template_source_from_create_params() -> Result<(), PlanError> {
        let plan = build_indexing_plan("../../examples/growfi/subgraph.yaml")?;
        let source = instantiate_dynamic_source(
            &plan,
            "Campaign",
            &["0x0000000000000000000000000000000000001001".to_string()],
            10845481,
        )
        .expect("Campaign template exists");

        assert_eq!(source.name, "Campaign");
        assert!(!source.template);
        assert!(source.dynamic);
        assert_eq!(source.template_name.as_deref(), Some("Campaign"));
        assert_eq!(
            source.address.as_deref(),
            Some("0x0000000000000000000000000000000000001001")
        );
        assert_eq!(source.start_block, Some(10845481));
        assert_eq!(source.triggers.len(), 39);
        Ok(())
    }
}
