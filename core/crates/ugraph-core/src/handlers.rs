use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use serde::Serialize;
use thiserror::Error;

use crate::{inspect_wasm_file, ImportKind, Manifest, ManifestError, WasmInspectError};

#[derive(Debug, Error)]
pub enum HandlerExportError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Wasm(#[from] WasmInspectError),
}

#[derive(Clone, Debug, Serialize)]
pub struct HandlerExportReport {
    pub ok: bool,
    pub handlers: Vec<HandlerExportCheck>,
    pub missing_handlers: Vec<HandlerExportCheck>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HandlerExportCheck {
    pub data_source: String,
    pub template: bool,
    pub wasm_path: PathBuf,
    pub handler: String,
    pub exported: bool,
}

pub fn check_handler_exports(
    manifest_path: impl AsRef<Path>,
    build_dir: impl AsRef<Path>,
) -> Result<HandlerExportReport, HandlerExportError> {
    let manifest_path = manifest_path.as_ref();
    let build_dir = build_dir.as_ref();
    let manifest = Manifest::load(manifest_path)?;
    manifest.validate_files(manifest_path)?;

    let build_manifest = Manifest::load(build_dir.join("subgraph.yaml")).ok();
    let mut handlers = Vec::new();
    for source in &manifest.data_sources {
        let wasm_path = compiled_wasm_path(
            build_dir,
            build_manifest
                .as_ref()
                .and_then(|manifest| {
                    manifest
                        .data_sources
                        .iter()
                        .find(|item| item.name == source.name)
                })
                .unwrap_or(source),
            false,
        );
        handlers.extend(check_source_handlers(
            &source.name,
            false,
            wasm_path,
            source.mapping.handler_names().into_iter(),
        )?);
    }
    for template in &manifest.templates {
        let wasm_path = compiled_wasm_path(
            build_dir,
            build_manifest
                .as_ref()
                .and_then(|manifest| {
                    manifest
                        .templates
                        .iter()
                        .find(|item| item.name == template.name)
                })
                .unwrap_or(template),
            true,
        );
        handlers.extend(check_source_handlers(
            &template.name,
            true,
            wasm_path,
            template.mapping.handler_names().into_iter(),
        )?);
    }

    let missing_handlers = handlers
        .iter()
        .filter(|handler| !handler.exported)
        .cloned()
        .collect::<Vec<_>>();

    Ok(HandlerExportReport {
        ok: missing_handlers.is_empty(),
        handlers,
        missing_handlers,
    })
}

fn check_source_handlers<'a>(
    data_source: &str,
    template: bool,
    wasm_path: PathBuf,
    handlers: impl Iterator<Item = &'a str>,
) -> Result<Vec<HandlerExportCheck>, WasmInspectError> {
    let inspection = inspect_wasm_file(&wasm_path)?;
    let exported_functions = inspection
        .exports
        .iter()
        .filter(|export| matches!(export.kind, ImportKind::Func))
        .map(|export| export.name.as_str())
        .collect::<BTreeSet<_>>();

    Ok(handlers
        .map(|handler| HandlerExportCheck {
            data_source: data_source.to_string(),
            template,
            wasm_path: wasm_path.clone(),
            handler: handler.to_string(),
            exported: exported_functions.contains(handler),
        })
        .collect())
}

fn compiled_wasm_path(build_dir: &Path, source: &crate::DataSource, template: bool) -> PathBuf {
    if source.mapping.file.ends_with(".wasm") {
        return build_dir.join(&source.mapping.file);
    }
    if template {
        build_dir
            .join("templates")
            .join(&source.name)
            .join(format!("{}.wasm", source.name))
    } else {
        build_dir
            .join(&source.name)
            .join(format!("{}.wasm", source.name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growfi_manifest_handlers_are_exported_by_build_wasm() -> Result<(), HandlerExportError> {
        let report = check_handler_exports(
            "../../examples/growfi/subgraph.yaml",
            "../../examples/growfi/build",
        )?;
        assert!(report.ok, "missing handlers: {:?}", report.missing_handlers);
        assert_eq!(report.handlers.len(), 60);
        Ok(())
    }
}
