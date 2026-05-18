use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;
use thiserror::Error;
use wasmparser::{ExternalKind, Parser, Payload, TypeRef};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub enum ImportKind {
    Func,
    Table,
    Memory,
    Global,
    Tag,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct WasmImport {
    pub module: String,
    pub name: String,
    pub kind: ImportKind,
}

impl WasmImport {
    pub fn graph_host_name(&self) -> String {
        if self.name.contains('.') || self.module.is_empty() || self.module == "env" {
            self.name.clone()
        } else {
            format!("{}.{}", self.module, self.name)
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WasmInspection {
    pub path: PathBuf,
    pub imports: Vec<WasmImport>,
    pub exports: Vec<WasmExport>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct WasmExport {
    pub name: String,
    pub kind: ImportKind,
}

#[derive(Clone, Debug, Serialize)]
pub struct WasmTreeInspection {
    pub files: Vec<WasmInspection>,
    pub required_imports: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CompatReport {
    pub ok: bool,
    pub required_imports: Vec<String>,
    pub missing_host_exports: Vec<String>,
    pub files: Vec<WasmInspection>,
}

#[derive(Debug, Error)]
pub enum WasmInspectError {
    #[error("failed to read wasm file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse wasm file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: wasmparser::BinaryReaderError,
    },
    #[error("failed to scan directory {path}: {source}")]
    Scan {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn inspect_wasm_file(path: impl AsRef<Path>) -> Result<WasmInspection, WasmInspectError> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|source| WasmInspectError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let imports = inspect_wasm_imports(&bytes).map_err(|source| WasmInspectError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    let exports = inspect_wasm_exports(&bytes).map_err(|source| WasmInspectError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(WasmInspection {
        path: path.to_path_buf(),
        imports,
        exports,
    })
}

pub fn inspect_wasm_imports(
    bytes: &[u8],
) -> Result<Vec<WasmImport>, wasmparser::BinaryReaderError> {
    let mut imports = Vec::new();
    for payload in Parser::new(0).parse_all(bytes) {
        if let Payload::ImportSection(section) = payload? {
            for import in section {
                let import = import?;
                imports.push(WasmImport {
                    module: import.module.to_string(),
                    name: import.name.to_string(),
                    kind: import_kind(&import.ty),
                });
            }
        }
    }
    imports.sort();
    imports.dedup();
    Ok(imports)
}

pub fn inspect_wasm_exports(
    bytes: &[u8],
) -> Result<Vec<WasmExport>, wasmparser::BinaryReaderError> {
    let mut exports = Vec::new();
    for payload in Parser::new(0).parse_all(bytes) {
        if let Payload::ExportSection(section) = payload? {
            for export in section {
                let export = export?;
                exports.push(WasmExport {
                    name: export.name.to_string(),
                    kind: export_kind(export.kind),
                });
            }
        }
    }
    exports.sort();
    exports.dedup();
    Ok(exports)
}

pub fn inspect_wasm_tree(root: impl AsRef<Path>) -> Result<WasmTreeInspection, WasmInspectError> {
    let mut files = Vec::new();
    collect_wasm_files(root.as_ref(), &mut files)?;
    files.sort();

    let inspections = files
        .into_iter()
        .map(inspect_wasm_file)
        .collect::<Result<Vec<_>, _>>()?;
    let required_imports = required_import_names(&inspections);

    Ok(WasmTreeInspection {
        files: inspections,
        required_imports,
    })
}

pub fn required_import_names(inspections: &[WasmInspection]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for inspection in inspections {
        for import in &inspection.imports {
            if matches!(import.kind, ImportKind::Func) {
                names.insert(import.graph_host_name());
            }
        }
    }
    names.into_iter().collect()
}

pub fn compatibility_report(root: impl AsRef<Path>) -> Result<CompatReport, WasmInspectError> {
    let tree = inspect_wasm_tree(root)?;
    let known = known_graph_node_host_exports();
    let missing_host_exports = tree
        .required_imports
        .iter()
        .filter(|name| !known.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    Ok(CompatReport {
        ok: missing_host_exports.is_empty(),
        required_imports: tree.required_imports,
        missing_host_exports,
        files: tree.files,
    })
}

pub fn known_graph_node_host_exports() -> BTreeSet<String> {
    [
        "abort",
        "arweave.transactionData",
        "bigDecimal.dividedBy",
        "bigDecimal.equals",
        "bigDecimal.fromString",
        "bigDecimal.minus",
        "bigDecimal.plus",
        "bigDecimal.times",
        "bigDecimal.toString",
        "bigInt.bitAnd",
        "bigInt.bitOr",
        "bigInt.dividedBy",
        "bigInt.dividedByDecimal",
        "bigInt.fromString",
        "bigInt.leftShift",
        "bigInt.minus",
        "bigInt.mod",
        "bigInt.plus",
        "bigInt.pow",
        "bigInt.rightShift",
        "bigInt.times",
        "box.profile",
        "crypto.keccak256",
        "dataSource.address",
        "dataSource.context",
        "dataSource.create",
        "dataSource.createWithContext",
        "dataSource.network",
        "ens.nameByHash",
        "ethereum.call",
        "ethereum.decode",
        "ethereum.encode",
        "ethereum.getBalance",
        "ethereum.hasCode",
        "gas",
        "ipfs.cat",
        "ipfs.getBlock",
        "ipfs.map",
        "json.fromBytes",
        "json.toBigInt",
        "json.toF64",
        "json.toI64",
        "json.toU64",
        "json.try_fromBytes",
        "log.log",
        "store.get",
        "store.get_in_block",
        "store.loadRelated",
        "store.remove",
        "store.set",
        "typeConversion.bigIntToHex",
        "typeConversion.bigIntToString",
        "typeConversion.bytesToBase58",
        "typeConversion.bytesToHex",
        "typeConversion.bytesToString",
        "typeConversion.stringToH160",
        "yaml.fromBytes",
        "yaml.try_fromBytes",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

fn import_kind(ty: &TypeRef) -> ImportKind {
    match ty {
        TypeRef::Func(_) => ImportKind::Func,
        TypeRef::Table(_) => ImportKind::Table,
        TypeRef::Memory(_) => ImportKind::Memory,
        TypeRef::Global(_) => ImportKind::Global,
        TypeRef::Tag(_) => ImportKind::Tag,
    }
}

fn export_kind(kind: ExternalKind) -> ImportKind {
    match kind {
        ExternalKind::Func => ImportKind::Func,
        ExternalKind::Table => ImportKind::Table,
        ExternalKind::Memory => ImportKind::Memory,
        ExternalKind::Global => ImportKind::Global,
        ExternalKind::Tag => ImportKind::Tag,
    }
}

fn collect_wasm_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), WasmInspectError> {
    let entries = fs::read_dir(dir).map_err(|source| WasmInspectError::Scan {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| WasmInspectError::Scan {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_wasm_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "wasm") {
            out.push(path);
        }
    }
    Ok(())
}

pub fn imports_by_file(inspections: &[WasmInspection]) -> BTreeMap<String, Vec<String>> {
    inspections
        .iter()
        .map(|inspection| {
            (
                inspection.path.display().to_string(),
                inspection
                    .imports
                    .iter()
                    .filter(|import| matches!(import.kind, ImportKind::Func))
                    .map(WasmImport::graph_host_name)
                    .collect(),
            )
        })
        .collect()
}

pub fn exports_by_file(inspections: &[WasmInspection]) -> BTreeMap<String, Vec<String>> {
    inspections
        .iter()
        .map(|inspection| {
            (
                inspection.path.display().to_string(),
                inspection
                    .exports
                    .iter()
                    .filter(|export| matches!(export.kind, ImportKind::Func))
                    .map(|export| export.name.clone())
                    .collect(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growfi_build_imports_are_known_graph_node_exports() -> Result<(), WasmInspectError> {
        let report = compatibility_report("../../examples/growfi/build")?;
        assert!(
            report.ok,
            "missing host exports: {:?}",
            report.missing_host_exports
        );
        assert!(!report.files.is_empty());
        assert!(report
            .required_imports
            .iter()
            .any(|name| name == "store.set"));
        assert!(report.files.iter().any(|file| {
            file.exports
                .iter()
                .any(|export| export.name == "handleCampaignCreated")
        }));
        Ok(())
    }
}
