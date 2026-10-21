//! The layer test (ADR-0002, QUALITY.md): reads `cargo metadata` for the workspace and fails
//! on a dependency that points outward, between two adapters, from layer 1 to a heavy crate,
//! or from a crate no layer lists. A new crate is placed in LAYERS before it builds green.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

struct Layer {
    name: &'static str,
    crates: &'static [&'static str],
    may_depend_on: &'static [&'static str],
    /// Crates of the layer never depend on each other.
    isolated: bool,
    /// External crates the layer must not depend on: a name, or a prefix ending in '-'.
    banned: &'static [&'static str],
}

const HEAVY: &[&str] = &[
    "async-nats",
    "aws-",
    "axum",
    "clickhouse",
    "duckdb",
    "google-cloud-",
    "hyper",
    "lapin",
    "mongodb",
    "object_store",
    "ort",
    "polars",
    "postgres",
    "rdkafka",
    "reqwest",
    "tokio",
    "tokio-postgres",
];

const LAYERS: &[Layer] = &[
    Layer {
        name: "domain",
        crates: &["mage-service-core", "mage-stream-core", "mage-stream-ports"],
        may_depend_on: &["domain"],
        isolated: false,
        banned: HEAVY,
    },
    Layer {
        name: "support",
        crates: &["mage-log", "mage-pg"],
        may_depend_on: &["domain"],
        isolated: true,
        banned: &[],
    },
    Layer {
        name: "adapters",
        crates: &[
            "mage-checks",
            "mage-connector-activemq",
            "mage-connector-clickhouse",
            "mage-connector-http",
            "mage-connector-ibmmq",
            "mage-connector-influxdb",
            "mage-connector-kafka",
            "mage-connector-local",
            "mage-connector-mongodb",
            "mage-connector-nats",
            "mage-connector-objectstore",
            "mage-connector-postgres",
            "mage-connector-pubsub",
            "mage-connector-rabbitmq",
            "mage-connector-search",
            "mage-connector-sqs",
            "mage-ledger",
        ],
        may_depend_on: &["domain", "support"],
        isolated: true,
        banned: &[],
    },
    Layer {
        name: "binaries",
        crates: &["mage-console", "mage-service"],
        may_depend_on: &["domain", "support", "adapters"],
        isolated: true,
        banned: &[],
    },
];

/// Package name to its normal and build dependencies (dev dependencies are left out: a test
/// may use anything).
type Graph = BTreeMap<String, BTreeSet<String>>;

fn layer_of(name: &str) -> Option<&'static Layer> {
    LAYERS.iter().find(|layer| layer.crates.contains(&name))
}

fn problems(graph: &Graph) -> Vec<String> {
    let mut found = Vec::new();
    for (name, dependencies) in graph {
        let Some(layer) = layer_of(name) else {
            found.push(format!(
                "{name} is in no layer: add it to LAYERS in tests/layers.rs"
            ));
            continue;
        };
        for dependency in dependencies {
            if graph.contains_key(dependency) {
                let Some(target) = layer_of(dependency) else {
                    continue;
                };
                if target.name == layer.name && layer.isolated {
                    found.push(format!(
                        "{name} depends on {dependency}: crates of {} do not depend on each other",
                        layer.name
                    ));
                } else if target.name != layer.name && !layer.may_depend_on.contains(&target.name) {
                    found.push(format!(
                        "{name} ({}) depends on {dependency} ({}): {} may depend on {}",
                        layer.name,
                        target.name,
                        layer.name,
                        layer.may_depend_on.join(", ")
                    ));
                }
            } else if layer.banned.iter().any(|banned| {
                dependency == banned || (banned.ends_with('-') && dependency.starts_with(banned))
            }) {
                found.push(format!(
                    "{name} ({}) depends on {dependency}: {} keeps out heavy crates",
                    layer.name, layer.name
                ));
            }
        }
    }
    found
}

fn workspace_graph() -> Graph {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml");
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
        ])
        .arg("--manifest-path")
        .arg(&manifest)
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata prints JSON");
    let mut graph = Graph::new();
    for package in metadata["packages"].as_array().expect("packages") {
        let name = package["name"].as_str().expect("name").to_string();
        let dependencies = package["dependencies"]
            .as_array()
            .expect("dependencies")
            .iter()
            .filter(|dependency| dependency["kind"].as_str() != Some("dev"))
            .map(|dependency| dependency["name"].as_str().expect("name").to_string())
            .collect();
        graph.insert(name, dependencies);
    }
    graph
}

#[test]
fn the_workspace_keeps_its_layers() {
    let graph = workspace_graph();
    assert!(graph.contains_key("mage-service"), "the workspace was read");
    let found = problems(&graph);
    assert!(
        found.is_empty(),
        "layer rules broken:\n{}",
        found.join("\n")
    );
}

#[test]
fn every_listed_crate_exists() {
    let graph = workspace_graph();
    let missing: Vec<_> = LAYERS
        .iter()
        .flat_map(|layer| layer.crates.iter())
        .filter(|name| !graph.contains_key(**name))
        .collect();
    assert!(
        missing.is_empty(),
        "LAYERS lists crates the workspace has not: {missing:?}"
    );
}

#[test]
fn the_rules_catch_each_kind_of_edge() {
    let graph = |edges: &[(&str, &[&str])]| -> Graph {
        let mut graph = Graph::new();
        for (name, dependencies) in edges {
            graph.insert(
                name.to_string(),
                dependencies.iter().map(|d| d.to_string()).collect(),
            );
            for dependency in *dependencies {
                if dependency.starts_with("mage-") {
                    graph.entry(dependency.to_string()).or_default();
                }
            }
        }
        graph
    };
    let outward = problems(&graph(&[("mage-stream-core", &["mage-connector-kafka"])]));
    assert_eq!(outward.len(), 1);
    assert!(
        outward[0].contains("domain may depend on domain"),
        "{outward:?}"
    );

    let sideways = problems(&graph(&[(
        "mage-connector-kafka",
        &["mage-connector-local"],
    )]));
    assert!(
        sideways[0].contains("do not depend on each other"),
        "{sideways:?}"
    );

    let heavy = problems(&graph(&[(
        "mage-stream-ports",
        &["tokio", "aws-sdk-s3", "serde", "ordered-float"],
    )]));
    assert_eq!(heavy.len(), 2, "{heavy:?}");
    assert!(
        heavy[0].contains("aws-sdk-s3") && heavy[1].contains("tokio"),
        "{heavy:?}"
    );

    let unknown = problems(&graph(&[("mage-new-thing", &[])]));
    assert!(unknown[0].contains("is in no layer"), "{unknown:?}");

    assert!(
        problems(&graph(&[(
            "mage-service",
            &["mage-connector-kafka", "tokio"]
        )]))
        .is_empty()
    );
}
