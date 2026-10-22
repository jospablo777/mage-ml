//! mage-service: runs pipelines exported from a Mage project, without Mage.

use mage_ledger as ledger;
use mage_log as log;

mod alerts;
mod api;
mod audit;
mod aws;
mod azure;
mod contract;
mod environment;
mod executor;
mod gcp;
mod ibm;
mod jobs;
mod keyed_state;
mod monitor;
mod python;
mod resident;
mod run_logs;
mod scheduler;
mod secrets;
mod service;
mod shadow;
mod state_files;
mod stream;
mod stream_chain;
mod stream_python;
mod stream_rust;
mod stream_time;
mod stream_window;
mod streams;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use mage_service_core::manifest::{Language, MANIFEST_FILE, Manifest, TriggerKind};
use mage_service_core::protocol::{BlockStatus, RunStatus};
use serde_json::{Map, Value};

use crate::ledger::Ledger;
use crate::python::{PythonConfig, PythonPool};
use crate::service::{Config, Service};

#[derive(Parser)]
#[command(
    name = "mage-service",
    version,
    about = "Runs pipelines exported from Mage: an HTTP API, schedules, run history and logs.",
    after_help = "Settings also come from MAGE_SERVICE_* environment variables; see `--help` of each command."
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    common: Common,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the HTTP API and run the schedules (the default).
    Serve(ServeArgs),
    /// Run one pipeline once and exit: 0 when it completes, 1 when it fails.
    Run(RunArgs),
    /// Check the manifest and the exported files, then exit.
    Validate,
    /// Show the pipelines, blocks and triggers of this service.
    Info,
    /// List the environment variables the blocks read, and whether each is set.
    Env,
    /// Warm Python workers for the jobs of MageML's scheduler (requests on stdin, events
    /// on stdout); MageML starts it, see docs/development/run-engine.md.
    Jobs(jobs::JobsArgs),
    /// Check that each stream's source and sinks answer, read-only, then exit: 0 when none
    /// failed.
    CheckConnections {
        /// Only this pipeline's stream.
        pipeline: Option<String>,
        /// Print JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Migrate the run history, or copy a SQLite one into PostgreSQL, then exit.
    Ledger {
        #[command(subcommand)]
        command: LedgerCommand,
    },
    /// Exit 0 when the local service answers /healthz; for Docker's HEALTHCHECK.
    Health {
        /// MAGE_SERVICE_PORT, else the platform's PORT, else 8080.
        #[arg(long, env = "MAGE_SERVICE_PORT")]
        port: Option<u16>,
    },
}

#[derive(Subcommand)]
enum LedgerCommand {
    /// Bring the run history to this engine's schema version.
    Migrate,
    /// Copy a SQLite run history (`runs.db`) into the PostgreSQL one; runs it has are kept.
    Import {
        /// The `runs.db` to copy.
        path: PathBuf,
    },
}

#[derive(Args, Clone)]
struct Common {
    /// The exported service: service.json, the project and the binaries.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_DIR",
        default_value = "/srv/mage-service"
    )]
    dir: PathBuf,
    /// Run history and run outputs.
    #[arg(long, global = true, env = "MAGE_SERVICE_DATA")]
    data: Option<PathBuf>,
    /// The Python that runs Python blocks.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_PYTHON",
        default_value = "python3"
    )]
    python: String,
    /// The Python block worker script.
    #[arg(long, global = true, env = "MAGE_SERVICE_WORKER")]
    worker: Option<PathBuf>,
    /// Python worker processes, which bounds Python blocks running at once.
    #[arg(long, global = true, env = "MAGE_SERVICE_PYTHON_WORKERS")]
    python_workers: Option<usize>,
    /// Blocks a Python worker runs before it is replaced.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_PYTHON_MAX_BLOCKS",
        default_value_t = 200
    )]
    python_max_blocks: u32,
    /// Memory in MB above which a Python worker serving streams is replaced; 0 replaces it
    /// when its memory doubled since its first stream batch plus 256 MB.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_PYTHON_MAX_RSS_MB",
        default_value_t = 0
    )]
    python_max_rss_mb: u64,
    /// Runs of all pipelines at the same time.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_MAX_RUNS",
        default_value_t = 8
    )]
    max_runs: usize,
    /// `env` passed to blocks.
    #[arg(long, global = true, env = "MAGE_SERVICE_ENV", default_value = "prod")]
    environment: String,
    #[arg(long, global = true, env = "MAGE_SERVICE_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
    /// A PostgreSQL run history (`postgres://...`), which instances on several machines
    /// share; without one, `runs.db` in the data directory.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_LEDGER_URL",
        hide_env_values = true
    )]
    ledger_url: Option<String>,
    /// The PostgreSQL schema of this service's run history; one service per schema.
    #[arg(
        long,
        global = true,
        env = "MAGE_SERVICE_LEDGER_SCHEMA",
        default_value = "mage_service"
    )]
    ledger_schema: String,
    /// `off` refuses to start on an older run history instead of migrating it; `mage-service
    /// ledger migrate` then migrates it.
    #[arg(long, global = true, env = "MAGE_SERVICE_LEDGER_MIGRATE", value_enum, default_value_t = Switch::On)]
    ledger_migrate: Switch,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, env = "MAGE_SERVICE_HOST", default_value = "127.0.0.1")]
    host: String,
    /// MAGE_SERVICE_PORT, else the PORT a platform sets (Code Engine, Cloud Run, Render),
    /// else 8080.
    #[arg(long, env = "MAGE_SERVICE_PORT")]
    port: Option<u16>,
    /// Run the active time triggers. Off by default, so a service exported next to a running
    /// Mage does not run the same schedule twice.
    #[arg(long, env = "MAGE_SERVICE_SCHEDULES", value_enum, default_value_t = Switch::Off)]
    schedules: Switch,
    /// The token for every API call. Without one, a service listening on loopback makes one
    /// and writes it to `service-token` in its data directory; any other address needs one.
    #[arg(long, env = "MAGE_SERVICE_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// A token that can only read runs, logs and the snapshot.
    #[arg(long, env = "MAGE_SERVICE_READ_TOKEN", hide_env_values = true)]
    read_token: Option<String>,
    /// A token that can only send messages to streams with an HTTP source, for webhook
    /// senders; they never hold the service token.
    #[arg(long, env = "MAGE_SERVICE_INGEST_TOKEN", hide_env_values = true)]
    ingest_token: Option<String>,
    /// `off` serves the API without tokens; only for a service no one else can reach.
    #[arg(long, env = "MAGE_SERVICE_AUTH", value_enum, default_value_t = Switch::On)]
    auth: Switch,
    /// Days finished runs and their outputs are kept; 0 keeps them forever.
    #[arg(long, env = "MAGE_SERVICE_RETENTION_DAYS", default_value_t = 30)]
    retention_days: u32,
    /// Seconds running runs get to finish after SIGTERM before they are cancelled.
    #[arg(long, env = "MAGE_SERVICE_SHUTDOWN_SECONDS", default_value_t = 60)]
    shutdown_seconds: u64,
}

/// Serve settings from the environment alone, for `mage-service` without a command.
#[derive(Parser)]
struct DefaultServe {
    #[command(flatten)]
    args: ServeArgs,
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
enum Switch {
    On,
    Off,
}

#[derive(Args)]
struct RunArgs {
    /// The pipeline to run.
    pipeline: String,
    /// A variable for the run's blocks, as key=value; the value is JSON when it parses.
    #[arg(long = "var", value_name = "KEY=VALUE")]
    variables: Vec<String>,
    /// Print the run as JSON instead of a table.
    #[arg(long)]
    json: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    log::init(match cli.common.log_format {
        LogFormat::Text => log::Format::Text,
        LogFormat::Json => log::Format::Json,
    });
    let command = cli
        .command
        .unwrap_or_else(|| Command::Serve(DefaultServe::parse_from(["mage-service"]).args));
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Could not start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(async move {
        match command {
            Command::Serve(args) => serve(cli.common, args).await,
            Command::Run(args) => run_once(cli.common, args).await,
            Command::Validate => validate(&cli.common).map(|_| ExitCode::SUCCESS),
            Command::Info => info(&cli.common).map(|_| ExitCode::SUCCESS),
            Command::Health { port } => Ok(health(listen_port(port))),
            Command::Env => env_report(&cli.common),
            Command::Ledger { command } => ledger_command(&cli.common, command),
            Command::CheckConnections { pipeline, json } => {
                check_connections(&cli.common, pipeline.as_deref(), json).await
            }
            Command::Jobs(args) => {
                log::to_stderr();
                jobs::run(args).await
            }
        }
    });
    match result {
        Ok(code) => code,
        Err(message) => {
            log::error(&message);
            ExitCode::FAILURE
        }
    }
}

fn load_manifest(common: &Common) -> Result<Manifest, String> {
    let path = common.dir.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&path).map_err(|e| {
        format!(
            "Could not read {}: {e}. Set --dir or MAGE_SERVICE_DIR.",
            path.display()
        )
    })?;
    let manifest = Manifest::parse(&text).map_err(|e| e.0)?;
    let problems = manifest.validate(Some(&common.dir.join(&manifest.service.project)));
    if !problems.is_empty() {
        return Err(format!(
            "The service cannot start:\n  - {}",
            problems.join("\n  - ")
        ));
    }
    Ok(manifest)
}

fn validate(common: &Common) -> Result<(), String> {
    let manifest = load_manifest(common)?;
    for pipeline in &manifest.pipelines {
        for trigger in &pipeline.triggers {
            if trigger.kind == TriggerKind::Time {
                scheduler::Schedule::new(pipeline, trigger).map_err(|e| e.to_string())?;
            }
        }
        for block in &pipeline.blocks {
            if let Some(binary) = &block.binary
                && !common.dir.join(binary).is_file()
            {
                return Err(format!(
                    "Pipeline {}: Rust block {} needs {binary}, which is not in the service.",
                    pipeline.uuid, block.uuid
                ));
            }
        }
    }
    println!(
        "{}: {} pipeline(s), {} block(s); everything needed is in place.",
        manifest.service.name,
        manifest.pipelines.len(),
        manifest
            .pipelines
            .iter()
            .map(|p| p.blocks.len())
            .sum::<usize>()
    );
    Ok(())
}

fn info(common: &Common) -> Result<(), String> {
    let manifest = load_manifest(common)?;
    println!(
        "{}  (from project {}, exported {}, Mage {})",
        manifest.service.name,
        manifest.service.project,
        manifest.service.exported_at,
        manifest.service.mage_version
    );
    for pipeline in &manifest.pipelines {
        println!(
            "\n{}  max {} run(s) at once",
            pipeline.uuid, pipeline.max_concurrent_runs
        );
        if let Some(description) = &pipeline.description {
            println!("  {description}");
        }
        for block in pipeline.topological_order().unwrap_or_default() {
            let language = match block.language {
                Language::Python => "py",
                Language::Rust => "rs",
                Language::R => "r",
                Language::Sql => "sql",
                Language::Window => "window",
            };
            let upstream = if block.upstream.is_empty() {
                String::new()
            } else {
                format!("  <- {}", block.upstream.join(", "))
            };
            println!(
                "  {:<3} {:<14} {}{upstream}",
                language,
                block.block_type.as_str(),
                block.uuid
            );
        }
        for trigger in &pipeline.triggers {
            println!(
                "  trigger {}: {} {}{}",
                trigger.name,
                trigger.kind.as_str(),
                trigger.describe().unwrap_or_default(),
                if trigger.active { "" } else { " (inactive)" }
            );
        }
    }
    Ok(())
}

fn load_secrets(manifest: &Manifest) -> secrets::Secrets {
    let environment: std::collections::BTreeMap<String, String> = std::env::vars().collect();
    secrets::Secrets::load(manifest, &environment)
}

fn env_report(common: &Common) -> Result<ExitCode, String> {
    let manifest = load_manifest(common)?;
    let secrets = load_secrets(&manifest);
    if manifest.environment.is_empty() {
        println!("The exported code reads no environment variables.");
        return Ok(ExitCode::SUCCESS);
    }
    let width = manifest
        .environment
        .iter()
        .map(|v| v.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    println!(
        "{:<width$}  {:<9}  {:<8}  {:<6}  used by",
        "name", "status", "required", "secret"
    );
    let mut missing_required = false;
    for variable in &manifest.environment {
        let status = match secrets.source(&variable.name) {
            Some(secrets::Source::Environment) => "set",
            Some(secrets::Source::File) => "set (file)",
            Some(secrets::Source::Directory) => "set (dir)",
            Some(secrets::Source::Provider) => "set (provider)",
            None => {
                if variable.required {
                    missing_required = true;
                }
                "MISSING"
            }
        };
        println!(
            "{:<width$}  {:<9}  {:<8}  {:<6}  {}",
            variable.name,
            status,
            if variable.required { "yes" } else { "no" },
            if variable.secret || secrets::looks_secret(&variable.name) {
                "yes"
            } else {
                "no"
            },
            variable
                .used_by
                .iter()
                .cloned()
                .chain(variable.connectors.iter().map(|c| format!("connector {c}")))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    for problem in &secrets.problems {
        println!("problem: {problem}");
    }
    Ok(if missing_required || !secrets.problems.is_empty() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// Where the run history is: PostgreSQL with a ledger URL, else the data directory's file.
fn ledger_location(common: &Common, service: &str) -> Result<ledger::Location, String> {
    let Some(url) = &common.ledger_url else {
        return Ok(ledger::Location::Sqlite(data_dir(common).join("runs.db")));
    };
    let schema = &common.ledger_schema;
    let valid = schema.len() <= 63
        && schema.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(format!(
            "MAGE_SERVICE_LEDGER_SCHEMA {schema} is not a schema name: letters, digits and _, \
             at most 63, not starting with a digit."
        ));
    }
    Ok(ledger::Location::Postgres {
        url: url.clone(),
        schema: schema.clone(),
        service: service.to_string(),
    })
}

async fn check_connections(
    common: &Common,
    pipeline: Option<&str>,
    json: bool,
) -> Result<ExitCode, String> {
    use mage_stream_ports::check::CheckResult;

    let manifest = load_manifest(common)?;
    if let Some(name) = pipeline
        && !manifest
            .pipelines
            .iter()
            .any(|p| p.uuid == name && p.stream.is_some())
    {
        return Err(format!("This service has no stream named {name}."));
    }
    let secrets = load_secrets(&manifest);
    log::set_redactions(secrets.redacted_values());
    let lookup = |name: &str| {
        secrets
            .additions
            .get(name)
            .cloned()
            .or_else(|| std::env::var(name).ok())
    };
    let checked = streams::check_connections(&manifest, pipeline, &lookup).await;
    let failed = checked
        .iter()
        .any(|c| c.report.result == CheckResult::Failed);
    if json {
        let rows: Vec<serde_json::Value> = checked
            .iter()
            .map(|c| {
                serde_json::json!({
                    "pipeline": c.pipeline,
                    "role": c.role,
                    "kind": c.kind,
                    "result": c.report.result.as_str(),
                    "summary": log::redact(&c.report.summary),
                    "facts": c.report.facts.iter()
                        .map(|(label, value)| serde_json::json!({"label": label, "value": value}))
                        .collect::<Vec<_>>(),
                    "problems": c.report.problems.iter().map(|p| log::redact(p)).collect::<Vec<_>>(),
                })
            })
            .collect();
        println!("{}", serde_json::Value::Array(rows));
    } else if checked.is_empty() {
        println!("This service has no streams.");
    } else {
        for c in &checked {
            println!(
                "{} {} ({}): {}: {}",
                c.pipeline,
                c.role,
                c.kind,
                c.report.result.as_str().replace('_', " "),
                log::redact(&c.report.summary)
            );
            for (label, value) in &c.report.facts {
                println!("    {label}: {value}");
            }
            for problem in &c.report.problems {
                println!("    - {}", log::redact(problem));
            }
        }
    }
    Ok(if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

fn ledger_command(common: &Common, command: LedgerCommand) -> Result<ExitCode, String> {
    let manifest = load_manifest(common)?;
    let location = ledger_location(common, &manifest.service.name)?;
    match (command, &location) {
        (LedgerCommand::Migrate, ledger::Location::Sqlite(path)) => {
            Ledger::open(path)
                .map_err(|e| e.to_string())?
                .release()
                .ok();
            println!(
                "The run history {} is at version {}.",
                path.display(),
                ledger::SQLITE_VERSION
            );
        }
        (LedgerCommand::Migrate, ledger::Location::Postgres { url, schema, .. }) => {
            let version = ledger::postgres_version(url, schema, true).map_err(|e| e.to_string())?;
            if version.current == version.known {
                println!(
                    "The run history in schema {schema} is at version {}.",
                    version.known
                );
            } else {
                println!(
                    "Migrated the run history in schema {schema} from version {} to {}.",
                    version.current, version.known
                );
            }
        }
        (LedgerCommand::Import { .. }, ledger::Location::Sqlite(_)) => {
            return Err(
                "Importing copies a SQLite run history into PostgreSQL: set \
                 MAGE_SERVICE_LEDGER_URL."
                    .into(),
            );
        }
        (LedgerCommand::Import { path }, ledger::Location::Postgres { schema, .. }) => {
            let ledger = Ledger::open_at(&location, common.ledger_migrate == Switch::On)
                .map_err(|e| e.to_string())?;
            let copied = ledger.import_sqlite(&path).map_err(|e| e.to_string())?;
            let _ = ledger.release();
            println!(
                "Copied {copied} run(s) from {} into schema {schema}.",
                path.display()
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn data_dir(common: &Common) -> PathBuf {
    common
        .data
        .clone()
        .unwrap_or_else(|| common.dir.join("data"))
}

/// When the Python worker starts: before the service accepts runs (serve, so a broken
/// Python fails at start-up), or alongside a one-shot run of a pipeline that needs it.
enum WarmPython {
    Now,
    WithRunOf(String),
}

async fn start_service(
    common: &Common,
    schedules_enabled: bool,
    retention_days: u32,
    warm: WarmPython,
) -> Result<Arc<Service>, String> {
    let manifest = load_manifest(common)?;
    let secrets = load_secrets(&manifest);
    if !secrets.problems.is_empty() {
        return Err(format!(
            "Secrets could not be read:\n  - {}",
            secrets.problems.join("\n  - ")
        ));
    }
    log::set_redactions(secrets.redacted_values());
    let (missing_required, missing_optional) = secrets.missing(&manifest);
    if !missing_required.is_empty() {
        let message = format!(
            "Environment variables the blocks need are not set: {}. Set them with -e, an \
             env file, NAME_FILE or MAGE_SERVICE_SECRETS_DIR (see `mage-service env`).",
            missing_required.join(", ")
        );
        if std::env::var("MAGE_SERVICE_STRICT_ENV").is_ok_and(|v| v == "on") {
            return Err(message);
        }
        log::warn(&message);
    }
    if !missing_optional.is_empty() {
        log::info(&format!(
            "Not set, read with a default: {}.",
            missing_optional.join(", ")
        ));
    }
    // Block processes start from an empty environment: the service's variables without its
    // own credentials, then the values read from secret files.
    let declared: std::collections::BTreeSet<String> = manifest
        .environment
        .iter()
        .map(|v| v.name.clone())
        .collect();
    // Variables only stream connectors read stay with the service (ADR-0033).
    let connector_only = streams::connector_variables(&manifest);
    let extra_env: Vec<(String, String)> = environment::block_environment(
        std::env::vars(),
        &declared,
        &environment::pass_list(),
        environment::Mode::from_env(),
    )
    .into_iter()
    .filter(|(name, _)| !connector_only.contains(name))
    .filter(|(name, _)| !secrets.additions.contains_key(name))
    .chain(
        secrets
            .additions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    )
    .collect();
    let data = data_dir(common);
    service::ensure_dir(&data.join("runs"))
        .map_err(|e| format!("Could not create {}: {e}", data.display()))?;
    let location = ledger_location(common, &manifest.service.name)?;
    let ledger = Ledger::open_at(&location, common.ledger_migrate == Switch::On)
        .map_err(|e| e.to_string())?;
    log::info(&format!(
        "Run history: {}, instance {}.",
        match &location {
            ledger::Location::Sqlite(path) => path.display().to_string(),
            ledger::Location::Postgres { schema, .. } => format!("PostgreSQL schema {schema}"),
        },
        ledger.instance()
    ));
    let recovered = ledger.recover_interrupted().map_err(|e| e.to_string())?;
    if !recovered.is_empty() {
        log::warn(&format!(
            "{} run(s) were running when the service stopped; they are marked failed: {}",
            recovered.len(),
            recovered.join(", ")
        ));
    }
    let needs_python = manifest.pipelines.iter().any(|p| {
        p.blocks
            .iter()
            .any(|b| matches!(b.language, Language::Python | Language::R | Language::Sql))
    });
    let python = if needs_python {
        let worker = common.worker.clone().unwrap_or_else(|| {
            common
                .dir
                .join("python/mage_ai/pipeline_services/runtime/worker.py")
        });
        if !worker.is_file() {
            return Err(format!(
                "The Python block worker {} is missing; set --worker or MAGE_SERVICE_WORKER.",
                worker.display()
            ));
        }
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2);
        Some(PythonConfig {
            executable: common.python.clone(),
            worker_script: worker,
            project_dir: common.dir.join(&manifest.service.project),
            max_workers: common.python_workers.unwrap_or(cpus.min(8)),
            max_blocks_per_worker: common.python_max_blocks.max(1),
            max_rss_bytes: common.python_max_rss_mb.saturating_mul(1_048_576),
            env: [
                ("MAGE_SERVICE".to_string(), "1".to_string()),
                ("MLFLOW_DISABLE_AGENT_HINT".to_string(), "1".to_string()),
                // Audit records of predictions (models.audit), searched at /v1/audit.
                (
                    "MAGE_SERVICE_AUDIT_DIR".to_string(),
                    data.join("audit").display().to_string(),
                ),
                (
                    "MAGE_SERVICE_NAME".to_string(),
                    manifest.service.name.clone(),
                ),
                (
                    "MAGE_SERVICE_SOURCE_SHA256".to_string(),
                    manifest.service.source_sha256.clone(),
                ),
                // Challengers' shadow records (models.predict), compared at /v1/models.
                (
                    "MAGE_SERVICE_SHADOW_DIR".to_string(),
                    data.join("shadow").display().to_string(),
                ),
                // R blocks run in an R session kept between runs (runtime.run_r_job).
                (
                    "MAGE_R_RESIDENT".to_string(),
                    if std::env::var("MAGE_SERVICE_R_RESIDENT").is_ok_and(|v| v == "off") {
                        "0"
                    } else {
                        "1"
                    }
                    .to_string(),
                ),
                (
                    "MAGE_SERVICE_MODELS_DIR".to_string(),
                    common.dir.join("models").display().to_string(),
                ),
            ]
            .into_iter()
            .chain(extra_env.iter().cloned())
            .collect(),
        })
    } else {
        None
    };
    // Each Python worker of the manifest is a pool of its own interpreter (its virtual
    // environment in the image), started when one of its blocks first runs.
    let python_workers: std::collections::BTreeMap<_, _> = match &python {
        Some(config) => manifest
            .python_workers
            .iter()
            .map(|(alias, executable)| {
                let executable = if std::path::Path::new(executable).is_absolute() {
                    executable.clone()
                } else {
                    common.dir.join(executable).display().to_string()
                };
                let pool = PythonPool::new(PythonConfig {
                    executable,
                    ..config.clone()
                });
                (alias.clone(), pool)
            })
            .collect(),
        None => Default::default(),
    };
    let python = python.map(PythonPool::new);
    let service = Service::new(
        manifest,
        Config {
            state_store: crate::state_files::open_store()?,
            service_dir: common.dir.clone(),
            data_dir: data,
            schedules_enabled,
            max_total_runs: common.max_runs,
            retention_days,
            environment: common.environment.clone(),
            extra_env,
        },
        ledger,
        python.clone(),
        python_workers,
    );
    if let Some(python) = python {
        match warm {
            WarmPython::Now => {
                if let Err(error) = python.warm().await {
                    return Err(format!("Python blocks cannot run: {error}"));
                }
            }
            WarmPython::WithRunOf(pipeline) => {
                let needs_python = service.manifest.pipeline(&pipeline).is_some_and(|p| {
                    p.blocks.iter().any(|b| {
                        matches!(b.language, Language::Python | Language::R | Language::Sql)
                    })
                });
                if needs_python {
                    python.warm_in_background().await;
                }
            }
        }
    }
    Ok(service)
}

async fn serve(common: Common, args: ServeArgs) -> Result<ExitCode, String> {
    let schedules_enabled = args.schedules == Switch::On;
    let service = start_service(
        &common,
        schedules_enabled,
        args.retention_days,
        WarmPython::Now,
    )
    .await?;
    let from_files = load_secrets(&service.manifest);
    let token_value = args
        .token
        .filter(|t| !t.trim().is_empty())
        .or_else(|| from_files.get("MAGE_SERVICE_TOKEN").map(str::to_string));
    let read_token_value = args
        .read_token
        .filter(|t| !t.trim().is_empty())
        .or_else(|| {
            from_files
                .get("MAGE_SERVICE_READ_TOKEN")
                .map(str::to_string)
        });
    let ingest_token_value = args
        .ingest_token
        .filter(|t| !t.trim().is_empty())
        .or_else(|| {
            from_files
                .get("MAGE_SERVICE_INGEST_TOKEN")
                .map(str::to_string)
        });
    let token = match (args.auth, token_value) {
        (Switch::Off, _) => {
            log::warn(
                "Authentication is off (MAGE_SERVICE_AUTH=off): anyone who reaches the port can run pipelines.",
            );
            None
        }
        (Switch::On, Some(token)) => Some(token),
        (Switch::On, None) => Some(generated_token(&args.host, &service.data_dir)?),
    };
    let state = api::AppState {
        service: service.clone(),
        auth: api::Auth {
            token,
            read_token: read_token_value,
            ingest_token: ingest_token_value,
        },
    };
    let port = listen_port(args.port);
    let address: SocketAddr = format!("{}:{}", args.host, port)
        .parse()
        .map_err(|e| format!("Invalid address {}:{}: {e}", args.host, port))?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|e| format!("Could not listen on {address}: {e}"))?;
    // The bound address: port 0 picks a free port.
    let address = listener.local_addr().unwrap_or(address);

    let manifest = &service.manifest;
    log::info(&format!(
        "{} serves {} pipeline(s) on http://{address} ({})",
        manifest.service.name,
        manifest.pipelines.len(),
        manifest
            .pipelines
            .iter()
            .map(|p| p.uuid.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ));
    let active_triggers = manifest
        .pipelines
        .iter()
        .flat_map(|p| p.triggers.iter())
        .filter(|t| t.kind == TriggerKind::Time && t.active)
        .count();
    if active_triggers > 0 && !schedules_enabled {
        log::warn(&format!(
            "Schedules are off: {active_triggers} active time trigger(s) will not run. Set \
             MAGE_SERVICE_SCHEDULES=on when this service, and not Mage, should run them."
        ));
    }

    let dispatcher = tokio::spawn(service.clone().dispatch());
    let streams = streams::start(&service);
    alerts::watch(service.clone());
    if schedules_enabled {
        let owner = format!("{}-{}", hostname(), std::process::id());
        tokio::spawn(scheduler::run(service.clone(), owner));
    }
    let pruner = service.clone();
    tokio::spawn(async move {
        loop {
            pruner.prune();
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    });

    let shutdown_service = service.clone();
    let deadline = Duration::from_secs(args.shutdown_seconds);
    let stopping = Arc::new(tokio::sync::Notify::new());
    let signalled = stopping.clone();
    let server = axum::serve(listener, api::router(state)).with_graceful_shutdown(async move {
        shutdown_signal().await;
        signalled.notify_one();
        log::info("Stopping: no new runs are accepted.");
        // Streams stop reading and finish the batches in flight.
        shutdown_service.stream_stop.cancel();
        shutdown_service.shutdown(deadline).await;
    });
    // Open connections hold a graceful shutdown; past the deadline the service stops anyway.
    let bounded = async {
        stopping.notified().await;
        tokio::time::sleep(deadline + Duration::from_secs(30)).await;
    };
    tokio::select! {
        result = server.into_future() => {
            result.map_err(|e| format!("The server failed: {e}"))?;
        }
        _ = bounded => log::warn("Stopping with connections still open, past the deadline."),
    }
    let _ = tokio::time::timeout(Duration::from_secs(15), dispatcher).await;
    for stream in streams {
        let _ = tokio::time::timeout(Duration::from_secs(25), stream).await;
    }
    // A restart recovers what this instance left without waiting for its lease to expire.
    let _ = service.ledger.release();
    log::info("Stopped.");
    // A connector call stuck in a blocking thread would keep the runtime, and so the process,
    // alive after its shutdown: everything that must finish has finished or timed out.
    std::process::exit(0)
}

/// Without a configured token, a service that only listens on loopback makes one and keeps
/// it in a file only its user can read. The token used to be printed in the log, where
/// anyone with access to the logs could take it; a service other hosts can reach must be
/// given a token.
fn generated_token(host: &str, data_dir: &Path) -> Result<String, String> {
    if !is_loopback(host) {
        return Err(format!(
            "No MAGE_SERVICE_TOKEN is set and the service listens on {host}, which other hosts \
             can reach. Set MAGE_SERVICE_TOKEN (or MAGE_SERVICE_TOKEN_FILE), or \
             MAGE_SERVICE_AUTH=off for a service no one else can reach."
        ));
    }
    let token = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let path = data_dir.join(TOKEN_FILE);
    write_private(&path, &token).map_err(|e| {
        format!(
            "Could not write the service token to {}: {e}",
            path.display()
        )
    })?;
    log::info(&format!(
        "No MAGE_SERVICE_TOKEN is set; this run of the service made a token, in {}",
        path.display()
    ));
    Ok(token)
}

pub const TOKEN_FILE: &str = "service-token";

fn is_loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .trim_matches(|c| c == '[' || c == ']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    // An existing file keeps its mode; make it private before writing.
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(text.as_bytes())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = terminate.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

fn health(port: u16) -> ExitCode {
    use std::io::{Read, Write};
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(3))
    else {
        eprintln!("No service answers on port {port}.");
        return ExitCode::FAILURE;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
    let request =
        format!("GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return ExitCode::FAILURE;
    }
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    if response.starts_with("HTTP/1.1 200") {
        ExitCode::SUCCESS
    } else {
        eprintln!("{}", response.lines().next().unwrap_or("No response."));
        ExitCode::FAILURE
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "local".into())
}

fn parse_variables(pairs: &[String]) -> Result<Map<String, Value>, String> {
    let mut variables = Map::new();
    for pair in pairs {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| format!("--var {pair:?} is not KEY=VALUE"))?;
        let value =
            serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
        variables.insert(key.trim().to_string(), value);
    }
    Ok(variables)
}

async fn run_once(common: Common, args: RunArgs) -> Result<ExitCode, String> {
    // The run's summary, or its JSON, is alone on stdout; logs go to stderr.
    log::to_stderr();
    let variables = parse_variables(&args.variables)?;
    let service = start_service(
        &common,
        false,
        0,
        WarmPython::WithRunOf(args.pipeline.clone()),
    )
    .await?;
    if service.manifest.pipeline(&args.pipeline).is_none() {
        let names: Vec<_> = service
            .manifest
            .pipelines
            .iter()
            .map(|p| p.uuid.as_str())
            .collect();
        return Err(format!(
            "The service has no pipeline {}; it has {}.",
            args.pipeline,
            names.join(", ")
        ));
    }
    let (run_id, _) = service
        .submit(&args.pipeline, "cli", &variables, None)
        .map_err(|e| e.to_string())?;
    let dispatcher = tokio::spawn(service.clone().dispatch());
    let interrupted = tokio::select! {
        _ = wait_for_run(&service, &run_id) => false,
        _ = shutdown_signal() => true,
    };
    if interrupted {
        log::warn("Interrupted: cancelling the run.");
        let _ = service.cancel(&run_id);
    }
    service
        .shutdown(Duration::from_secs(if interrupted { 5 } else { 0 }))
        .await;
    let _ = tokio::time::timeout(Duration::from_secs(15), dispatcher).await;
    let _ = service.ledger.release();
    let detail = service
        .ledger
        .run(&run_id)
        .map_err(|e| e.to_string())?
        .ok_or("the run is missing from the run history")?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&detail).unwrap_or_default()
        );
    } else {
        print_run(&detail, &service.run_dir(&run_id));
    }
    Ok(if detail.run.status == RunStatus::Completed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

async fn wait_for_run(service: &Service, run_id: &str) {
    loop {
        if let Ok(Some(detail)) = service.ledger.run(run_id)
            && detail.run.status.finished()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn print_run(detail: &mage_service_core::protocol::RunDetail, run_dir: &Path) {
    let run = &detail.run;
    println!();
    println!(
        "{} {}  run {}  {}",
        status_mark(run.status),
        run.pipeline,
        run.id,
        run.duration_ms.map(format_ms).unwrap_or_default()
    );
    let width = detail
        .blocks
        .iter()
        .map(|b| b.block.len())
        .max()
        .unwrap_or(5)
        .max(5);
    for block in &detail.blocks {
        let mark = match block.status {
            BlockStatus::Completed => "ok  ",
            BlockStatus::Failed => "FAIL",
            BlockStatus::UpstreamFailed => "skip",
            BlockStatus::ConditionFailed => "cond",
            BlockStatus::Cancelled => "stop",
            BlockStatus::Pending | BlockStatus::Running => "... ",
        };
        let attempts = if block.attempts > 1 {
            format!("  {} attempts", block.attempts)
        } else {
            String::new()
        };
        println!(
            "  {mark}  {:<width$}  {:>9}{attempts}",
            block.block,
            block.duration_ms.map(format_ms).unwrap_or_default()
        );
        if matches!(
            block.status,
            BlockStatus::Failed | BlockStatus::ConditionFailed
        ) && let Some(error) = &block.error
        {
            for line in error.lines() {
                println!("        {line}");
            }
        }
    }
    if let Some(error) = &run.error
        && run.status != RunStatus::Completed
    {
        println!("\n{error}");
    }
    println!("\nLog: {}", run_dir.join("run.log").display());
}

fn status_mark(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Completed => "COMPLETED",
        RunStatus::Failed => "FAILED",
        RunStatus::Cancelled => "CANCELLED",
        RunStatus::Queued => "QUEUED",
        RunStatus::Running => "RUNNING",
    }
}

fn format_ms(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.2} s", ms as f64 / 1000.0)
    } else {
        format!("{}m {:02}s", ms / 60_000, (ms % 60_000) / 1000)
    }
}

/// The port to listen on: MAGE_SERVICE_PORT (or --port), else the PORT a platform sets, else
/// 8080. Platforms such as Code Engine and Cloud Run route to PORT; a service that ignored
/// it failed their health checks unless MAGE_SERVICE_PORT repeated it (I-21).
fn listen_port(configured: Option<u16>) -> u16 {
    port_from(configured, std::env::var("PORT").ok().as_deref())
}

fn port_from(configured: Option<u16>, platform: Option<&str>) -> u16 {
    configured
        .or_else(|| platform?.trim().parse().ok())
        .unwrap_or(8080)
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_port_is_the_configured_one_else_the_platforms_else_8080() {
        assert_eq!(super::port_from(Some(9000), Some("7000")), 9000);
        assert_eq!(super::port_from(None, Some("7000")), 7000);
        assert_eq!(super::port_from(None, Some("not a port")), 8080);
        assert_eq!(super::port_from(None, None), 8080);
    }

    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn loopback_addresses() {
        for host in ["127.0.0.1", "::1", "[::1]", "localhost", "127.0.0.2"] {
            assert!(is_loopback(host), "{host}");
        }
        for host in ["0.0.0.0", "::", "10.0.0.5", "example.com"] {
            assert!(!is_loopback(host), "{host}");
        }
    }

    #[test]
    fn a_reachable_service_needs_a_token() {
        let folder = tempfile::tempdir().unwrap();

        let error = generated_token("0.0.0.0", folder.path()).unwrap_err();

        assert!(error.contains("Set MAGE_SERVICE_TOKEN"), "{error}");
        assert!(!folder.path().join(TOKEN_FILE).exists());
    }

    #[test]
    fn a_loopback_service_keeps_its_token_in_a_private_file() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join(TOKEN_FILE);
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let token = generated_token("127.0.0.1", folder.path()).unwrap();

        assert_eq!(token.len(), 64);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), token);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
