//! mage-service: runs pipelines exported from a Mage project, without Mage.

mod api;
mod executor;
mod ibm;
mod ledger;
mod log;
mod python;
mod scheduler;
mod secrets;
mod service;

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
    /// Exit 0 when the local service answers /healthz; for Docker's HEALTHCHECK.
    Health {
        #[arg(long, env = "MAGE_SERVICE_PORT", default_value_t = 8080)]
        port: u16,
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
    #[arg(long, env = "MAGE_SERVICE_PORT", default_value_t = 8080)]
    port: u16,
    /// Run the active time triggers. Off by default, so a service exported next to a running
    /// Mage does not run the same schedule twice.
    #[arg(long, env = "MAGE_SERVICE_SCHEDULES", value_enum, default_value_t = Switch::Off)]
    schedules: Switch,
    /// The token for every API call. Without one, the service makes one and prints it.
    #[arg(long, env = "MAGE_SERVICE_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// A token that can only read runs, logs and the snapshot.
    #[arg(long, env = "MAGE_SERVICE_READ_TOKEN", hide_env_values = true)]
    read_token: Option<String>,
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
            Command::Health { port } => Ok(health(port)),
            Command::Env => env_report(&cli.common),
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
                match trigger.kind {
                    TriggerKind::Time => "time",
                    TriggerKind::Api => "api",
                },
                trigger.schedule.as_deref().unwrap_or(""),
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
            variable.used_by.join(", ")
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

fn data_dir(common: &Common) -> PathBuf {
    common
        .data
        .clone()
        .unwrap_or_else(|| common.dir.join("data"))
}

async fn start_service(
    common: &Common,
    schedules_enabled: bool,
    retention_days: u32,
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
    let extra_env: Vec<(String, String)> = secrets
        .additions
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let data = data_dir(common);
    service::ensure_dir(&data.join("runs"))
        .map_err(|e| format!("Could not create {}: {e}", data.display()))?;
    let ledger = Ledger::open(&data.join("runs.db")).map_err(|e| e.to_string())?;
    let recovered = ledger.recover_interrupted().map_err(|e| e.to_string())?;
    if !recovered.is_empty() {
        log::warn(&format!(
            "{} run(s) were running when the service stopped; they are marked failed: {}",
            recovered.len(),
            recovered.join(", ")
        ));
    }
    let needs_python = manifest
        .pipelines
        .iter()
        .any(|p| p.blocks.iter().any(|b| b.language == Language::Python));
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
        Some(PythonPool::new(PythonConfig {
            executable: common.python.clone(),
            worker_script: worker,
            project_dir: common.dir.join(&manifest.service.project),
            max_workers: common.python_workers.unwrap_or(cpus.min(8)),
            max_blocks_per_worker: common.python_max_blocks.max(1),
            env: [
                ("MAGE_SERVICE".to_string(), "1".to_string()),
                ("MLFLOW_DISABLE_AGENT_HINT".to_string(), "1".to_string()),
                (
                    "MAGE_SERVICE_MODELS_DIR".to_string(),
                    common.dir.join("models").display().to_string(),
                ),
            ]
            .into_iter()
            .chain(extra_env.iter().cloned())
            .collect(),
        }))
    } else {
        None
    };
    let service = Service::new(
        manifest,
        Config {
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
    );
    if let Some(python) = python
        && let Err(error) = python.warm().await
    {
        return Err(format!("Python blocks cannot run: {error}"));
    }
    Ok(service)
}

async fn serve(common: Common, args: ServeArgs) -> Result<ExitCode, String> {
    let schedules_enabled = args.schedules == Switch::On;
    let service = start_service(&common, schedules_enabled, args.retention_days).await?;
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
    let token = match (args.auth, token_value) {
        (Switch::Off, _) => {
            log::warn(
                "Authentication is off (MAGE_SERVICE_AUTH=off): anyone who reaches the port can run pipelines.",
            );
            None
        }
        (Switch::On, Some(token)) => Some(token),
        (Switch::On, None) => {
            let token = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            log::info(&format!(
                "No MAGE_SERVICE_TOKEN is set; this run of the service accepts the token {token}"
            ));
            Some(token)
        }
    };
    let state = api::AppState {
        service: service.clone(),
        auth: api::Auth {
            token,
            read_token: read_token_value,
        },
    };
    let address: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .map_err(|e| format!("Invalid address {}:{}: {e}", args.host, args.port))?;
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|e| format!("Could not listen on {address}: {e}"))?;

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
    let server = axum::serve(listener, api::router(state)).with_graceful_shutdown(async move {
        shutdown_signal().await;
        log::info("Stopping: no new runs are accepted.");
        shutdown_service.shutdown(deadline).await;
    });
    server
        .await
        .map_err(|e| format!("The server failed: {e}"))?;
    let _ = tokio::time::timeout(Duration::from_secs(15), dispatcher).await;
    log::info("Stopped.");
    Ok(ExitCode::SUCCESS)
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
    let variables = parse_variables(&args.variables)?;
    let service = start_service(&common, false, 0).await?;
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
        if block.status == BlockStatus::Failed
            && let Some(error) = &block.error
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
