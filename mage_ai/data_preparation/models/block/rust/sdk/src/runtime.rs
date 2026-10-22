//! The generated `main` of a block binary calls [`main`]: it runs the block function on
//! the job, writes its outputs, runs its tests and reports errors and panics to Mage.

use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::prelude::*;

use crate::context::{BlockContext, Vars};
use crate::extract::{FromInput, Inputs, Item};
use crate::output::{IntoOutputs, Output, read_frame, write_output};
use crate::protocol::{
    API_VERSION, ChainBlockRecord, ChainResult, ERROR_FILE, ErrorRecord, Job, OutputRecord,
    RESULT_FILE, RunResult, TestRecord, write_json_atomic,
};

/// A block function: any function whose parameters are [`FromInput`] values and whose
/// return value is [`IntoOutputs`].
#[diagnostic::on_unimplemented(
    message = "this function cannot be a Mage block function",
    label = "check its parameters and return type",
    note = "parameters can be LazyFrame, DataFrame, Option<LazyFrame>, Vec<LazyFrame>, serde_json::Value, Vars or BlockContext, up to 12",
    note = "it returns LazyFrame, DataFrame, serde_json::Value, bool, (), a tuple or Vec of tables, an Option of these, or a Result of any of them"
)]
pub trait BlockFn<Args> {
    fn call(self, inputs: &mut Inputs<'_>) -> anyhow::Result<Vec<Output>>;
}

macro_rules! block_fn {
    ($($arg:ident),*) => {
        impl<F, R, $($arg,)*> BlockFn<($($arg,)*)> for F
        where
            F: FnOnce($($arg),*) -> R,
            R: IntoOutputs,
            $($arg: FromInput,)*
        {
            #[allow(non_snake_case, unused_variables)]
            fn call(self, inputs: &mut Inputs<'_>) -> anyhow::Result<Vec<Output>> {
                $(let $arg = $arg::from_input(inputs)?;)*
                (self)($($arg),*).into_outputs()
            }
        }
    };
}

block_fn!();
block_fn!(A1);
block_fn!(A1, A2);
block_fn!(A1, A2, A3);
block_fn!(A1, A2, A3, A4);
block_fn!(A1, A2, A3, A4, A5);
block_fn!(A1, A2, A3, A4, A5, A6);
block_fn!(A1, A2, A3, A4, A5, A6, A7);
block_fn!(A1, A2, A3, A4, A5, A6, A7, A8);
block_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9);
block_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
block_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11);
block_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12);

/// What a test function receives: the block's first table, JSON value or decision.
pub struct TestSubject {
    frame: Option<DataFrame>,
    json: Option<Value>,
    decision: Option<bool>,
}

pub trait TestOutcome {
    fn into_outcome(self) -> Result<(), String>;
}

impl TestOutcome for () {
    fn into_outcome(self) -> Result<(), String> {
        Ok(())
    }
}

impl TestOutcome for bool {
    fn into_outcome(self) -> Result<(), String> {
        if self {
            Ok(())
        } else {
            Err("The test returned false".into())
        }
    }
}

impl<E: Into<anyhow::Error>> TestOutcome for Result<(), E> {
    fn into_outcome(self) -> Result<(), String> {
        self.map_err(|error| format!("{:#}", error.into()))
    }
}

/// A test function: `fn(&DataFrame)`, `fn(&Value)` or `fn(bool)` returning `()`, `bool`
/// or `Result<()>`.
#[diagnostic::on_unimplemented(
    message = "this function cannot be a Mage block test",
    label = "check its parameter and return type",
    note = "a test takes &DataFrame (the first table output), &serde_json::Value or bool (a decision) and returns (), bool or Result<()>"
)]
pub trait TestFn<Subject> {
    fn check(&self, subject: &TestSubject) -> Result<(), String>;
}

impl<F, R> TestFn<DataFrame> for F
where
    F: Fn(&DataFrame) -> R,
    R: TestOutcome,
{
    fn check(&self, subject: &TestSubject) -> Result<(), String> {
        match &subject.frame {
            Some(frame) => self(frame).into_outcome(),
            None => Err("The test takes a table and the block returned none".into()),
        }
    }
}

impl<F, R> TestFn<Value> for F
where
    F: Fn(&Value) -> R,
    R: TestOutcome,
{
    fn check(&self, subject: &TestSubject) -> Result<(), String> {
        match &subject.json {
            Some(value) => self(value).into_outcome(),
            None => Err("The test takes a JSON value and the block returned none".into()),
        }
    }
}

impl<F, R> TestFn<bool> for F
where
    F: Fn(bool) -> R,
    R: TestOutcome,
{
    fn check(&self, subject: &TestSubject) -> Result<(), String> {
        match subject.decision {
            Some(value) => self(value).into_outcome(),
            None => Err("The test takes a decision and the block returned none".into()),
        }
    }
}

type Check = Box<dyn Fn(&TestSubject) -> Result<(), String>>;

pub struct Test {
    name: &'static str,
    check: Check,
}

/// Registers a test function; the generated `main` calls this for each `test_*` function.
pub fn test<S, F: TestFn<S> + 'static>(name: &'static str, function: F) -> Test {
    Test {
        name,
        check: Box::new(move |subject| function.check(subject)),
    }
}

pub(crate) static PANIC: Mutex<Option<ErrorRecord>> = Mutex::new(None);

pub(crate) fn install_panic_hook() {
    // Mage reports the panic with its location; Rust's default message would repeat it.
    panic::set_hook(Box::new(move |info| {
        let message = if let Some(text) = info.payload().downcast_ref::<&str>() {
            (*text).to_string()
        } else if let Some(text) = info.payload().downcast_ref::<String>() {
            text.clone()
        } else {
            "The block panicked".to_string()
        };
        let location = info.location().map(|location| {
            format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        });
        let backtrace = std::backtrace::Backtrace::capture();
        let message = if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            format!("{message}\n\n{backtrace}")
        } else {
            message
        };
        if let Ok(mut slot) = PANIC.lock()
            && slot.is_none()
        {
            *slot = Some(ErrorRecord {
                kind: "panic",
                message,
                location,
                block_uuid: current_block(),
            });
        }
    }));
}

/// The block binary exits when the Mage process that started it is gone, so a stopped
/// block run leaves no Rust process computing.
pub(crate) fn exit_with_parent() {
    #[cfg(unix)]
    {
        let parent = std::os::unix::process::parent_id();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if std::os::unix::process::parent_id() != parent {
                    eprintln!("Mage stopped the block run; exiting.");
                    std::process::exit(130);
                }
            }
        });
    }
}

/// Runs the block function on the job in `job_dir`; [`main`] reports its error to Mage.
pub fn run_job<Args, F: BlockFn<Args>>(
    block: F,
    tests: Vec<Test>,
    job_dir: &Path,
) -> anyhow::Result<()> {
    let mut timings = Timings::start();
    let job = Job::read(job_dir).context("Reading the job from Mage")?;
    let output_dir = job_dir.join(&job.output_dir);
    std::fs::create_dir_all(&output_dir)?;
    let scratch_dir = job_dir.join("scratch");
    std::fs::create_dir_all(&scratch_dir)?;
    let context = BlockContext {
        block_uuid: job.block_uuid.clone(),
        block_type: job.block_type.clone(),
        pipeline_uuid: job.pipeline_uuid.clone(),
        execution_partition: job.execution_partition.clone(),
        variables: Vars(job.variables.clone()),
        scratch_dir,
        block_sha256: job.block_sha256.clone(),
    };
    let upstream = job.inputs.len();
    let mut inputs = Inputs {
        context: &context,
        job_dir,
        items: job
            .inputs
            .into_iter()
            .map(Item::File)
            .collect::<Vec<_>>()
            .into_iter(),
        taken: 0,
    };
    timings.mark("job");
    let outputs = block.call(&mut inputs)?;
    timings.mark("function");
    if inputs.taken < upstream {
        eprintln!(
            "Note: the block has {upstream} upstream outputs and its function takes {}; the \
             others are unused.",
            inputs.taken
        );
    }

    if job.block_type == "conditional"
        && !(outputs.len() == 1 && matches!(outputs[0], Output::Decision(_)))
    {
        bail!("A conditional block returns a bool: true runs the blocks it governs");
    }

    let mut records = Vec::with_capacity(outputs.len());
    for (index, output) in outputs.into_iter().enumerate() {
        records.push(write_output(&output_dir, job_dir, index, output)?);
    }

    timings.mark("outputs");
    let results = if tests.is_empty() {
        Vec::new()
    } else {
        let subject = test_subject(job_dir, &records)?;
        tests
            .iter()
            .map(|test| {
                let outcome = panic::catch_unwind(AssertUnwindSafe(|| (test.check)(&subject)))
                    .unwrap_or_else(|_| Err(take_panic_message()));
                TestRecord {
                    name: test.name.to_string(),
                    passed: outcome.is_ok(),
                    message: outcome.err(),
                }
            })
            .collect()
    };

    timings.mark("tests");
    timings.report();
    write_json_atomic(
        &job_dir.join(RESULT_FILE),
        &RunResult {
            api_version: API_VERSION,
            outputs: records,
            tests: results,
        },
    )
}

/// Phase times inside the block process, printed when MAGE_RUST_TIMINGS=1. A lazy
/// output computes in `outputs`, where it is written.
struct Timings {
    enabled: bool,
    last: std::time::Instant,
    phases: Vec<(&'static str, f64)>,
}

impl Timings {
    fn start() -> Timings {
        Timings {
            enabled: std::env::var("MAGE_RUST_TIMINGS").is_ok_and(|v| v == "1"),
            last: std::time::Instant::now(),
            phases: Vec::new(),
        }
    }

    fn mark(&mut self, phase: &'static str) {
        if self.enabled {
            let now = std::time::Instant::now();
            self.phases
                .push((phase, now.duration_since(self.last).as_secs_f64()));
            self.last = now;
        }
    }

    fn report(&self) {
        if self.enabled {
            let phases: Vec<String> = self
                .phases
                .iter()
                .map(|(name, seconds)| format!("{name} {seconds:.3} s"))
                .collect();
            println!("[timings] {}", phases.join(", "));
        }
    }
}

pub(crate) fn take_panic_message() -> String {
    PANIC
        .lock()
        .ok()
        .and_then(|mut slot| slot.take())
        .map(|record| match record.location {
            Some(location) => format!("{} (at {location})", record.message),
            None => record.message,
        })
        .unwrap_or_else(|| "The test panicked".to_string())
}

fn test_subject(job_dir: &Path, records: &[OutputRecord]) -> anyhow::Result<TestSubject> {
    let mut subject = TestSubject {
        frame: None,
        json: None,
        decision: None,
    };
    for record in records {
        match record {
            OutputRecord::Frame { path, .. } if subject.frame.is_none() => {
                subject.frame = Some(read_frame(&job_dir.join(path))?);
            }
            OutputRecord::Json { path } if subject.json.is_none() => {
                subject.json = Some(serde_json::from_slice(&std::fs::read(job_dir.join(path))?)?);
            }
            OutputRecord::Decision { value } if subject.decision.is_none() => {
                subject.decision = Some(*value);
            }
            _ => {}
        }
    }
    Ok(subject)
}

/// The error and its causes, each once: Polars errors repeat their message as their
/// source.
pub fn describe(error: &anyhow::Error) -> String {
    let mut text = error.to_string();
    let mut shown = vec![text.clone()];
    let mut causes = Vec::new();
    for cause in error.chain().skip(1) {
        let message = cause.to_string();
        let repeated = shown
            .iter()
            .any(|earlier| earlier.starts_with(&message) || earlier.ends_with(&message));
        if !repeated {
            causes.push(message.clone());
            shown.push(message);
        }
    }
    if !causes.is_empty() {
        text.push_str("\n\nCaused by:");
        for cause in causes {
            text.push_str("\n    ");
            text.push_str(&cause.replace('\n', "\n    "));
        }
    }
    text
}

/// The entry point of a block binary: `binary <job directory>`.
pub fn main<Args, F: BlockFn<Args>>(block: F, tests: Vec<Test>) -> ! {
    let Some(job_dir) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("This is a Mage block; Mage runs it with a job directory.");
        std::process::exit(2);
    };
    install_panic_hook();
    exit_with_parent();
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| run_job(block, tests, &job_dir)));
    let record = match outcome {
        Ok(Ok(())) => std::process::exit(0),
        Ok(Err(error)) => ErrorRecord {
            kind: "error",
            message: describe(&error),
            location: None,
            block_uuid: None,
        },
        Err(_) => PANIC
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .unwrap_or(ErrorRecord {
                kind: "panic",
                message: "The block panicked".to_string(),
                location: None,
                block_uuid: None,
            }),
    };
    if let Err(error) = write_json_atomic(&job_dir.join(ERROR_FILE), &record) {
        eprintln!("Could not report the error to Mage: {error}");
    }
    std::process::exit(1);
}

// ---- chains -----------------------------------------------------------------------------

/// The line a chain prints before each block's output, so the logs name the block.
pub const BLOCK_MARKER: &str = "\u{1e}mage-block ";

static CURRENT_BLOCK: Mutex<Option<String>> = Mutex::new(None);

fn current_block() -> Option<String> {
    CURRENT_BLOCK.lock().ok().and_then(|slot| slot.clone())
}

pub(crate) fn set_current_block(uuid: &str) {
    if let Ok(mut slot) = CURRENT_BLOCK.lock() {
        *slot = Some(uuid.to_string());
    }
}

/// A block's function and tests, for a chain: what the generated library of a block
/// returns from `mage_block`.
type BlockCall = Box<dyn FnOnce(&mut Inputs<'_>) -> anyhow::Result<Vec<Output>>>;

/// The blocks a chain binary holds: each block's name and the function that returns it.
pub type ChainEntries = Vec<(&'static str, fn() -> ChainBlock)>;

pub struct ChainBlock {
    call: BlockCall,
    tests: Vec<Test>,
}

/// Wraps a block function and its tests for [`run_chain`].
pub fn chain_block<Args, F>(function: F, tests: Vec<Test>) -> ChainBlock
where
    F: BlockFn<Args> + 'static,
{
    ChainBlock {
        call: Box::new(move |inputs: &mut Inputs<'_>| function.call(inputs)),
        tests,
    }
}

/// The entry point of a binary that runs a chain of blocks: `binary <job directory>`,
/// with the chain in job.json. Each block takes the previous block's output in memory;
/// a table stays a lazy plan, so Polars optimizes the chain's queries as one (a filter
/// reaches the scan of the first block, the join streams into the aggregation). A
/// block's output is computed on its own only when Mage keeps it, its tests check it,
/// or it is the last block's.
pub fn run_chain(entries: ChainEntries) -> ! {
    let Some(job_dir) = std::env::args_os().nth(1).map(PathBuf::from) else {
        eprintln!("This runs Mage blocks; Mage runs it with a job directory.");
        std::process::exit(2);
    };
    install_panic_hook();
    exit_with_parent();
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| run_chain_job(&entries, &job_dir)));
    let record = match outcome {
        Ok(Ok(())) => std::process::exit(0),
        Ok(Err(error)) => ErrorRecord {
            kind: "error",
            message: describe(&error),
            location: None,
            block_uuid: current_block(),
        },
        Err(_) => PANIC
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .unwrap_or(ErrorRecord {
                kind: "panic",
                message: "The block panicked".to_string(),
                location: None,
                block_uuid: current_block(),
            }),
    };
    if let Err(error) = write_json_atomic(&job_dir.join(ERROR_FILE), &record) {
        eprintln!("Could not report the error to Mage: {error}");
    }
    std::process::exit(1);
}

fn run_tests(
    block: &str,
    tests: &[Test],
    subject: &TestSubject,
) -> anyhow::Result<Vec<TestRecord>> {
    let records: Vec<TestRecord> = tests
        .iter()
        .map(|test| {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| (test.check)(subject)))
                .unwrap_or_else(|_| Err(take_panic_message()));
            TestRecord {
                name: format!("{block}: {}", test.name),
                passed: outcome.is_ok(),
                message: outcome.err(),
            }
        })
        .collect();
    let failed: Vec<String> = records
        .iter()
        .filter(|t| !t.passed)
        .map(|t| format!("{} ({})", t.name, t.message.as_deref().unwrap_or("failed")))
        .collect();
    if !failed.is_empty() {
        bail!(
            "{} of {} tests failed: {}",
            failed.len(),
            records.len(),
            failed.join("; ")
        );
    }
    Ok(records)
}

fn run_chain_job(entries: &ChainEntries, job_dir: &Path) -> anyhow::Result<()> {
    let job = Job::read(job_dir).context("Reading the job from Mage")?;
    let steps = job
        .chain
        .clone()
        .ok_or_else(|| anyhow!("The job names no chain of blocks"))?;
    let output_dir = job_dir.join(&job.output_dir);
    std::fs::create_dir_all(&output_dir)?;
    let scratch_dir = job_dir.join("scratch");
    std::fs::create_dir_all(&scratch_dir)?;
    let mut items: Vec<Item> = job.inputs.into_iter().map(Item::File).collect();
    let mut blocks = Vec::with_capacity(steps.len());
    let mut tests = Vec::new();
    let mut final_outputs = Vec::new();

    for (position, step) in steps.iter().enumerate() {
        set_current_block(&step.block_uuid);
        println!("{BLOCK_MARKER}{}", step.block_uuid);
        let started = std::time::Instant::now();
        let make = entries
            .iter()
            .find(|(name, _)| *name == step.entry)
            .map(|(_, make)| *make)
            .ok_or_else(|| anyhow!("This binary has no block {}", step.entry))?;
        let block = make();
        let context = BlockContext {
            block_uuid: step.block_uuid.clone(),
            block_type: step.block_type.clone(),
            pipeline_uuid: job.pipeline_uuid.clone(),
            execution_partition: job.execution_partition.clone(),
            variables: Vars(job.variables.clone()),
            scratch_dir: scratch_dir.clone(),
            block_sha256: step.block_sha256.clone(),
        };
        let mut inputs = Inputs {
            context: &context,
            job_dir,
            items: std::mem::take(&mut items).into_iter(),
            taken: 0,
        };
        let outputs = (block.call)(&mut inputs)?;
        let mut kept = Vec::new();
        let mut planned = false;
        if position + 1 == steps.len() {
            for (index, output) in outputs.into_iter().enumerate() {
                final_outputs.push(write_output(&output_dir, job_dir, index, output)?);
            }
            if !block.tests.is_empty() {
                let subject = test_subject(job_dir, &final_outputs)?;
                tests.extend(run_tests(&step.block_uuid, &block.tests, &subject)?);
            }
        } else {
            let materialize = step.keep_output || !block.tests.is_empty();
            let mut subject = TestSubject {
                frame: None,
                json: None,
                decision: None,
            };
            let block_dir = output_dir.join("chain").join(position.to_string());
            for (index, output) in outputs.into_iter().enumerate() {
                let item = match output {
                    Output::Lazy(mut frame) if !materialize => {
                        // Resolve the plan's columns and types now, without computing
                        // it: a query that names a missing column fails at this block,
                        // not at the block that later computes the chain.
                        frame.collect_schema().with_context(|| {
                            format!("Planning the query of block {}", step.block_uuid)
                        })?;
                        planned = true;
                        Item::Lazy(frame)
                    }
                    Output::Lazy(frame) => {
                        let frame = frame
                            .collect()
                            .with_context(|| format!("Computing output {index}"))?;
                        materialized(frame, &mut subject)
                    }
                    Output::Frame(frame) => materialized(frame, &mut subject),
                    Output::Json(value) => {
                        if subject.json.is_none() {
                            subject.json = Some(value.clone());
                        }
                        Item::Json(value)
                    }
                    Output::Decision(_) => {
                        bail!("A conditional block cannot run in a chain of blocks")
                    }
                };
                if step.keep_output {
                    std::fs::create_dir_all(&block_dir)?;
                    let output = match &item {
                        Item::Lazy(frame) => Output::Frame(frame.as_ref().clone().collect()?),
                        Item::Json(value) => Output::Json(value.clone()),
                        Item::File(_) => unreachable!("chain outputs are in memory"),
                    };
                    kept.push(write_output(&block_dir, job_dir, index, output)?);
                }
                items.push(item);
            }
            if !block.tests.is_empty() {
                tests.extend(run_tests(&step.block_uuid, &block.tests, &subject)?);
            }
        }
        blocks.push(ChainBlockRecord {
            block_uuid: step.block_uuid.clone(),
            seconds: started.elapsed().as_secs_f64(),
            outputs: kept,
            planned,
        });
    }
    write_json_atomic(
        &job_dir.join(RESULT_FILE),
        &ChainResult {
            api_version: API_VERSION,
            outputs: final_outputs,
            tests,
            blocks,
        },
    )
}

// ---- resident jobs ----------------------------------------------------------------------

/// The line a resident process prints on stdout and on stderr after each job: every line
/// before it on each stream is that job's.
pub const JOB_DONE: &str = "\u{1e}mage-job-done";

/// A boxed block function, run as a block of its own.
struct Boxed(BlockCall);

/// The `Args` of [`Boxed`]: no block function's argument tuple.
pub struct BoxedArgs;

impl BlockFn<BoxedArgs> for Boxed {
    fn call(self, inputs: &mut Inputs<'_>) -> anyhow::Result<Vec<Output>> {
        (self.0)(inputs)
    }
}

#[derive(serde::Deserialize)]
struct JobRequest {
    job_dir: PathBuf,
    /// The block to run on its own; without one, the chain job.json lists.
    #[serde(default)]
    block: Option<String>,
}

/// The entry point of a service's resident block program (MAGE_RESIDENT=1): one process
/// runs the jobs of many runs, so what a block loads once (a model session in a static, a
/// connection pool) stays loaded between runs. Each line on stdin is a job
/// (`{"job_dir": ..., "block": <entry> or null for a chain}`), run as the program would
/// run it on its own, with its result or error in the job directory; then [`JOB_DONE`] on
/// stdout and stderr. After a panic the process reports it and exits, as its state may be
/// broken; the service starts another one for the next job.
pub fn serve_jobs(entries: ChainEntries) -> ! {
    use std::io::{BufRead, Write};

    install_panic_hook();
    exit_with_parent();
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        let request: JobRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("A resident block program got a request it cannot read: {error}");
                std::process::exit(2);
            }
        };
        if let Ok(mut slot) = PANIC.lock() {
            *slot = None;
        }
        if let Ok(mut slot) = CURRENT_BLOCK.lock() {
            *slot = None;
        }
        let chained = request.block.is_none();
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| match &request.block {
            Some(entry) => {
                let make = entries
                    .iter()
                    .find(|(name, _)| name == entry)
                    .map(|(_, make)| *make)
                    .ok_or_else(|| anyhow!("This program has no block {entry}"))?;
                let block = make();
                run_job(Boxed(block.call), block.tests, &request.job_dir)
            }
            None => run_chain_job(&entries, &request.job_dir),
        }));
        let panicked = outcome.is_err();
        let record = match outcome {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(ErrorRecord {
                kind: "error",
                message: describe(&error),
                location: None,
                block_uuid: if chained { current_block() } else { None },
            }),
            Err(_) => Some(
                PANIC
                    .lock()
                    .ok()
                    .and_then(|mut slot| slot.take())
                    .unwrap_or(ErrorRecord {
                        kind: "panic",
                        message: "The block panicked".to_string(),
                        location: None,
                        block_uuid: if chained { current_block() } else { None },
                    }),
            ),
        };
        if let Some(record) = record
            && let Err(error) = write_json_atomic(&request.job_dir.join(ERROR_FILE), &record)
        {
            eprintln!("Could not report the error to Mage: {error}");
        }
        let _ = std::io::stdout().flush();
        eprintln!("{JOB_DONE}");
        println!("{JOB_DONE}");
        let _ = std::io::stdout().flush();
        if panicked {
            std::process::exit(1);
        }
    }
    std::process::exit(0)
}

/// A computed table: the next block takes it as a plan over the data in memory.
fn materialized(frame: DataFrame, subject: &mut TestSubject) -> Item {
    if subject.frame.is_none() {
        subject.frame = Some(frame.clone());
    }
    Item::Lazy(Box::new(frame.lazy()))
}
