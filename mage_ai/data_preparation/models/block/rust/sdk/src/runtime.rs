//! The generated `main` of a block binary calls [`main`]: it runs the block function on
//! the job, writes its outputs, runs its tests and reports errors and panics to Mage.

use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::prelude::*;

use crate::context::{BlockContext, Vars};
use crate::extract::{FromInput, Inputs};
use crate::output::{IntoOutputs, Output, read_frame, write_output};
use crate::protocol::{
    API_VERSION, ERROR_FILE, ErrorRecord, Job, OutputRecord, RESULT_FILE, RunResult, TestRecord,
    write_json_atomic,
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

static PANIC: Mutex<Option<ErrorRecord>> = Mutex::new(None);

fn install_panic_hook() {
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
            });
        }
    }));
}

/// The block binary exits when the Mage process that started it is gone, so a stopped
/// block run leaves no Rust process computing.
fn exit_with_parent() {
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
    };
    let upstream = job.inputs.len();
    let mut inputs = Inputs {
        context: &context,
        job_dir,
        items: job.inputs.into_iter(),
        taken: 0,
    };
    let outputs = block.call(&mut inputs)?;
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

    write_json_atomic(
        &job_dir.join(RESULT_FILE),
        &RunResult {
            api_version: API_VERSION,
            outputs: records,
            tests: results,
        },
    )
}

fn take_panic_message() -> String {
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
        },
        Err(_) => PANIC
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .unwrap_or(ErrorRecord {
                kind: "panic",
                message: "The block panicked".to_string(),
                location: None,
            }),
    };
    if let Err(error) = write_json_atomic(&job_dir.join(ERROR_FILE), &record) {
        eprintln!("Could not report the error to Mage: {error}");
    }
    std::process::exit(1);
}
