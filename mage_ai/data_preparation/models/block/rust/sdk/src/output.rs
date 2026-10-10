//! Block function return values. Tables become Mage DataFrame outputs, JSON values
//! become dict or list outputs, a bool is a conditional block's decision and `()` is no
//! output. A `Result` fails the block with its error.

use std::path::Path;

use crate::prelude::*;
use serde_json::Value;

use crate::protocol::{FrameFormat, OutputRecord};

pub enum Output {
    Lazy(Box<LazyFrame>),
    Frame(DataFrame),
    Json(Value),
    Decision(bool),
}

#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be returned by a Mage block function",
    label = "Mage cannot store this",
    note = "a block function returns LazyFrame, DataFrame, serde_json::Value, bool (conditional blocks) or (), a tuple or Vec of tables, an Option of these, or a Result of any of them"
)]
pub trait IntoOutputs {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>>;
}

impl IntoOutputs for () {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(Vec::new())
    }
}

impl IntoOutputs for LazyFrame {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(vec![Output::Lazy(Box::new(self))])
    }
}

impl IntoOutputs for DataFrame {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(vec![Output::Frame(self)])
    }
}

impl IntoOutputs for Value {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(vec![Output::Json(self)])
    }
}

impl IntoOutputs for bool {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(vec![Output::Decision(self)])
    }
}

impl<T: IntoOutputs> IntoOutputs for Option<T> {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        match self {
            Some(value) => value.into_outputs(),
            None => Ok(Vec::new()),
        }
    }
}

impl<T: IntoOutputs, E: Into<anyhow::Error>> IntoOutputs for Result<T, E> {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        self.map_err(Into::into)?.into_outputs()
    }
}

/// Several outputs: output_0, output_1, ... in order.
impl IntoOutputs for Vec<DataFrame> {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(self.into_iter().map(Output::Frame).collect())
    }
}

impl IntoOutputs for Vec<LazyFrame> {
    fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
        Ok(self
            .into_iter()
            .map(|frame| Output::Lazy(Box::new(frame)))
            .collect())
    }
}

macro_rules! tuple_outputs {
    ($($name:ident),+) => {
        impl<$($name: IntoOutputs),+> IntoOutputs for ($($name,)+) {
            #[allow(non_snake_case)]
            fn into_outputs(self) -> anyhow::Result<Vec<Output>> {
                let ($($name,)+) = self;
                let mut outputs = Vec::new();
                $(outputs.extend($name.into_outputs()?);)+
                Ok(outputs)
            }
        }
    };
}

tuple_outputs!(A, B);
tuple_outputs!(A, B, C);
tuple_outputs!(A, B, C, D);

fn ipc_options() -> IpcWriterOptions {
    // Uncompressed: Mage reads the file once, right after the block exits.
    IpcWriterOptions {
        compression: None,
        ..Default::default()
    }
}

/// Writes one output to `dir/output_<index>.<extension>` and describes it.
pub(crate) fn write_output(
    dir: &Path,
    job_dir: &Path,
    index: usize,
    output: Output,
) -> anyhow::Result<OutputRecord> {
    let relative = |name: &str| -> anyhow::Result<String> {
        let path = dir.join(name);
        Ok(path
            .strip_prefix(job_dir)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned())
    };
    match output {
        Output::Lazy(frame) => {
            let frame = *frame;
            let name = format!("output_{index}.arrow");
            let path = dir.join(&name);
            // The streaming engine writes the table as it computes it, without holding
            // all of it in memory when the query allows.
            frame
                .sink(
                    SinkDestination::File {
                        target: SinkTarget::Path(PlRefPath::try_from_path(&path)?),
                    },
                    FileWriteFormat::Ipc(ipc_options()),
                    UnifiedSinkArgs::default(),
                )?
                .collect_with_engine(Engine::Streaming)
                .with_context(|| format!("Computing output {index}"))?;
            Ok(OutputRecord::Frame {
                format: FrameFormat::Ipc,
                path: relative(&name)?,
                rows: None,
            })
        }
        Output::Frame(mut frame) => {
            let name = format!("output_{index}.arrow");
            let file = std::fs::File::create(dir.join(&name))?;
            let options = ipc_options();
            IpcWriter::new(file)
                .with_compression(options.compression)
                .finish(&mut frame)
                .with_context(|| format!("Writing output {index}"))?;
            Ok(OutputRecord::Frame {
                format: FrameFormat::Ipc,
                path: relative(&name)?,
                rows: Some(frame.height() as u64),
            })
        }
        Output::Json(value) => {
            let name = format!("output_{index}.json");
            std::fs::write(dir.join(&name), serde_json::to_vec(&value)?)?;
            Ok(OutputRecord::Json {
                path: relative(&name)?,
            })
        }
        Output::Decision(value) => Ok(OutputRecord::Decision { value }),
    }
}

/// The first table output read back, for the block's tests.
pub(crate) fn read_frame(path: &Path) -> anyhow::Result<DataFrame> {
    Ok(LazyFrame::scan_ipc(
        PlRefPath::try_from_path(path)?,
        Default::default(),
        Default::default(),
    )?
    .collect()?)
}
