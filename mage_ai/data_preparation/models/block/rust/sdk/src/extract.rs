//! Block function parameters. Each parameter type takes its value from the run: tables
//! and JSON values take the next upstream output, in the order of the upstream blocks;
//! `Vars` and `BlockContext` take no input.

use std::path::Path;

use crate::context::{BlockContext, Vars};
use crate::prelude::*;
use crate::protocol::{FrameFormat, Input};

/// One upstream output: a file Mage wrote, or, inside a chain of Rust blocks run as one
/// process, the previous block's output in memory.
#[derive(Clone)]
pub(crate) enum Item {
    File(Input),
    /// A table, still a lazy plan: the next block's query extends it, and Polars
    /// optimizes the blocks' queries together.
    Lazy(Box<LazyFrame>),
    Json(Value),
}

pub struct Inputs<'a> {
    pub(crate) context: &'a BlockContext,
    pub(crate) job_dir: &'a Path,
    pub(crate) items: std::vec::IntoIter<Item>,
    /// The number of upstream outputs taken so far, for error messages.
    pub(crate) taken: usize,
}

impl Inputs<'_> {
    fn next(&mut self, wanted: &str) -> anyhow::Result<(usize, Item)> {
        let index = self.taken;
        match self.items.next() {
            Some(input) => {
                self.taken += 1;
                Ok((index, input))
            }
            None => bail!(
                "The block function takes {wanted} as upstream output {}, but the block has \
                 {index} upstream output{}",
                index + 1,
                if index == 1 { "" } else { "s" },
            ),
        }
    }

    fn remaining(&self) -> usize {
        self.items.len()
    }
}

fn scan(job_dir: &Path, format: FrameFormat, path: &Path) -> anyhow::Result<LazyFrame> {
    let path = job_dir.join(path);
    let path = PlRefPath::try_from_path(&path)?;
    Ok(match format {
        FrameFormat::Ipc => LazyFrame::scan_ipc(path, Default::default(), Default::default())?,
        FrameFormat::Parquet => LazyFrame::scan_parquet(path, Default::default())?,
    })
}

/// A value that a block function takes as a parameter.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a parameter of a Mage block function",
    label = "Mage cannot pass this",
    note = "parameters can be LazyFrame or DataFrame (the next upstream table), Option<LazyFrame>, Vec<LazyFrame> (all remaining tables), serde_json::Value (a dict or list output), Vars (pipeline variables) or BlockContext"
)]
pub trait FromInput: Sized {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self>;
}

/// The next upstream table, read lazily: Polars reads only the columns and rows the
/// block's query uses.
impl FromInput for LazyFrame {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        let (index, item) = inputs.next("a table")?;
        match item {
            Item::File(Input::Frame { format, path }) => scan(inputs.job_dir, format, &path),
            Item::Lazy(frame) => Ok(*frame),
            Item::File(Input::Json { .. }) | Item::Json(_) => bail!(
                "Upstream output {} is not a table; take it as serde_json::Value",
                index + 1
            ),
            Item::File(Input::Empty) => bail!(
                "Upstream output {} is empty; take it as Option<LazyFrame>",
                index + 1
            ),
        }
    }
}

/// The next upstream table, read into memory.
impl FromInput for DataFrame {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        let position = inputs.taken + 1;
        LazyFrame::from_input(inputs)?
            .collect()
            .with_context(|| format!("Reading upstream output {position}"))
    }
}

/// The next upstream output, or None when the upstream block returned nothing.
impl<T: FromInput> FromInput for Option<T> {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        if inputs.remaining() == 0 {
            return Ok(None);
        }
        let mut peek = inputs.items.clone();
        if matches!(peek.next(), Some(Item::File(Input::Empty))) {
            inputs.items.next();
            inputs.taken += 1;
            return Ok(None);
        }
        T::from_input(inputs).map(Some)
    }
}

/// Every remaining upstream table, for blocks with any number of upstream blocks.
impl FromInput for Vec<LazyFrame> {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        let mut frames = Vec::with_capacity(inputs.remaining());
        while inputs.remaining() > 0 {
            frames.push(LazyFrame::from_input(inputs)?);
        }
        Ok(frames)
    }
}

impl FromInput for Vec<DataFrame> {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        Vec::<LazyFrame>::from_input(inputs)?
            .into_iter()
            .map(|frame| frame.collect().map_err(Into::into))
            .collect()
    }
}

/// The next upstream output that is not a table, such as a dict or a list.
impl FromInput for Value {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        let (index, item) = inputs.next("a JSON value")?;
        match item {
            Item::File(Input::Json { path }) => {
                let bytes = std::fs::read(inputs.job_dir.join(path))?;
                Ok(serde_json::from_slice(&bytes)?)
            }
            Item::Json(value) => Ok(value),
            Item::File(Input::Empty) => Ok(Value::Null),
            Item::File(Input::Frame { .. }) | Item::Lazy(_) => Err(anyhow!(
                "Upstream output {} is a table; take it as LazyFrame or DataFrame",
                index + 1
            )),
        }
    }
}

impl FromInput for Vars {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        Ok(inputs.context.variables.clone())
    }
}

impl FromInput for BlockContext {
    fn from_input(inputs: &mut Inputs<'_>) -> anyhow::Result<Self> {
        Ok(inputs.context.clone())
    }
}
