//! The one planner of MageML (I-18): which blocks of a pipeline run together. MageML's
//! scheduler and `mage run`, the editor's Compile panel and exported services all ask it,
//! so a pipeline is never chained one way in one place and another way elsewhere.
//!
//! A caller describes each block with [`BlockFacts`]: what the planner cannot know, such as
//! why a block must run alone or must write its own output, comes in as a reason. The
//! planner returns
//!
//! - **groups**: blocks that run one after another in one job. In MageML a group is a
//!   stage, one process; in a service it is a chain, one worker request or one Rust binary;
//! - **segments**: blocks of a group compiled into one unit, a Rust or R plan or SQL views,
//!   that only the last block of runs;
//! - **findings**: each place where a block and the next could chain but do not, with the
//!   reason, for the Compile panel to show with its fix.
//!
//! A group or segment is a straight line: each block is the only downstream block of the
//! one before, and has no other upstream block, so nothing else needs the outputs in
//! between. Blocks are planned in topological order; the first block of a line starts it.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Python,
    R,
    Rust,
    Sql,
    /// Languages the planner never chains (YAML, window blocks, markdown).
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    /// Runs scheduled by MageML: groups are stages, in which any language may follow any
    /// other; segments are Rust and R plans and SQL views.
    Mageml,
    /// Exported services: groups are chains of one runtime, Rust blocks in one binary or
    /// Python, R and SQL blocks of one Python worker in one request.
    Service,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BlockFacts {
    pub uuid: String,
    pub language: Option<Language>,
    #[serde(default)]
    pub upstream: Vec<String>,
    /// The named Python worker the block runs in; None for the default one.
    #[serde(default)]
    pub worker: Option<String>,
    /// The connection profile the block uses (io_config profile of a Rust block, the data
    /// provider profile of a SQL block).
    #[serde(default)]
    pub profile: Option<String>,
    /// The database of a SQL block (duckdb, postgres, clickhouse...).
    #[serde(default)]
    pub provider: Option<String>,
    /// Why the block must run on its own, outside any group, for this target; None when it
    /// may join one.
    #[serde(default)]
    pub alone: Option<String>,
    /// Why something needs the block's own output (an exporter, tests, a contract,
    /// monitors), so no segment continues after it; None when nothing does.
    #[serde(default)]
    pub output_needed: Option<String>,
    /// Why a SQL block cannot become a view; None when it can.
    #[serde(default)]
    pub view_problem: Option<String>,
    /// For a service: the Rust block was compiled into the service's binary.
    #[serde(default)]
    pub compiled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Options {
    pub target: Target,
    /// Groups at all (MageML: block fusion on; services: MAGE_SERVICE_CHAINS not off).
    #[serde(default = "yes")]
    pub groups: bool,
    /// Segments (MageML: MAGE_COMPILED_CHAINS not off).
    #[serde(default = "yes")]
    pub segments: bool,
    /// Rust chains in services (MAGE_SERVICE_RUST_CHAINS not off).
    #[serde(default = "yes")]
    pub rust_chains: bool,
}

fn yes() -> bool {
    true
}

impl Options {
    pub fn new(target: Target) -> Self {
        Options {
            target,
            groups: true,
            segments: true,
            rust_chains: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentKind {
    Rust,
    R,
    Sql,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Segment {
    pub kind: SegmentKind,
    pub blocks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Group,
    Segment,
}

/// A block and the next block of its line that do not chain, and why.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub level: Level,
    pub block: String,
    pub next: String,
    pub code: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    /// Every block in exactly one group, in topological order; a block that runs alone is
    /// a group of one.
    pub groups: Vec<Vec<String>>,
    /// Segments of two blocks or more.
    pub segments: Vec<Segment>,
    pub findings: Vec<Finding>,
    /// Blocks in topological order.
    pub order: Vec<String>,
}

impl Plan {
    /// The block after this one in its group, if any.
    pub fn next_in_group(&self, uuid: &str) -> Option<&str> {
        self.groups.iter().find_map(|group| {
            let position = group.iter().position(|b| b == uuid)?;
            group.get(position + 1).map(String::as_str)
        })
    }

    /// The segment the block belongs to, if any.
    pub fn segment_of(&self, uuid: &str) -> Option<&Segment> {
        self.segments
            .iter()
            .find(|s| s.blocks.iter().any(|b| b == uuid))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("Block {0} is listed twice.")]
    Duplicate(String),
    #[error("The blocks form a cycle through {0}.")]
    Cycle(String),
}

/// Why `block` and `next` do not chain; None when they do. `first` is the line's first
/// block, `profile` the connection profile the line uses so far.
type Rule<'a> = dyn Fn(&BlockFacts, &BlockFacts, &BlockFacts, &Option<String>) -> Option<(&'static str, String)>
    + 'a;

pub fn plan(blocks: &[BlockFacts], options: &Options) -> Result<Plan, PlanError> {
    let order = topological(blocks)?;
    let by_uuid: HashMap<&str, &BlockFacts> = blocks.iter().map(|b| (b.uuid.as_str(), b)).collect();
    let ordered: Vec<&BlockFacts> = order.iter().map(|u| by_uuid[u.as_str()]).collect();
    let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
    for block in blocks {
        for up in &block.upstream {
            if by_uuid.contains_key(up.as_str()) {
                downstream
                    .entry(up.as_str())
                    .or_default()
                    .push(block.uuid.as_str());
            }
        }
    }
    let mut findings = Vec::new();

    let group_rule: Box<Rule> = match options.target {
        Target::Mageml => {
            Box::new(|_first, block, next, _profile| alone(block).or_else(|| alone(next)))
        }
        Target::Service => Box::new(|first, block, next, profile| {
            if let Some(found) = alone(block).or_else(|| alone(next)) {
                return Some(found);
            }
            service_rule(first, next, profile, options)
        }),
    };
    let groups = if options.groups {
        lines(
            &ordered,
            &downstream,
            &by_uuid,
            Level::Group,
            &group_rule,
            &mut findings,
            |b| options.target == Target::Mageml || service_kind(b, options).is_some(),
        )
    } else {
        ordered.iter().map(|b| vec![b.uuid.clone()]).collect()
    };
    let mut groups = groups;
    // Blocks no line took run alone.
    let placed: std::collections::HashSet<&str> =
        groups.iter().flatten().map(String::as_str).collect();
    let alone_blocks: Vec<Vec<String>> = ordered
        .iter()
        .filter(|b| !placed.contains(b.uuid.as_str()))
        .map(|b| vec![b.uuid.clone()])
        .collect();
    groups.extend(alone_blocks);
    let position: HashMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, u)| (u.as_str(), i))
        .collect();
    groups.sort_by_key(|g| position[g[0].as_str()]);

    let mut segments = Vec::new();
    if options.target == Target::Mageml && options.groups && options.segments {
        let segment_rule: Box<Rule> = Box::new(segment_rule);
        for group in &groups {
            let members: Vec<&BlockFacts> = group.iter().map(|u| by_uuid[u.as_str()]).collect();
            // Within a group each block's only downstream block is the next one.
            let mut next_of: HashMap<&str, Vec<&str>> = HashMap::new();
            for pair in members.windows(2) {
                next_of.insert(pair[0].uuid.as_str(), vec![pair[1].uuid.as_str()]);
            }
            for line in lines(
                &members,
                &next_of,
                &by_uuid,
                Level::Segment,
                &segment_rule,
                &mut findings,
                |b| segment_kind(b).is_some(),
            ) {
                if line.len() > 1 {
                    let kind = segment_kind(by_uuid[line[0].as_str()]).expect("segment block");
                    segments.push(Segment { kind, blocks: line });
                }
            }
        }
    }
    Ok(Plan {
        groups,
        segments,
        findings,
        order,
    })
}

fn alone(block: &BlockFacts) -> Option<(&'static str, String)> {
    block.alone.as_ref().map(|reason| {
        (
            "runs_alone",
            format!("{} runs on its own: {reason}", block.uuid),
        )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceKind {
    Rust,
    Worker,
}

fn service_kind(block: &BlockFacts, options: &Options) -> Option<ServiceKind> {
    match block.language? {
        Language::Rust if block.compiled && options.rust_chains => Some(ServiceKind::Rust),
        Language::Python | Language::R | Language::Sql => Some(ServiceKind::Worker),
        _ => None,
    }
}

fn service_rule(
    first: &BlockFacts,
    next: &BlockFacts,
    profile: &Option<String>,
    options: &Options,
) -> Option<(&'static str, String)> {
    let kind = service_kind(first, options)?;
    if service_kind(next, options) != Some(kind) {
        return Some((
            "runtime_switch",
            format!(
                "{} runs in {} and {} in {}: the data moves between them.",
                first.uuid,
                runtime_name(first),
                next.uuid,
                runtime_name(next)
            ),
        ));
    }
    if kind == ServiceKind::Worker && next.worker != first.worker {
        return Some(("worker_switch", worker_switch(first, next)));
    }
    if kind == ServiceKind::Rust
        && let (Some(a), Some(b)) = (profile, &next.profile)
        && a != b
    {
        return Some(("profile_switch", profile_switch(next, a, b)));
    }
    None
}

fn segment_kind(block: &BlockFacts) -> Option<SegmentKind> {
    match block.language? {
        Language::Rust => Some(SegmentKind::Rust),
        Language::R => Some(SegmentKind::R),
        Language::Sql => Some(SegmentKind::Sql),
        _ => None,
    }
}

fn segment_rule(
    first: &BlockFacts,
    block: &BlockFacts,
    next: &BlockFacts,
    profile: &Option<String>,
) -> Option<(&'static str, String)> {
    let kind = segment_kind(first)?;
    if next.language != first.language {
        return Some((
            "language_switch",
            format!(
                "{} is {} and {} is {}: each compiles on its own.",
                block.uuid,
                language_name(block.language),
                next.uuid,
                language_name(next.language)
            ),
        ));
    }
    // Every block of a SQL chain but the last becomes a view; the last reads the chain and
    // writes its own table, so it may append.
    if kind == SegmentKind::Sql
        && let Some(problem) = &block.view_problem
    {
        return Some((
            "not_a_view",
            format!("{} cannot become a view: {problem}", block.uuid),
        ));
    }
    if let Some(reason) = &block.output_needed {
        return Some((
            "output_needed",
            format!(
                "{}'s own output is needed ({reason}), so it runs instead of passing a plan on.",
                block.uuid
            ),
        ));
    }
    match kind {
        SegmentKind::Rust => {
            if let (Some(a), Some(b)) = (profile, &next.profile)
                && a != b
            {
                return Some(("profile_switch", profile_switch(next, a, b)));
            }
        }
        SegmentKind::R => {}
        SegmentKind::Sql => {
            if next.provider != first.provider {
                return Some((
                    "database_switch",
                    format!(
                        "{} runs on another database than {}.",
                        next.uuid, block.uuid
                    ),
                ));
            }
            if next.profile != first.profile {
                return Some((
                    "profile_switch",
                    format!(
                        "{} uses another connection profile than {}.",
                        next.uuid, block.uuid
                    ),
                ));
            }
        }
    }
    None
}

fn worker_switch(block: &BlockFacts, next: &BlockFacts) -> String {
    let name = |w: &Option<String>| w.clone().unwrap_or_else(|| "the default worker".into());
    format!(
        "{} runs in {} and {} in {}: the data moves between two processes.",
        block.uuid,
        name(&block.worker),
        next.uuid,
        name(&next.worker)
    )
}

fn profile_switch(next: &BlockFacts, a: &str, b: &str) -> String {
    format!(
        "{} uses connection profile {b}, the blocks before it {a}.",
        next.uuid
    )
}

fn runtime_name(block: &BlockFacts) -> String {
    match block.language {
        Some(Language::Rust) if block.compiled => "the service's Rust binary".into(),
        Some(Language::Rust) => "a Rust program of its own".into(),
        Some(Language::Python | Language::R | Language::Sql) => match &block.worker {
            Some(w) => format!("Python worker {w}"),
            None => "the Python worker".into(),
        },
        _ => "its own runtime".into(),
    }
}

fn language_name(language: Option<Language>) -> &'static str {
    match language {
        Some(Language::Python) => "Python",
        Some(Language::R) => "R",
        Some(Language::Rust) => "Rust",
        Some(Language::Sql) => "SQL",
        _ => "another language",
    }
}

/// Straight lines through `blocks` (in topological order) where `rule` lets each block be
/// followed by the next. Blocks `eligible` rejects belong to no line; a line of one block
/// is returned too. Each refusal between a block and its only possible next block is a
/// finding.
fn lines(
    blocks: &[&BlockFacts],
    downstream: &HashMap<&str, Vec<&str>>,
    by_uuid: &HashMap<&str, &BlockFacts>,
    level: Level,
    rule: &Rule,
    findings: &mut Vec<Finding>,
    eligible: impl Fn(&BlockFacts) -> bool,
) -> Vec<Vec<String>> {
    let mut assigned: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut result = Vec::new();
    for &block in blocks {
        if assigned.contains(block.uuid.as_str()) || !eligible(block) {
            continue;
        }
        let first = block;
        let mut line = vec![block.uuid.clone()];
        assigned.insert(block.uuid.as_str());
        let mut profile = block.profile.clone();
        let mut last = block;
        // The block after `last` in a straight line: its only downstream block, when that
        // block has no other upstream block.
        let only_next = |last: &BlockFacts| -> Option<&BlockFacts> {
            let next = match downstream.get(last.uuid.as_str()).map(Vec::as_slice) {
                Some([only]) => by_uuid.get(only).copied()?,
                _ => return None,
            };
            let upstream = next
                .upstream
                .iter()
                .filter(|u| by_uuid.contains_key(u.as_str()));
            (upstream.count() == 1).then_some(next)
        };
        while let Some(next) = only_next(last) {
            let refused = rule(first, last, next, &profile);
            if let Some((code, reason)) = &refused {
                findings.push(Finding {
                    level,
                    block: last.uuid.clone(),
                    next: next.uuid.clone(),
                    code: code.to_string(),
                    reason: reason.clone(),
                });
            }
            if refused.is_some() || !eligible(next) {
                break;
            }
            if profile.is_none() {
                profile = next.profile.clone();
            }
            assigned.insert(next.uuid.as_str());
            line.push(next.uuid.clone());
            last = next;
        }
        result.push(line);
    }
    result
}

/// Kahn's order; among blocks ready together, the order they were given in.
fn topological(blocks: &[BlockFacts]) -> Result<Vec<String>, PlanError> {
    let mut index = HashMap::new();
    for (i, block) in blocks.iter().enumerate() {
        if index.insert(block.uuid.as_str(), i).is_some() {
            return Err(PlanError::Duplicate(block.uuid.clone()));
        }
    }
    let mut pending: Vec<usize> = blocks
        .iter()
        .map(|b| {
            b.upstream
                .iter()
                .filter(|u| index.contains_key(u.as_str()))
                .count()
        })
        .collect();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); blocks.len()];
    for (i, block) in blocks.iter().enumerate() {
        for up in &block.upstream {
            if let Some(&u) = index.get(up.as_str()) {
                children[u].push(i);
            }
        }
    }
    let mut ready: BTreeMap<usize, ()> = pending
        .iter()
        .enumerate()
        .filter(|(_, n)| **n == 0)
        .map(|(i, _)| (i, ()))
        .collect();
    let mut order = Vec::with_capacity(blocks.len());
    while let Some((&i, _)) = ready.iter().next() {
        ready.remove(&i);
        order.push(blocks[i].uuid.clone());
        for &child in &children[i] {
            pending[child] -= 1;
            if pending[child] == 0 {
                ready.insert(child, ());
            }
        }
    }
    if order.len() != blocks.len() {
        let stuck = blocks
            .iter()
            .enumerate()
            .find(|(i, _)| pending[*i] > 0)
            .map(|(_, b)| b.uuid.clone());
        return Err(PlanError::Cycle(stuck.unwrap_or_default()));
    }
    Ok(order)
}

/// The planner over JSON, for callers in other languages: `{"blocks": [...], "options":
/// {...}}` in, the [`Plan`] out.
pub fn plan_json(input: &str) -> Result<String, String> {
    #[derive(Deserialize)]
    struct Input {
        blocks: Vec<BlockFacts>,
        options: Options,
    }
    let input: Input =
        serde_json::from_str(input).map_err(|e| format!("Invalid plan input: {e}"))?;
    let plan = plan(&input.blocks, &input.options).map_err(|e| e.to_string())?;
    serde_json::to_string(&plan).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(uuid: &str, language: Language, upstream: &[&str]) -> BlockFacts {
        BlockFacts {
            uuid: uuid.into(),
            language: Some(language),
            upstream: upstream.iter().map(|u| u.to_string()).collect(),
            compiled: true,
            ..Default::default()
        }
    }

    fn mageml() -> Options {
        Options::new(Target::Mageml)
    }

    fn service() -> Options {
        Options::new(Target::Service)
    }

    fn groups(plan: &Plan) -> Vec<Vec<&str>> {
        plan.groups
            .iter()
            .map(|g| g.iter().map(String::as_str).collect())
            .collect()
    }

    #[test]
    fn a_straight_line_of_any_languages_is_one_stage_in_mageml() {
        let blocks = [
            block("load", Language::Python, &[]),
            block("clean", Language::Rust, &["load"]),
            block("model", Language::R, &["clean"]),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(groups(&plan), vec![vec!["load", "clean", "model"]]);
        assert!(plan.segments.is_empty());
    }

    #[test]
    fn fan_out_and_fan_in_end_lines() {
        // a -> b, a -> c, b + c -> d
        let blocks = [
            block("a", Language::Python, &[]),
            block("b", Language::Python, &["a"]),
            block("c", Language::Python, &["a"]),
            block("d", Language::Python, &["b", "c"]),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(
            groups(&plan),
            vec![vec!["a"], vec!["b"], vec!["c"], vec!["d"]]
        );
        assert!(
            plan.findings.is_empty(),
            "structure is not advice: {:?}",
            plan.findings
        );
    }

    #[test]
    fn a_block_that_runs_alone_splits_the_stage_and_says_why() {
        let mut middle = block("check", Language::Python, &["load"]);
        middle.alone = Some("it has a trigger rule".into());
        let blocks = [
            block("load", Language::Python, &[]),
            middle,
            block("save", Language::Python, &["check"]),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(
            groups(&plan),
            vec![vec!["load"], vec!["check"], vec!["save"]]
        );
        assert_eq!(plan.findings[0].code, "runs_alone");
        assert!(plan.findings[0].reason.contains("trigger rule"));
    }

    #[test]
    fn rust_blocks_compile_into_one_segment_until_an_output_is_needed() {
        let mut aggregate = block("aggregate", Language::Rust, &["enrich"]);
        aggregate.output_needed = Some("it has tests".into());
        let blocks = [
            block("load", Language::Rust, &[]),
            block("enrich", Language::Rust, &["load"]),
            aggregate,
            block("save", Language::Rust, &["aggregate"]),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(
            plan.segments,
            vec![Segment {
                kind: SegmentKind::Rust,
                blocks: vec!["load".into(), "enrich".into(), "aggregate".into()]
            },]
        );
        let finding = plan
            .findings
            .iter()
            .find(|f| f.level == Level::Segment)
            .unwrap();
        assert_eq!(
            (finding.block.as_str(), finding.code.as_str()),
            ("aggregate", "output_needed")
        );
    }

    #[test]
    fn rust_segments_need_one_connection_profile_in_mageml_as_in_services() {
        let mut a = block("a", Language::Rust, &[]);
        a.profile = Some("warehouse".into());
        let b = block("b", Language::Rust, &["a"]);
        let mut c = block("c", Language::Rust, &["b"]);
        c.profile = Some("lake".into());
        let blocks = [a, b, c];
        let mageml_plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(mageml_plan.segments[0].blocks, vec!["a", "b"]);
        let service_plan = plan(&blocks, &service()).unwrap();
        assert_eq!(groups(&service_plan), vec![vec!["a", "b"], vec!["c"]]);
        for found in [&mageml_plan.findings, &service_plan.findings] {
            assert!(
                found
                    .iter()
                    .any(|f| f.code == "profile_switch" && f.next == "c")
            );
        }
    }

    #[test]
    fn sql_views_need_one_database_and_profile_and_a_viewable_block() {
        let sql = |uuid: &str, up: &[&str], provider: &str| {
            let mut b = block(uuid, Language::Sql, up);
            b.provider = Some(provider.into());
            b.profile = Some("default".into());
            b
        };
        // raw cannot be a view, but it may end a chain: it reads the views before it and
        // writes its own table.
        let mut raw = sql("raw", &["b"], "duckdb");
        raw.view_problem = Some("It runs raw SQL.".into());
        let blocks = [
            sql("a", &[], "duckdb"),
            sql("b", &["a"], "duckdb"),
            raw,
            sql("d", &["raw"], "duckdb"),
            sql("e", &["d"], "postgres"),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(
            plan.segments,
            vec![Segment {
                kind: SegmentKind::Sql,
                blocks: vec!["a".into(), "b".into(), "raw".into()]
            }]
        );
        let codes: Vec<(&str, &str)> = plan
            .findings
            .iter()
            .map(|f| (f.block.as_str(), f.code.as_str()))
            .collect();
        assert_eq!(codes, vec![("raw", "not_a_view"), ("d", "database_switch")]);
    }

    #[test]
    fn services_chain_one_runtime_and_one_python_worker() {
        let mut feast = block("features", Language::Python, &["load"]);
        feast.worker = Some("feast".into());
        let blocks = [
            block("load", Language::Python, &[]),
            feast,
            block("score", Language::Rust, &["features"]),
            block("save", Language::Rust, &["score"]),
        ];
        let plan = plan(&blocks, &service()).unwrap();
        assert_eq!(
            groups(&plan),
            vec![vec!["load"], vec!["features"], vec!["score", "save"]]
        );
        let codes: Vec<&str> = plan.findings.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(codes, vec!["worker_switch", "runtime_switch"]);
    }

    #[test]
    fn options_turn_groups_and_rust_chains_off() {
        let blocks = [
            block("a", Language::Rust, &[]),
            block("b", Language::Rust, &["a"]),
        ];
        let mut off = service();
        off.rust_chains = false;
        assert_eq!(
            groups(&plan(&blocks, &off).unwrap()),
            vec![vec!["a"], vec!["b"]]
        );
        let mut no_groups = mageml();
        no_groups.groups = false;
        let plan = plan(&blocks, &no_groups).unwrap();
        assert_eq!(groups(&plan), vec![vec!["a"], vec!["b"]]);
        assert!(plan.segments.is_empty());
    }

    #[test]
    fn order_is_topological_whatever_the_input_order() {
        let blocks = [
            block("c", Language::Python, &["b"]),
            block("b", Language::Python, &["a"]),
            block("a", Language::Python, &[]),
        ];
        let plan = plan(&blocks, &mageml()).unwrap();
        assert_eq!(plan.order, vec!["a", "b", "c"]);
        assert_eq!(groups(&plan), vec![vec!["a", "b", "c"]]);
        assert_eq!(plan.next_in_group("b"), Some("c"));
    }

    #[test]
    fn cycles_and_duplicates_are_errors() {
        let blocks = [
            block("a", Language::Python, &["b"]),
            block("b", Language::Python, &["a"]),
        ];
        assert!(matches!(plan(&blocks, &mageml()), Err(PlanError::Cycle(_))));
        let blocks = [
            block("a", Language::Python, &[]),
            block("a", Language::Python, &[]),
        ];
        assert_eq!(
            plan(&blocks, &mageml()),
            Err(PlanError::Duplicate("a".into()))
        );
    }

    #[test]
    fn upstream_blocks_outside_the_plan_are_ignored() {
        // A sensor or a block of another pipeline is not planned.
        let blocks = [
            block("a", Language::Python, &["sensor"]),
            block("b", Language::Python, &["a"]),
        ];
        assert_eq!(
            groups(&plan(&blocks, &mageml()).unwrap()),
            vec![vec!["a", "b"]]
        );
    }

    #[test]
    fn json_round_trip() {
        let out = plan_json(r#"{"blocks": [{"uuid": "a", "language": "rust"}, {"uuid": "b", "language": "rust", "upstream": ["a"]}], "options": {"target": "mageml"}}"#).unwrap();
        let plan: Plan = serde_json::from_str(&out).unwrap();
        assert_eq!(plan.segments[0].blocks, vec!["a", "b"]);
        assert!(
            plan_json("{}")
                .unwrap_err()
                .starts_with("Invalid plan input")
        );
    }
}
