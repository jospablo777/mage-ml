use mage::prelude::*;
use rayon::prelude::*;

/// An engagement score per session, computed row by row on every core with Rayon: the
/// kind of logic that Polars expressions do not express and Python runs slowly.
fn transform(sessions: DataFrame) -> Result<DataFrame> {
    let events = sessions.column("events")?.i64()?;
    let pages = sessions.column("pages")?.i64()?;
    let duration = sessions.column("duration_ms")?.i64()?;
    let scroll = sessions.column("max_scroll_depth")?.f64()?;
    let purchased = sessions.column("purchased")?.bool()?;

    let rows: Vec<Session> = (0..sessions.height())
        .map(|i| Session {
            events: events.get(i).unwrap_or(0),
            pages: pages.get(i).unwrap_or(0),
            duration_ms: duration.get(i).unwrap_or(0),
            scroll: scroll.get(i).unwrap_or(0.0),
            purchased: purchased.get(i).unwrap_or(false),
        })
        .collect();
    let scores: Vec<f64> = rows.par_iter().map(score).collect();

    let mut output = sessions;
    output.with_column(Column::new("engagement".into(), scores))?;
    Ok(output)
}

struct Session {
    events: i64,
    pages: i64,
    duration_ms: i64,
    scroll: f64,
    purchased: bool,
}

/// Diminishing returns on time and events, a bonus for depth and for buying.
fn score(session: &Session) -> f64 {
    let minutes = session.duration_ms as f64 / 60_000.0;
    let time = (1.0 + minutes).ln();
    let breadth = (session.pages as f64).sqrt();
    let activity = (1.0 + session.events as f64).log2();
    let depth = session.scroll.clamp(0.0, 1.0);
    let bought = if session.purchased { 2.0 } else { 0.0 };
    ((time + breadth + activity) * (0.5 + depth) + bought) * 10.0
}

fn test_scores_are_positive(output: &DataFrame) -> Result<()> {
    let low = output.column("engagement")?.f64()?.min().unwrap_or(0.0);
    ensure!(low >= 0.0, "A score is negative: {low}");
    Ok(())
}
