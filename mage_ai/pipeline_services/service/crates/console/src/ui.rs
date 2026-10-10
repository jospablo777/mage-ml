use mage_service_core::protocol::{ControlCommand, PipelineSnapshot};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Cell, Chart, Clear, Dataset, GraphType, LineGauge, List,
        ListItem, ListState, Padding, Paragraph, Row, Table, TableState, Wrap,
    },
};

use crate::{App, Mode, clean, unix_ms};

const CANVAS: Color = Color::Rgb(24, 24, 28);
const PANEL: Color = Color::Rgb(35, 36, 41);
const BORDER: Color = Color::Rgb(68, 68, 79);
const TEXT: Color = Color::Rgb(249, 250, 252);
const MUTED: Color = Color::Rgb(180, 184, 192);
const PURPLE: Color = Color::Rgb(149, 111, 255);
const LAVENDER: Color = Color::Rgb(196, 185, 239);
const RUST_ACCENT: Color = Color::Rgb(232, 139, 97);
const ICE: Color = Color::Rgb(149, 236, 226);
const GREEN: Color = Color::Rgb(47, 203, 82);
const WARNING: Color = Color::Rgb(255, 204, 25);
const ERROR: Color = Color::Rgb(255, 84, 125);
const SELECTED: Color = Color::Rgb(53, 43, 78);

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    frame.render_widget(Block::default().style(style(TEXT).bg(CANVAS)), area);
    if area.width < 60 || area.height < 18 {
        frame.render_widget(
            Paragraph::new("MageML / pipeline console\nResize to at least 60 x 18\nq quits")
                .block(panel(" Terminal size ")),
            area,
        );
        return;
    }
    let wide = area.width >= 110 && area.height >= 30;
    let content = area.inner(Margin::new(1, 0));
    let rows = Layout::vertical([
        Constraint::Length(if wide { 6 } else { 3 }),
        Constraint::Min(12),
        Constraint::Length(3),
    ])
    .split(content);
    draw_header(frame, rows[0], app, wide);
    if wide {
        let columns = Layout::horizontal([
            Constraint::Length(29),
            Constraint::Length(2),
            Constraint::Min(60),
        ])
        .split(rows[1]);
        draw_rail(frame, columns[0], app);
        draw_details(frame, columns[2], app);
    } else {
        draw_compact(frame, rows[1], app);
    }
    draw_footer(frame, rows[2], app);
    if app.help {
        draw_help(frame);
    }
    if app.pending.is_some() {
        draw_confirmation(frame, app);
    }
}

fn style(color: Color) -> Style {
    Style::default().fg(color)
}

fn strong(color: Color) -> Style {
    style(color).add_modifier(Modifier::BOLD)
}

fn panel(title: impl Into<Line<'static>>) -> Block<'static> {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(style(BORDER))
        .style(style(TEXT).bg(PANEL))
        .title_style(strong(MUTED))
        .title(title)
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App, wide: bool) {
    let service = app
        .snapshot
        .as_ref()
        .map_or("Connecting".to_owned(), |s| clean(&s.service_id, 80));
    let revision = app
        .snapshot
        .as_ref()
        .map_or("Awaiting snapshot".to_owned(), |s| clean(&s.revision, 80));
    let simulated = app.mode == Mode::Demo || revision.starts_with("demo/");
    let (state, state_color) = connection_state(app);
    if wide {
        draw_logo(frame, Rect::new(area.x + 1, area.y + 1, 12, 4));
        frame.render_widget(
            Paragraph::new(vec![
                Line::from("█▄ ▄█ █  "),
                Line::from("█ ▀ █ █  "),
                Line::from("▀   ▀ ▀▀▀"),
            ])
            .style(strong(RUST_ACCENT)),
            Rect::new(area.x + 14, area.y + 2, 9, 3),
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(vec![
                    Span::styled("Mage", strong(TEXT)),
                    Span::styled("ML", strong(RUST_ACCENT)),
                ]),
                Line::from(Span::styled("pipeline console", style(LAVENDER))),
                Line::from(Span::styled("", style(MUTED))),
            ]),
            Rect::new(area.x + 25, area.y + 2, 18, 3),
        );
        let columns = Layout::horizontal([Constraint::Min(30), Constraint::Length(32)]).split(
            Rect::new(area.x + 44, area.y + 1, area.width.saturating_sub(44), 4),
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(service, strong(TEXT))),
                Line::from(Span::styled(revision, style(MUTED))),
                Line::from(vec![
                    Span::styled(
                        if simulated {
                            "simulated workload"
                        } else {
                            "exported service"
                        },
                        style(if simulated { WARNING } else { LAVENDER }),
                    ),
                    Span::styled(
                        format!(
                            "  /  {} pipelines",
                            app.snapshot.as_ref().map_or(0, |s| s.pipelines.len())
                        ),
                        style(MUTED),
                    ),
                ]),
            ]),
            columns[0],
        );
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    format!(" {} {state} ", activity(app)),
                    strong(state_color),
                )),
                Line::from(Span::styled(sample_age(app), style(MUTED))),
                Line::from(Span::styled(control_label(app), style(LAVENDER))),
            ])
            .alignment(Alignment::Right),
            columns[1],
        );
    } else {
        let top = Rect::new(area.x, area.y, area.width, 1);
        let columns = Layout::horizontal([Constraint::Min(20), Constraint::Length(23)]).split(top);
        frame.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(" Mage ", strong(TEXT).bg(Color::Rgb(125, 85, 236))),
                Span::styled("ML", strong(RUST_ACCENT)),
                Span::styled(
                    format!(
                        "  {}",
                        fit_text(&service, columns[0].width.saturating_sub(10))
                    ),
                    strong(LAVENDER),
                ),
            ])),
            columns[0],
        );
        frame.render_widget(
            Paragraph::new(state)
                .style(strong(state_color))
                .right_aligned(),
            columns[1],
        );
        let bottom = Layout::horizontal([Constraint::Min(20), Constraint::Length(23)])
            .split(Rect::new(area.x, area.y + 1, area.width, 1));
        let description = if simulated {
            "simulated workload".to_owned()
        } else {
            sample_age(app)
        };
        frame.render_widget(
            Paragraph::new(description).style(style(if simulated { WARNING } else { MUTED })),
            bottom[0],
        );
        frame.render_widget(
            Paragraph::new(control_label(app))
                .style(style(LAVENDER))
                .right_aligned(),
            bottom[1],
        );
    }

    frame.render_widget(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(style(BORDER)),
        area,
    );
}

fn connection_state(app: &App) -> (&'static str, Color) {
    match app.mode {
        Mode::Demo => ("Demo fixture", WARNING),
        Mode::Snapshot => ("Saved snapshot", MUTED),
        Mode::Live if !app.connected => ("Disconnected", ERROR),
        Mode::Live if app.stale() => ("Stale / clock skew", WARNING),
        Mode::Live => ("Connected", ICE),
    }
}

fn control_label(app: &App) -> &'static str {
    if app.in_progress {
        "request pending"
    } else if app.retry_request.is_some() {
        "delivery unknown"
    } else if app.can_control() {
        "controls enabled"
    } else if app.allow_control {
        "controls unavailable"
    } else {
        "read only"
    }
}

fn sample_age(app: &App) -> String {
    if app.mode == Mode::Demo {
        return "fixed fixture values".into();
    }
    app.snapshot.as_ref().map_or("no sample".into(), |s| {
        if s.sampled_at_unix_ms > unix_ms().saturating_add(5_000) {
            "server clock ahead".into()
        } else {
            format!(
                "sample age {:.1}s",
                unix_ms().saturating_sub(s.sampled_at_unix_ms) as f64 / 1_000.0
            )
        }
    })
}

fn activity(app: &App) -> &'static str {
    if app.motion_active() {
        ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"][app.animation_phase() as usize % 8]
    } else {
        "●"
    }
}

fn draw_logo(frame: &mut Frame, area: Rect) {
    // Polygon coordinates follow the frontend's GradientLogo.tsx mark.
    const SHAPES: [&[(f64, f64)]; 3] = [
        &[
            (15.3266, 0.0),
            (19.2641, 0.0),
            (11.9687, 14.0),
            (8.03125, 14.0),
        ],
        &[
            (11.9692, 0.0),
            (8.03164, 0.0),
            (0.736328, 14.0),
            (4.67383, 14.0),
            (8.03164, 7.55626),
            (8.03164, 14.0),
            (11.9691, 14.0),
        ],
        &[
            (15.3269, 0.0),
            (19.2644, 0.0),
            (19.2644, 14.0),
            (15.3269, 14.0),
        ],
    ];
    let pixel = |x: u16, y: u16| {
        let point = (
            (f64::from(x) + 0.5) * 20.0 / f64::from(area.width),
            (f64::from(y) + 0.5) * 14.0 / f64::from(area.height * 2),
        );
        let shape = SHAPES
            .iter()
            .enumerate()
            .rev()
            .find(|(_, polygon)| inside(point, polygon));
        match shape {
            None => CANVAS,
            Some((layer, _)) => {
                let t = f64::from(x) / f64::from(area.width.max(1));
                let opacity = if layer == 0 { 0.4 } else { 1.0 };
                let blend = |a: f64, b: f64| (24.0 + ((a + (b - a) * t) - 24.0) * opacity) as u8;
                Color::Rgb(blend(125.0, 42.0), blend(85.0, 178.0), blend(236.0, 254.0))
            }
        }
    };
    let lines: Vec<Line> = (0..area.height)
        .map(|y| {
            Line::from(
                (0..area.width)
                    .map(|x| Span::styled("▀", style(pixel(x, y * 2)).bg(pixel(x, y * 2 + 1))))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), area);
}

fn inside((x, y): (f64, f64), polygon: &[(f64, f64)]) -> bool {
    let mut result = false;
    let mut previous = polygon.len() - 1;
    for current in 0..polygon.len() {
        let (xi, yi) = polygon[current];
        let (xj, yj) = polygon[previous];
        if ((yi > y) != (yj > y)) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
            result = !result;
        }
        previous = current;
    }
    result
}

fn draw_rail(frame: &mut Frame, area: Rect, app: &App) {
    let tall = area.height >= 30;
    let rows = Layout::vertical([
        Constraint::Min(12),
        Constraint::Length(1),
        Constraint::Length(8),
        Constraint::Length(if tall { 1 } else { 0 }),
        Constraint::Length(if tall { 7 } else { 0 }),
    ])
    .split(area);
    draw_pipelines(frame, rows[0], app);
    let Some(snapshot) = &app.snapshot else {
        return;
    };
    let active: u64 = snapshot
        .pipelines
        .iter()
        .map(|p| u64::from(p.in_flight))
        .sum();
    let queued: u64 = snapshot.pipelines.iter().map(|p| u64::from(p.queued)).sum();
    let paused = snapshot
        .pipelines
        .iter()
        .filter(|p| p.status == "paused")
        .count();
    let failures: u128 = snapshot
        .pipelines
        .iter()
        .map(|p| u128::from(p.runs_failed))
        .sum();
    frame.render_widget(
        Paragraph::new(vec![
            summary_line("Active runs", active.to_string(), ICE),
            summary_line(
                "Queued runs",
                queued.to_string(),
                if queued > 0 { WARNING } else { TEXT },
            ),
            summary_line("Paused pipelines", paused.to_string(), WARNING),
            Line::from(""),
            summary_line(
                "Lifetime failures",
                failures.to_string(),
                if failures > 0 { ERROR } else { MUTED },
            ),
            summary_line(
                "Embedded models",
                snapshot.models.len().to_string(),
                if snapshot.models.is_empty() {
                    MUTED
                } else {
                    LAVENDER
                },
            ),
        ])
        .block(panel(" Service totals ").padding(Padding::horizontal(1))),
        rows[2],
    );
    if !tall {
        return;
    }
    let mut lines = vec![
        Line::from(Span::styled(control_label(app), strong(LAVENDER))),
        Line::from(Span::styled(
            if app.mode == Mode::Demo {
                "Fixture / no commands"
            } else {
                "Revision-checked commands"
            },
            style(MUTED),
        )),
        Line::from(Span::styled("?  keyboard reference", style(MUTED))),
    ];
    if rows[4].height >= 7 {
        lines.insert(2, Line::from(""));
        lines.push(Line::from(Span::styled(
            "q  close this console",
            style(MUTED),
        )));
    }
    frame.render_widget(
        Paragraph::new(lines).block(panel(" Session ").padding(Padding::horizontal(1))),
        rows[4],
    );
}

fn summary_line(label: &str, value: String, color: Color) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{label:<19}"), style(MUTED)),
        Span::styled(
            number_text(value.parse::<u128>().unwrap_or(0), 6),
            strong(color),
        ),
    ])
}

fn draw_pipelines(frame: &mut Frame, area: Rect, app: &App) {
    let Some(snapshot) = &app.snapshot else {
        frame.render_widget(
            Paragraph::new("Awaiting snapshot").block(panel(" Pipelines ")),
            area,
        );
        return;
    };
    let items: Vec<ListItem> = snapshot
        .pipelines
        .iter()
        .enumerate()
        .map(|(index, p)| {
            let selected = index == app.selected;
            let mut lines = vec![
                Line::from(Span::styled(
                    fit_text(&p.id, area.width.saturating_sub(5)),
                    if selected { strong(TEXT) } else { style(TEXT) },
                )),
                Line::from(vec![
                    Span::styled(
                        format!(
                            "{} ",
                            if selected && p.in_flight > 0 {
                                activity(app)
                            } else {
                                "●"
                            }
                        ),
                        status_style(&p.status),
                    ),
                    Span::styled(clean(&p.status, 24), status_style(&p.status)),
                ]),
                Line::from(Span::styled(
                    format!(
                        "{} active · {} queued",
                        number_text(u128::from(p.in_flight), 5),
                        number_text(u128::from(p.queued), 5)
                    ),
                    style(MUTED),
                )),
            ];
            if area.height >= 15 {
                lines.push(Line::from(""));
            }
            ListItem::new(lines)
        })
        .collect();
    let mut state = ListState::default().with_selected(Some(app.selected));
    frame.render_stateful_widget(
        List::new(items)
            .block(
                panel(format!(" Pipelines / {} ", snapshot.pipelines.len()))
                    .title_bottom(
                        Line::from(Span::styled(" ↑↓ select ", style(LAVENDER))).right_aligned(),
                    )
                    .padding(Padding::new(0, 0, u16::from(area.height >= 15), 0)),
            )
            .highlight_symbol("▎ ")
            .highlight_style(Style::default().bg(SELECTED)),
        area,
        &mut state,
    );
}

fn draw_details(frame: &mut Frame, area: Rect, app: &App) {
    let Some(p) = app.selected_pipeline() else {
        frame.render_widget(
            Paragraph::new("No pipelines in this snapshot").block(panel(" Pipeline ")),
            area,
        );
        return;
    };
    let tall = area.height >= 30;
    let rows = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(if tall { 6 } else { 4 }),
        Constraint::Length(if tall { 13 } else { 8 }),
        Constraint::Min(5),
    ])
    .split(area);
    draw_selected(frame, rows[0], app);
    draw_metrics(frame, rows[1], p);
    draw_charts(frame, rows[2], p);
    let columns = Layout::horizontal([
        Constraint::Percentage(59),
        Constraint::Length(1),
        Constraint::Min(24),
    ])
    .split(rows[3]);
    if p.recent_runs.is_empty() {
        draw_overview(frame, columns[0], app);
    } else {
        draw_recent_runs(frame, columns[0], p);
    }
    draw_failure(frame, columns[2], p);
}

/// Milliseconds since the epoch of an RFC 3339 UTC time such as 2026-10-10T07:01:23.334Z.
fn unix_ms_of(text: &str) -> Option<i128> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 || bytes[4] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let millis = if bytes.get(19) == Some(&b'.') {
        text.get(20..23)
            .and_then(|m| m.parse::<i64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(
        (i128::from(days) * 86_400 + i128::from(hour * 3600 + minute * 60 + second)) * 1000
            + i128::from(millis),
    )
}

fn duration_text(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.1} s", ms as f64 / 1000.0)
    } else if ms < 3_600_000 {
        format!("{}m {:02}s", ms / 60_000, (ms % 60_000) / 1000)
    } else {
        format!("{}h {:02}m", ms / 3_600_000, (ms % 3_600_000) / 60_000)
    }
}

/// When the next scheduled run starts, or why none will.
fn schedule_text(app: &App, p: &PipelineSnapshot) -> Option<(String, Color)> {
    let snapshot = app.snapshot.as_ref()?;
    let active: Vec<_> = p
        .triggers
        .iter()
        .filter(|t| t.active && t.kind == "time")
        .collect();
    if active.is_empty() {
        return None;
    }
    if !snapshot.schedules_enabled {
        return Some(("schedules off".into(), WARNING));
    }
    let now = i128::from(snapshot.sampled_at_unix_ms);
    let next = active
        .iter()
        .filter_map(|t| Some((t, unix_ms_of(t.next_run_at.as_deref()?)?)))
        .min_by_key(|(_, at)| *at)?;
    let wait = u64::try_from((next.1 - now).max(0)).unwrap_or(0);
    Some((
        format!(
            "next {} in {}",
            clean(&next.0.name, 40),
            duration_text(wait)
        ),
        ICE,
    ))
}

fn draw_recent_runs(frame: &mut Frame, area: Rect, p: &PipelineSnapshot) {
    let rows = p.recent_runs.iter().map(|run| {
        let (mark, color) = match run.status {
            mage_service_core::protocol::RunStatus::Completed => ("✓ completed", GREEN),
            mage_service_core::protocol::RunStatus::Failed => ("✗ failed", ERROR),
            mage_service_core::protocol::RunStatus::Cancelled => ("■ cancelled", WARNING),
            mage_service_core::protocol::RunStatus::Running => ("● running", PURPLE),
            mage_service_core::protocol::RunStatus::Queued => ("○ queued", MUTED),
        };
        let started = run
            .started_at
            .as_deref()
            .unwrap_or(&run.created_at)
            .get(5..19)
            .unwrap_or("")
            .replace('T', " ");
        Row::new(vec![
            Cell::from(mark).style(style(color)),
            Cell::from(clean(&run.source, 40)).style(style(MUTED)),
            Cell::from(started),
            Cell::from(run.duration_ms.map(duration_text).unwrap_or_default()),
            Cell::from(clean(run.error.as_deref().unwrap_or(""), 300)).style(style(ERROR)),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Length(16),
            Constraint::Length(15),
            Constraint::Length(9),
            Constraint::Min(10),
        ],
    )
    .header(
        Row::new(["Status", "Started by", "Started (UTC)", "Duration", "Error"])
            .style(style(MUTED))
            .bottom_margin(1),
    )
    .column_spacing(1)
    .block(
        panel(format!(" Recent runs / {} ", clean(&p.id, 60)))
            .title_bottom(Line::from(Span::styled(" newest first ", style(MUTED))).right_aligned())
            .padding(Padding::new(1, 1, 1, 0)),
    );
    frame.render_widget(table, area);
}

fn draw_selected(frame: &mut Frame, area: Rect, app: &App) {
    let Some(p) = app.selected_pipeline() else {
        return;
    };
    let columns = Layout::horizontal([Constraint::Percentage(62), Constraint::Min(20)]).split(area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(format!("{:02} / ", app.selected + 1), style(PURPLE)),
                Span::styled(clean(&p.id, 100), strong(TEXT)),
            ]),
            Line::from(vec![
                Span::styled(
                    format!(
                        "{} {}",
                        if p.in_flight > 0 {
                            activity(app)
                        } else {
                            "●"
                        },
                        clean(&p.status, 24)
                    ),
                    status_style(&p.status),
                ),
                Span::styled(
                    format!("   config revision {}", p.config_revision),
                    style(MUTED),
                ),
            ])
            .spans
            .into_iter()
            .chain(
                schedule_text(app, p)
                    .map(|(text, color)| Span::styled(format!("   ·   {text}"), style(color))),
            )
            .collect::<Vec<_>>()
            .into(),
        ]),
        columns[0],
    );
    let right = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
    ])
    .split(columns[1]);
    frame.render_widget(
        Paragraph::new(format!(
            "Active / limit   {} / {}",
            p.in_flight, p.max_in_flight
        ))
        .style(style(MUTED)),
        right[0],
    );
    frame.render_widget(
        LineGauge::default()
            .filled_style(style(if p.in_flight > p.max_in_flight {
                WARNING
            } else {
                PURPLE
            }))
            .unfilled_style(style(BORDER))
            .line_set(symbols::line::THICK)
            .label("")
            .ratio(capacity(p)),
        right[1],
    );
}

fn capacity(p: &PipelineSnapshot) -> f64 {
    if p.max_in_flight == 0 {
        return if p.in_flight == 0 { 0.0 } else { 1.0 };
    }
    (f64::from(p.in_flight) / f64::from(p.max_in_flight)).min(1.0)
}

fn draw_metrics(frame: &mut Frame, area: Rect, p: &PipelineSnapshot) {
    let cards = Layout::horizontal([Constraint::Ratio(1, 4); 4])
        .spacing(1)
        .split(area);
    metric(
        frame,
        cards[0],
        "Succeeded",
        p.runs_succeeded.to_string(),
        "service lifetime",
        ICE,
    );
    metric(
        frame,
        cards[1],
        "Failed",
        p.runs_failed.to_string(),
        "service lifetime",
        if p.runs_failed > 0 { ERROR } else { MUTED },
    );
    metric(
        frame,
        cards[2],
        "In flight",
        p.in_flight.to_string(),
        "active runs",
        LAVENDER,
    );
    metric(
        frame,
        cards[3],
        "Queued",
        p.queued.to_string(),
        "waiting runs",
        if p.queued > 0 { WARNING } else { TEXT },
    );
}

fn metric(frame: &mut Frame, area: Rect, label: &str, value: String, caption: &str, color: Color) {
    let block = panel(format!(" {label} ")).padding(Padding::horizontal(1));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height >= 4 && value.len() * 4 <= usize::from(inner.width) {
        let digits: [&[&str; 3]; 10] = [
            &["█▀█", "█ █", "▀▀▀"],
            &["▄█ ", " █ ", "▄█▄"],
            &["▀▀█", "█▀▀", "▀▀▀"],
            &["▀▀█", " ▀█", "▀▀▀"],
            &["█ █", "▀▀█", "  ▀"],
            &["█▀▀", "▀▀█", "▀▀▀"],
            &["█▀▀", "█▀█", "▀▀▀"],
            &["▀▀█", "  █", "  ▀"],
            &["█▀█", "█▀█", "▀▀▀"],
            &["█▀█", "▀▀█", "▀▀▀"],
        ];
        let lines: Vec<Line> = (0..3)
            .map(|row| {
                Line::from(
                    value
                        .bytes()
                        .map(|digit| digits[usize::from(digit - b'0')][row])
                        .collect::<Vec<_>>()
                        .join(" "),
                )
                .style(strong(color))
            })
            .chain(std::iter::once(
                Line::from(caption.to_owned()).style(style(MUTED)),
            ))
            .collect();
        frame.render_widget(Paragraph::new(lines), inner);
    } else {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    number_text(value.parse::<u128>().unwrap_or(0), inner.width),
                    strong(color),
                )),
                Line::from(Span::styled(
                    match caption {
                        "service lifetime" => "lifetime",
                        "active runs" => "active",
                        "waiting runs" => "waiting",
                        _ => caption,
                    }
                    .to_owned(),
                    style(MUTED),
                )),
            ]),
            inner,
        );
    }
}

fn draw_charts(frame: &mut Frame, area: Rect, p: &PipelineSnapshot) {
    let columns = Layout::horizontal([Constraint::Ratio(1, 2); 2])
        .spacing(1)
        .split(area);
    let duration = percentile95(&p.latency_ms).map_or("n/a".into(), |v| {
        format!(
            "{} ms",
            number_text(u128::from(v), columns[0].width.saturating_sub(25))
        )
    });
    series(
        frame,
        columns[0],
        &p.latency_ms,
        format!(" Run duration / p95 {duration} "),
        format!("{} completions · order →", p.latency_ms.len()),
        PURPLE,
    );
    let unit = window_unit(p.window_seconds);
    let latest = p.throughput.last().map_or("n/a".into(), |v| {
        format!(
            "{} runs/{unit}",
            number_text(u128::from(*v), columns[1].width.saturating_sub(24))
        )
    });
    series(
        frame,
        columns[1],
        &p.throughput,
        format!(" Throughput / {latest} "),
        format!("{} windows of 1 {unit} →", p.throughput.len()),
        ICE,
    );
}

fn window_unit(seconds: u64) -> &'static str {
    match seconds {
        0 | 1 => "s",
        60 => "min",
        3600 => "h",
        _ => "window",
    }
}

fn series(
    frame: &mut Frame,
    area: Rect,
    values: &[u64],
    title: String,
    caption: String,
    color: Color,
) {
    let block = panel(title)
        .title_style(strong(color))
        .title_bottom(
            Line::from(Span::styled(format!(" {caption} "), style(MUTED))).right_aligned(),
        )
        .padding(Padding::new(1, 1, 1, 0));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if values.is_empty() {
        frame.render_widget(Paragraph::new("No samples").style(style(MUTED)), inner);
        return;
    }
    let peak = values.iter().copied().max().unwrap_or(0).max(1);
    let points: Vec<(f64, f64)> = values
        .iter()
        .enumerate()
        .map(|(i, value)| {
            (
                i as f64,
                (u128::from(*value) * 1_000 / u128::from(peak)) as f64,
            )
        })
        .collect();
    let end = values.len().saturating_sub(1).max(1) as f64;
    let middle = [(0.0, 500.0), (end, 500.0)];
    let datasets = vec![
        Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(style(BORDER))
            .data(&middle),
        Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(if values.len() == 1 {
                GraphType::Scatter
            } else {
                GraphType::Line
            })
            .style(style(color))
            .data(&points),
    ];
    frame.render_widget(
        Chart::new(datasets)
            .style(style(TEXT).bg(PANEL))
            .x_axis(
                Axis::default()
                    .bounds([0.0, end])
                    .style(style(BORDER))
                    .labels([
                        Span::styled("first", style(MUTED)),
                        Span::styled("latest", style(MUTED)),
                    ]),
            )
            .y_axis(
                Axis::default()
                    .bounds([0.0, 1_000.0])
                    .style(style(BORDER))
                    .labels(if inner.height < 7 {
                        vec![
                            Span::styled("0", style(MUTED)),
                            Span::styled(short_number(peak), style(MUTED)),
                        ]
                    } else {
                        vec!["0".to_owned(), short_number(peak / 2), short_number(peak)]
                            .into_iter()
                            .map(|value| Span::styled(value, style(MUTED)))
                            .collect()
                    }),
            ),
        inner,
    );
}

fn number_text(value: u128, width: u16) -> String {
    let exact = value.to_string();
    if exact.len() <= usize::from(width) {
        return exact;
    }
    for precision in (0..=2).rev() {
        let abbreviated = format!("≈{:.*e}", precision, value as f64);
        if abbreviated.chars().count() <= usize::from(width) {
            return abbreviated;
        }
    }
    "…".into()
}

fn fit_text(value: &str, width: u16) -> String {
    let text = clean(value, 256);
    if Line::from(text.as_str()).width() <= usize::from(width) {
        return text;
    }
    let mut result = String::new();
    for c in text.chars() {
        if Line::from(format!("{result}{c}…")).width() > usize::from(width) {
            break;
        }
        result.push(c);
    }
    result.push('…');
    result
}

fn short_number(value: u64) -> String {
    number_text(u128::from(value), 6)
}

fn draw_overview(frame: &mut Frame, area: Rect, app: &App) {
    let Some(snapshot) = &app.snapshot else {
        return;
    };
    let rows = snapshot.pipelines.iter().map(|p| {
        Row::new(vec![
            Cell::from(clean(&p.id, 80)),
            Cell::from(clean(&p.status, 24)).style(status_style(&p.status)),
            Cell::from(number_text(u128::from(p.runs_succeeded), 6)).style(style(ICE)),
            Cell::from(number_text(u128::from(p.runs_failed), 6))
                .style(style(if p.runs_failed > 0 { ERROR } else { MUTED })),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Min(18),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(6),
        ],
    )
    .header(
        Row::new(["Pipeline", "Status", "Pass", "Fail"])
            .style(style(MUTED))
            .bottom_margin(1),
    )
    .column_spacing(1)
    .row_highlight_style(Style::default().bg(SELECTED))
    .highlight_symbol("▎ ")
    .block(
        panel(format!(" Service overview ({}) ", snapshot.pipelines.len()))
            .title_bottom(
                Line::from(Span::styled(" service lifetime ", style(MUTED))).right_aligned(),
            )
            .padding(Padding::new(1, 1, 1, 0)),
    );
    let mut state = TableState::default().with_selected(Some(app.selected));
    frame.render_stateful_widget(table, area, &mut state);
}

fn draw_failure(frame: &mut Frame, area: Rect, p: &PipelineSnapshot) {
    let color = if p.last_error.is_some() { ERROR } else { MUTED };
    let mut lines = vec![
        Line::from(Span::styled(
            if p.last_error.is_some() {
                "!  Last recorded failure"
            } else {
                "No failure detail supplied"
            },
            strong(color),
        )),
        Line::from(""),
    ];
    if let Some(error) = &p.last_error {
        lines.push(Line::from(Span::styled(clean(error, 1_024), style(TEXT))));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(panel(" Failure detail ").padding(Padding::new(1, 1, 1, 0))),
        area,
    );
}

fn draw_compact(frame: &mut Frame, area: Rect, app: &App) {
    let Some(p) = app.selected_pipeline() else {
        frame.render_widget(
            Paragraph::new("Awaiting pipeline data").block(panel(" Pipelines ")),
            area,
        );
        return;
    };
    let charts_height = if area.height >= 17 { 8 } else { 0 };
    let rows = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(4),
        Constraint::Length(charts_height),
        Constraint::Min(1),
    ])
    .split(area);
    let total = app.snapshot.as_ref().map_or(0, |s| s.pipelines.len());
    let top = Layout::horizontal([Constraint::Min(20), Constraint::Length(16)]).split(Rect::new(
        rows[0].x,
        rows[0].y,
        rows[0].width,
        1,
    ));
    frame.render_widget(
        Paragraph::new(format!(
            " {:02}/{total:02}  {}",
            app.selected + 1,
            fit_text(&p.id, top[0].width.saturating_sub(8))
        ))
        .style(strong(LAVENDER)),
        top[0],
    );
    frame.render_widget(
        Paragraph::new(format!(
            "{} {}",
            if p.in_flight > 0 {
                activity(app)
            } else {
                "●"
            },
            fit_text(&p.status, 13)
        ))
        .style(status_style(&p.status))
        .right_aligned(),
        top[1],
    );
    frame.render_widget(
        Paragraph::new(format!(
            " ↑↓ select   Active / limit {}/{}   config {}",
            p.in_flight, p.max_in_flight, p.config_revision
        ))
        .style(style(MUTED)),
        Rect::new(rows[0].x, rows[0].y + 1, rows[0].width, 1),
    );
    draw_metrics(frame, rows[1], p);
    if charts_height > 0 {
        draw_charts(frame, rows[2], p);
    }
    let error = p
        .last_error
        .as_ref()
        .map_or("No failure detail supplied".to_owned(), |e| clean(e, 1_024));
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Last recorded failure",
                strong(if p.last_error.is_some() { ERROR } else { MUTED }),
            )),
            Line::from(error),
        ])
        .wrap(Wrap { trim: false }),
        rows[3],
    );
}

fn draw_footer(frame: &mut Frame, area: Rect, app: &App) {
    let key = |text: &'static str| Span::styled(format!(" {text} "), strong(LAVENDER).bg(SELECTED));
    let mut hints = vec![key("↑↓"), Span::styled(" pipeline  ", style(MUTED))];
    if area.width >= 100 {
        hints.extend([
            key("p"),
            Span::styled(" pause/resume  ", style(MUTED)),
            key("+/-"),
            Span::styled(" limit  ", style(MUTED)),
        ]);
    }
    hints.extend([
        key("m"),
        Span::styled(
            if app.reduced_motion {
                " motion off  "
            } else {
                " motion on  "
            },
            style(MUTED),
        ),
        key("?"),
        Span::styled(" help  ", style(MUTED)),
        key("q"),
        Span::styled(" quit", style(MUTED)),
    ]);
    let message = if app.mode == Mode::Demo && app.message.starts_with("Connected.") {
        "Fixture values. Select a pipeline to inspect its metrics.".to_owned()
    } else {
        clean(&app.message, 350)
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(hints),
            Line::from(Span::styled(
                message,
                style(if app.retry_request.is_some() {
                    WARNING
                } else {
                    MUTED
                }),
            )),
        ])
        .block(
            Block::default()
                .borders(Borders::TOP)
                .border_style(style(BORDER)),
        ),
        area,
    );
}

fn status_style(status: &str) -> Style {
    strong(match status {
        "running" | "ready" | "succeeded" => GREEN,
        "failed" | "degraded" => ERROR,
        "paused" | "draining" => WARNING,
        _ => MUTED,
    })
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width.saturating_sub(2));
    let height = height.min(area.height.saturating_sub(2));
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}

fn draw_help(frame: &mut Frame) {
    let area = centered(frame.area(), 78, 18);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from("↑↓ / j k  Select a pipeline"),
            Line::from("p         Review pause or resume admission"),
            Line::from("+ / -     Review concurrency change (1..16)"),
            Line::from("Enter     Submit reviewed request"),
            Line::from("Esc       Dismiss without submitting"),
            Line::from("r         Review retry after unknown delivery"),
            Line::from("m         Toggle reduced motion"),
            Line::from("? / Esc   Close help       q / Ctrl-C  Quit"),
            Line::from(""),
            Line::from(Span::styled("Metric definitions", strong(ICE))),
            Line::from("Duration: completed runs, includes failures."),
            Line::from("Throughput: runs per supplied 1-second UTC window."),
            Line::from("Counters: service lifetime. Missing data: n/a."),
            Line::from("Pause stops admission. Active runs continue."),
        ])
        .block(
            panel(" MageML / keyboard reference ")
                .border_style(style(PURPLE))
                .title_bottom(
                    Line::from(Span::styled(" ? / Esc close ", strong(LAVENDER))).right_aligned(),
                )
                .padding(Padding::horizontal(1)),
        ),
        area,
    );
}

fn draw_confirmation(frame: &mut Frame, app: &App) {
    let Some(request) = &app.pending else { return };
    let command = match &request.command {
        ControlCommand::Pause {} => "Pause new admission; active runs continue".to_owned(),
        ControlCommand::Resume {} => "Resume admission under current capacity limits".to_owned(),
        ControlCommand::SetConcurrency { max_in_flight } => {
            format!("Set max in flight to {max_in_flight}; active runs continue")
        }
    };
    let area = centered(frame.area(), 78, 18);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                format!("Pipeline: {}", clean(&request.pipeline_id, 256)),
                strong(TEXT),
            )),
            Line::from(""),
            Line::from(Span::styled(command, style(WARNING))),
            Line::from(format!(
                "Expected config revision: {}",
                request.expected_config_revision
            )),
            Line::from(format!("Request ID: {}", clean(&request.request_id, 100))),
        ])
        .wrap(Wrap { trim: false })
        .block(
            panel(" Review control request ")
                .border_style(style(WARNING))
                .title_bottom(
                    Line::from(Span::styled(
                        if app.can_control() {
                            " Enter submit · Esc dismiss "
                        } else {
                            " Submission unavailable · Esc dismiss "
                        },
                        strong(WARNING),
                    ))
                    .centered(),
                )
                .padding(Padding::horizontal(1)),
        ),
        area,
    );
}

fn percentile95(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.get((sorted.len() * 95).div_ceil(100) - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::demo_snapshot;
    use ratatui::{Terminal, backend::TestBackend};

    fn render(app: &App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        let mut text = String::new();
        for y in 0..height {
            for x in 0..width {
                text.push_str(terminal.backend().buffer()[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn renders_all_pipelines_and_labels_simulation() {
        let mut app = App::new(Mode::Demo, false);
        app.update(demo_snapshot()).unwrap();
        let output = render(&app, 132, 32);
        for expected in [
            "customer_features",
            "model_scores",
            "warehouse_export",
            "simulated workload",
            "p95",
            "read only",
        ] {
            assert!(output.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn overview_shows_unselected_pipeline_outcomes() {
        let mut app = App::new(Mode::Demo, false);
        app.update(demo_snapshot()).unwrap();
        let output = render(&app, 132, 32);
        assert!(output.contains("Service overview (3)"));
        let score_row = output
            .lines()
            .find(|line| line.contains("model_scores") && line.contains("962"))
            .expect("model score counters must be visible without selecting it");
        assert!(score_row.contains("running"));
        let export_row = output
            .lines()
            .find(|line| line.contains("warehouse_export") && line.contains("81"))
            .expect("export counters must be visible without selecting it");
        assert!(export_row.contains("paused"));
    }

    #[test]
    fn narrow_and_tiny_terminals_do_not_panic() {
        let mut app = App::new(Mode::Demo, false);
        app.update(demo_snapshot()).unwrap();
        for (width, height) in [(1, 1), (40, 10), (60, 18), (80, 24), (100, 24)] {
            render(&app, width, height);
        }
    }

    #[test]
    fn empty_metrics_remain_absent() {
        let mut app = App::new(Mode::Demo, false);
        let mut snapshot = demo_snapshot();
        snapshot.pipelines[0].latency_ms.clear();
        snapshot.pipelines[0].throughput.clear();
        app.update(snapshot).unwrap();
        let output = render(&app, 132, 32);
        assert!(output.contains("No samples"));
        assert!(output.contains("p95 n/a"));
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile95(&[]), None);
        assert_eq!(percentile95(&(1..=100).collect::<Vec<_>>()), Some(95));
        assert_eq!(percentile95(&[40, 10, 20]), Some(40));
    }

    #[test]
    fn extreme_metrics_render_without_arithmetic_overflow() {
        let mut app = App::new(Mode::Snapshot, false);
        let mut snapshot = demo_snapshot();
        snapshot.pipelines[0].latency_ms = vec![0, u64::MAX / 2, u64::MAX];
        snapshot.pipelines[0].throughput = vec![u64::MAX, 0, u64::MAX / 2];
        app.update(snapshot).unwrap();
        let output = render(&app, 132, 32);
        assert!(output.contains(&u64::MAX.to_string()));
    }

    #[test]
    fn compact_status_survives_long_identifiers() {
        let mut app = App::new(Mode::Live, true);
        let mut snapshot = demo_snapshot();
        snapshot.service_id = "service_".repeat(30);
        snapshot.pipelines[0].id = "pipeline_".repeat(28);
        snapshot.pipelines[0].status = "paused".into();
        snapshot.sampled_at_unix_ms = 0;
        app.update(snapshot).unwrap();
        for (width, height) in [(60, 18), (80, 24)] {
            let output = render(&app, width, height);
            assert!(output.contains("Stale / clock skew"));
            assert!(output.contains("paused"));
            assert!(output.contains("controls unavailable"));
            assert!(output.contains('…'));
        }
    }

    #[test]
    fn large_counts_use_marked_approximations_in_narrow_cards() {
        let mut app = App::new(Mode::Snapshot, false);
        let mut snapshot = demo_snapshot();
        snapshot.pipelines[0].runs_succeeded = u64::MAX;
        snapshot.pipelines[0].runs_failed = u64::MAX;
        app.update(snapshot).unwrap();
        for (width, height) in [(60, 18), (80, 24), (132, 32)] {
            let output = render(&app, width, height);
            assert!(output.contains('≈'));
            if output.contains("1844674407") {
                assert!(output.contains(&u64::MAX.to_string()));
            }
        }
        assert_eq!(number_text(1284, 6), "1284");
        assert_eq!(number_text(u128::from(u64::MAX), 6), "≈2e19");
        assert_eq!(number_text(u128::MAX, 7), "≈3.4e38");
    }

    #[test]
    fn medium_layout_keeps_three_pipeline_entries_and_totals() {
        let mut app = App::new(Mode::Demo, false);
        app.update(demo_snapshot()).unwrap();
        for (width, height) in [(110, 30), (132, 32), (150, 44)] {
            let output = render(&app, width, height);
            let rail = output
                .lines()
                .map(|line| line.chars().take(30).collect::<String>())
                .collect::<Vec<_>>()
                .join("\n");
            for value in [
                "customer_features",
                "model_scores",
                "warehouse_export",
                "Lifetime failures",
            ] {
                assert!(
                    rail.contains(value),
                    "{width}x{height} rail missing {value}"
                );
            }
        }
    }

    #[test]
    fn compact_confirmation_preserves_submission_availability() {
        let mut app = App::new(Mode::Live, true);
        let mut snapshot = demo_snapshot();
        snapshot.pipelines[0].id = "p".repeat(256);
        app.update(snapshot).unwrap();
        app.handle_key(crossterm::event::KeyCode::Char('p'));
        let output = render(&app, 60, 18);
        assert!(output.contains("Enter submit · Esc dismiss"));
        assert!(output.contains("Expected config revision: 8"));
        app.disconnect("timeout");
        let output = render(&app, 60, 18);
        assert!(output.contains("Submission unavailable · Esc dismiss"));
    }

    #[test]
    fn compact_help_keeps_last_shortcut_and_pause_semantics() {
        let mut app = App::new(Mode::Demo, false);
        app.update(demo_snapshot()).unwrap();
        app.help = true;
        let output = render(&app, 60, 18);
        assert!(output.contains("r         Review retry"));
        assert!(output.contains("Active runs continue"));
        assert!(output.contains("? / Esc close"));
    }

    #[test]
    fn over_limit_activity_keeps_actual_count_with_bounded_gauge() {
        let mut app = App::new(Mode::Demo, false);
        let mut snapshot = demo_snapshot();
        snapshot.pipelines[0].in_flight = 9;
        snapshot.pipelines[0].max_in_flight = 2;
        app.update(snapshot).unwrap();
        assert_eq!(capacity(app.selected_pipeline().unwrap()), 1.0);
        assert!(render(&app, 132, 32).contains("9 / 2"));
        app.snapshot.as_mut().unwrap().pipelines[0].max_in_flight = 0;
        assert_eq!(capacity(app.selected_pipeline().unwrap()), 1.0);
    }
}
