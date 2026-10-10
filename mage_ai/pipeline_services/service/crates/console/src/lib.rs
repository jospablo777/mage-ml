pub mod svg;
pub mod ui;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossterm::event::KeyCode;
use mage_service_core::protocol::{ControlCommand, ControlRequest, PipelineSnapshot, Snapshot};

pub const MAX_RESPONSE_BYTES: u64 = 1_048_576;
pub const STALE_AFTER: Duration = Duration::from_secs(5);
pub const ANIMATION_INTERVAL: Duration = Duration::from_millis(125);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Live,
    Demo,
    Snapshot,
}

pub struct App {
    pub mode: Mode,
    pub snapshot: Option<Snapshot>,
    pub selected: usize,
    pub allow_control: bool,
    pub connected: bool,
    pub received_at: Option<Instant>,
    pub message: String,
    pub help: bool,
    pub pending: Option<ControlRequest>,
    pub in_progress: bool,
    pub retry_request: Option<ControlRequest>,
    pub reduced_motion: bool,
    animation_frame: u64,
    last_animation_tick: Option<Instant>,
    request_sequence: u64,
    session_nonce: String,
}

impl App {
    pub fn new(mode: Mode, allow_control: bool) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            mode,
            snapshot: None,
            selected: 0,
            allow_control: allow_control && mode == Mode::Live,
            connected: false,
            received_at: None,
            message: "Waiting for the first snapshot".into(),
            help: false,
            pending: None,
            in_progress: false,
            retry_request: None,
            reduced_motion: false,
            animation_frame: 0,
            last_animation_tick: None,
            request_sequence: 0,
            session_nonce: format!("{nanos:x}-{:x}", std::process::id()),
        }
    }

    pub fn selected_pipeline(&self) -> Option<&PipelineSnapshot> {
        self.snapshot.as_ref()?.pipelines.get(self.selected)
    }

    pub fn animation_phase(&self) -> u64 {
        self.animation_frame
    }

    pub fn set_animation_frame(&mut self, frame: u64) {
        self.animation_frame = frame;
        self.last_animation_tick = None;
    }

    pub fn motion_active(&self) -> bool {
        !self.reduced_motion
            && !self.help
            && self.pending.is_none()
            && match self.mode {
                Mode::Live => self.connected && !self.stale(),
                Mode::Demo => true,
                Mode::Snapshot => false,
            }
            && self
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.pipelines.iter().any(|p| p.in_flight > 0))
    }

    pub fn advance_animation(&mut self, now: Instant) -> bool {
        if !self.motion_active() {
            self.last_animation_tick = None;
            return false;
        }
        let Some(previous) = self.last_animation_tick else {
            self.last_animation_tick = Some(now);
            return false;
        };
        if now.saturating_duration_since(previous) < ANIMATION_INTERVAL {
            return false;
        }
        self.last_animation_tick = Some(now);
        self.animation_frame = self.animation_frame.wrapping_add(1);
        true
    }

    pub fn update(&mut self, snapshot: Snapshot) -> Result<(), String> {
        validate_snapshot(&snapshot)?;
        let selected_id = self.selected_pipeline().map(|p| p.id.clone());
        self.selected = selected_id
            .and_then(|id| snapshot.pipelines.iter().position(|p| p.id == id))
            .unwrap_or(0);
        self.snapshot = Some(snapshot);
        self.connected = true;
        self.received_at = Some(Instant::now());
        if self.message == "Waiting for the first snapshot" {
            self.message = "Connected. Press ? for shortcuts".into();
        }
        Ok(())
    }

    pub fn stale(&self) -> bool {
        self.received_at
            .is_none_or(|time| time.elapsed() > STALE_AFTER)
            || self.snapshot.as_ref().is_none_or(|snapshot| {
                let now = unix_ms();
                snapshot.sampled_at_unix_ms > now.saturating_add(5_000)
                    || now.saturating_sub(snapshot.sampled_at_unix_ms) > 5_000
            })
    }

    pub fn can_control(&self) -> bool {
        self.allow_control && self.connected && !self.stale() && !self.in_progress
    }

    pub fn disconnect(&mut self, error: &str) {
        self.connected = false;
        self.message = format!("Connection failed: {}", clean(error, 240));
    }

    pub fn handle_key(&mut self, key: KeyCode) -> Option<ControlRequest> {
        if self.help {
            if matches!(key, KeyCode::Esc | KeyCode::Char('?')) {
                self.help = false;
            }
            return None;
        }
        if self.pending.is_some() {
            match key {
                KeyCode::Esc => self.pending = None,
                KeyCode::Enter if self.can_control() => {
                    self.in_progress = true;
                    self.message = "Submitting control request".into();
                    return self.pending.take();
                }
                _ => {}
            }
            return None;
        }
        let count = self.snapshot.as_ref().map_or(0, |s| s.pipelines.len());
        match key {
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('m') => {
                self.reduced_motion = !self.reduced_motion;
                self.message = if self.reduced_motion {
                    "Reduced motion enabled"
                } else {
                    "Activity animation enabled"
                }
                .into();
            }
            KeyCode::Down | KeyCode::Char('j') if count > 0 => {
                self.selected = (self.selected + 1) % count;
            }
            KeyCode::Up | KeyCode::Char('k') if count > 0 => {
                self.selected = (self.selected + count - 1) % count;
            }
            KeyCode::Char('r') if self.can_control() => {
                self.pending = self.retry_request.clone();
                if self.pending.is_none() {
                    self.message = "No request awaiting delivery reconciliation".into();
                }
            }
            KeyCode::Char('p' | '+' | '-') if !self.can_control() => {
                self.message = "Controls require live, fresh data and --allow-control".into();
            }
            KeyCode::Char('p' | '+' | '-') if self.retry_request.is_some() => {
                self.message =
                    "Resolve the previous request first; r reviews delivery retry".into();
            }
            KeyCode::Char(command @ ('p' | '+' | '-')) => {
                let pipeline = self.selected_pipeline()?;
                let command = match command {
                    'p' if pipeline.status == "paused" => ControlCommand::Resume {},
                    'p' => ControlCommand::Pause {},
                    '+' => ControlCommand::SetConcurrency {
                        max_in_flight: pipeline.max_in_flight.saturating_add(1).min(16),
                    },
                    '-' => ControlCommand::SetConcurrency {
                        max_in_flight: pipeline.max_in_flight.saturating_sub(1).max(1),
                    },
                    _ => unreachable!(),
                };
                let pipeline_id = pipeline.id.clone();
                let expected_config_revision = pipeline.config_revision;
                self.request_sequence += 1;
                self.pending = Some(ControlRequest {
                    pipeline_id,
                    request_id: format!("console-{}-{}", self.session_nonce, self.request_sequence),
                    expected_config_revision,
                    command,
                });
            }
            _ => {}
        }
        None
    }

    pub fn control_result(&mut self, request: ControlRequest, result: ControlOutcome) {
        self.in_progress = false;
        match result {
            ControlOutcome::Accepted => {
                self.retry_request = None;
                self.message =
                    "Request accepted. Inspect the next snapshot for current state".into();
            }
            ControlOutcome::Rejected(reason) => {
                self.retry_request = None;
                self.message = format!("Request rejected: {}", clean(&reason, 200));
            }
            ControlOutcome::Unknown(reason) => {
                self.retry_request = Some(request);
                self.message = format!(
                    "Delivery unknown: {}. Inspect state; r retries the same request ID",
                    clean(&reason, 100)
                );
            }
        }
    }
}

pub enum ControlOutcome {
    Accepted,
    Rejected(String),
    Unknown(String),
}

pub fn clean(input: &str, max_chars: usize) -> String {
    input
        .chars()
        .filter(|c| {
            !c.is_control()
                && !matches!(*c, '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2060}'..='\u{206f}' | '\u{feff}')
        })
        .take(max_chars)
        .collect()
}

pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), String> {
    if snapshot.service_id.len() > 256 || snapshot.revision.len() > 256 {
        return Err("Service identity exceeds 256 bytes".into());
    }
    if snapshot.pipelines.len() > 256 {
        return Err("Snapshot exceeds 256 pipelines".into());
    }
    let mut ids = std::collections::HashSet::new();
    for pipeline in &snapshot.pipelines {
        if pipeline.id.is_empty() || pipeline.id.len() > 256 || !ids.insert(&pipeline.id) {
            return Err("Pipeline IDs must be unique and contain 1..256 bytes".into());
        }
        if pipeline.latency_ms.len() > 120 || pipeline.throughput.len() > 120 {
            return Err("A metric series exceeds 120 samples".into());
        }
        if pipeline
            .last_error
            .as_ref()
            .is_some_and(|s| s.len() > 16_384)
        {
            return Err("Error detail exceeds 16 KiB".into());
        }
    }
    Ok(())
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub fn demo_snapshot() -> Snapshot {
    Snapshot {
        service_id: "feature-services".into(),
        revision: "demo/fixture".into(),
        sampled_at_unix_ms: unix_ms(),
        pipelines: vec![
            PipelineSnapshot {
                id: "customer_features".into(),
                status: "running".into(),
                runs_succeeded: 1_284,
                runs_failed: 2,
                in_flight: 3,
                queued: 7,
                latency_ms: vec![46, 51, 43, 70, 53, 58, 48, 44, 54, 50, 49, 45],
                throughput: vec![7, 9, 11, 10, 13, 12, 14, 9, 11, 13, 10, 12],
                last_error: Some("2026-10-09T08:14:00Z: upstream request timed out".into()),
                config_revision: 8,
                max_in_flight: 8,
                ..Default::default()
            },
            PipelineSnapshot {
                id: "model_scores".into(),
                status: "running".into(),
                runs_succeeded: 962,
                runs_failed: 0,
                in_flight: 1,
                queued: 0,
                latency_ms: vec![180, 220, 195, 190, 201, 198],
                throughput: vec![4, 4, 5, 6, 5, 5],
                last_error: None,
                config_revision: 3,
                max_in_flight: 2,
                ..Default::default()
            },
            PipelineSnapshot {
                id: "warehouse_export".into(),
                status: "paused".into(),
                runs_succeeded: 81,
                runs_failed: 1,
                in_flight: 0,
                queued: 2,
                latency_ms: vec![2_130, 2_550, 2_320, 2_680],
                throughput: vec![0, 1, 0, 0, 1, 0],
                last_error: Some("2026-10-09T08:02:00Z: sink rejected schema v4".into()),
                config_revision: 12,
                max_in_flight: 1,
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_survives_server_reordering() {
        let mut app = App::new(Mode::Live, false);
        app.update(demo_snapshot()).unwrap();
        app.handle_key(KeyCode::Down);
        app.handle_key(KeyCode::Down);
        let mut next = demo_snapshot();
        next.pipelines.reverse();
        app.update(next).unwrap();
        assert_eq!(app.selected_pipeline().unwrap().id, "warehouse_export");
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn mutation_requires_confirmation_and_live_permission() {
        let mut app = App::new(Mode::Live, true);
        app.update(demo_snapshot()).unwrap();
        assert!(app.handle_key(KeyCode::Char('p')).is_none());
        assert!(app.pending.is_some());
        app.handle_key(KeyCode::Esc);
        assert!(app.pending.is_none());
        app.handle_key(KeyCode::Char('p'));
        assert!(app.handle_key(KeyCode::Enter).is_some());
        assert!(app.in_progress);
        let mut demo = App::new(Mode::Demo, true);
        demo.update(demo_snapshot()).unwrap();
        demo.handle_key(KeyCode::Char('p'));
        assert!(demo.pending.is_none());
    }

    #[test]
    fn stale_or_disconnected_data_blocks_controls() {
        let mut app = App::new(Mode::Live, true);
        let mut snapshot = demo_snapshot();
        snapshot.sampled_at_unix_ms = 0;
        app.update(snapshot).unwrap();
        app.handle_key(KeyCode::Char('+'));
        assert!(app.pending.is_none());
        app.update(demo_snapshot()).unwrap();
        app.disconnect("timeout");
        app.handle_key(KeyCode::Char('p'));
        assert!(app.pending.is_none());
    }

    #[test]
    fn uncertain_delivery_reuses_request_identity() {
        let mut app = App::new(Mode::Live, true);
        app.update(demo_snapshot()).unwrap();
        app.handle_key(KeyCode::Char('p'));
        let request = app.handle_key(KeyCode::Enter).unwrap();
        app.control_result(request.clone(), ControlOutcome::Unknown("timeout".into()));
        app.handle_key(KeyCode::Char('+'));
        assert!(app.pending.is_none());
        app.handle_key(KeyCode::Char('r'));
        assert_eq!(
            app.handle_key(KeyCode::Enter).unwrap().request_id,
            request.request_id
        );
    }

    #[test]
    fn hostile_terminal_text_is_filtered_and_bounded() {
        assert_eq!(clean("ok\x1b[2J\n\u{202e}bad", 100), "ok[2Jbad");
        assert_eq!(clean("αβγδε", 3), "αβγ");
    }

    #[test]
    fn duplicate_pipeline_ids_are_rejected() {
        let mut snapshot = demo_snapshot();
        snapshot.pipelines.push(snapshot.pipelines[0].clone());
        assert!(validate_snapshot(&snapshot).is_err());
    }

    #[test]
    fn animation_has_a_bounded_cadence_without_changing_metrics() {
        let mut app = App::new(Mode::Demo, false);
        let mut snapshot = demo_snapshot();
        snapshot.sampled_at_unix_ms = 0;
        app.update(snapshot.clone()).unwrap();
        let now = Instant::now();
        assert!(!app.advance_animation(now));
        assert!(!app.advance_animation(now + Duration::from_millis(124)));
        assert!(app.advance_animation(now + ANIMATION_INTERVAL));
        assert_eq!(app.animation_phase(), 1);
        assert!(!app.advance_animation(now + ANIMATION_INTERVAL));
        assert_eq!(app.snapshot, Some(snapshot));
    }

    #[test]
    fn live_motion_stops_for_stale_or_disconnected_samples() {
        let mut app = App::new(Mode::Live, false);
        app.update(demo_snapshot()).unwrap();
        assert!(app.motion_active());
        app.received_at = Some(Instant::now() - Duration::from_secs(6));
        assert!(!app.motion_active());
        app.update(demo_snapshot()).unwrap();
        app.snapshot.as_mut().unwrap().sampled_at_unix_ms = 0;
        assert!(!app.motion_active());
        app.update(demo_snapshot()).unwrap();
        app.disconnect("timeout");
        assert!(!app.motion_active());
    }

    #[test]
    fn snapshots_and_idle_workloads_have_no_activity_animation() {
        let mut saved = App::new(Mode::Snapshot, false);
        saved.update(demo_snapshot()).unwrap();
        assert!(!saved.motion_active());
        let mut demo = App::new(Mode::Demo, false);
        let mut snapshot = demo_snapshot();
        for pipeline in &mut snapshot.pipelines {
            pipeline.in_flight = 0;
        }
        demo.update(snapshot).unwrap();
        assert!(!demo.motion_active());
    }

    #[test]
    fn reduced_motion_and_dialogs_freeze_animation() {
        let mut app = App::new(Mode::Live, true);
        app.update(demo_snapshot()).unwrap();
        app.set_animation_frame(9);
        app.handle_key(KeyCode::Char('m'));
        assert!(app.reduced_motion);
        assert!(!app.advance_animation(Instant::now()));
        assert_eq!(app.animation_phase(), 9);
        app.handle_key(KeyCode::Char('m'));
        assert!(app.motion_active());
        app.handle_key(KeyCode::Char('?'));
        assert!(!app.motion_active());
        app.handle_key(KeyCode::Esc);
        app.handle_key(KeyCode::Char('p'));
        assert!(!app.motion_active());
        assert!(app.pending.is_some());
    }
}
