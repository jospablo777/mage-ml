use std::{
    error::Error,
    fs::File,
    io::{self, IsTerminal, Read},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use mage_console::{
    ANIMATION_INTERVAL, App, ControlOutcome, MAX_RESPONSE_BYTES, Mode, clean, demo_snapshot, ui,
};
use mage_service_core::protocol::{ControlCommand, ControlReceipt, ControlRequest, Snapshot};
use ratatui::{
    Terminal,
    backend::{CrosstermBackend, TestBackend},
};
use reqwest::blocking::{Client, Response};

struct Options {
    mode: Mode,
    endpoint: String,
    snapshot: Option<PathBuf>,
    allow_control: bool,
    render_once: bool,
    render_svg: bool,
    reduced_motion: bool,
    width: u16,
    height: u16,
    frame: u64,
}

enum WorkerMessage {
    Snapshot(Result<Snapshot, String>),
    Control(ControlRequest, ControlOutcome),
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some(options) = options()? else {
        return Ok(());
    };
    let mut app = App::new(options.mode, options.allow_control);
    app.reduced_motion = options.reduced_motion;
    app.set_animation_frame(options.frame);
    if options.mode == Mode::Demo {
        app.update(demo_snapshot())?;
        app.message = "Demo fixture. No pipeline execution or remote controls".into();
    } else if let Some(path) = &options.snapshot {
        let bytes = bounded_read(File::open(path)?)?;
        app.update(serde_json::from_slice(&bytes)?)?;
        app.message = "Saved snapshot. No live connection or remote controls".into();
    }
    if options.render_once || options.render_svg {
        if options.mode == Mode::Live {
            return Err("Offline rendering requires --demo or --snapshot FILE".into());
        }
        return render_once(&app, options.render_svg, options.width, options.height);
    }
    if !io::stdout().is_terminal() || !io::stdin().is_terminal() {
        return Err(
            "Interactive mode requires a terminal; use --demo --render-once for text output".into(),
        );
    }
    let (event_sender, event_receiver) = mpsc::sync_channel(4);
    let (command_sender, command_receiver) = mpsc::sync_channel(1);
    let stopping = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, stopping.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, stopping.clone())?;
    #[cfg(unix)]
    signal_hook::flag::register(signal_hook::consts::SIGHUP, stopping.clone())?;
    let worker = if options.mode == Mode::Live {
        let endpoint = validate_endpoint(&options.endpoint)?;
        // The read token when there is one, so a monitoring console never holds the full
        // token; controls need the service token.
        let read_token = required_token("MAGE_SERVICE_READ_TOKEN")
            .or_else(|_| required_token("MAGE_SERVICE_TOKEN"))
            .map_err(|_| "Set MAGE_SERVICE_READ_TOKEN or MAGE_SERVICE_TOKEN to read the service")?;
        let control_token = if options.allow_control {
            Some(required_token("MAGE_SERVICE_TOKEN")?)
        } else {
            None
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        let stopping = stopping.clone();
        Some(thread::spawn(move || {
            worker_loop(
                client,
                endpoint,
                read_token,
                control_token,
                command_receiver,
                event_sender,
                stopping,
            );
        }))
    } else {
        None
    };
    let result = run_terminal(&mut app, &event_receiver, &command_sender, &stopping);
    stopping.store(true, Ordering::Relaxed);
    drop(event_receiver);
    drop(command_sender);
    if let Some(worker) = worker {
        let _ = worker.join();
    }
    result
}

fn options() -> Result<Option<Options>, Box<dyn Error>> {
    parse_options(std::env::args().skip(1))
}

fn parse_options(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<Options>, Box<dyn Error>> {
    let mut options = Options {
        mode: Mode::Live,
        endpoint: "http://127.0.0.1:8090".into(),
        snapshot: None,
        allow_control: false,
        render_once: false,
        render_svg: false,
        reduced_motion: false,
        width: 150,
        height: 44,
        frame: 0,
    };
    let mut selected_mode = false;
    let mut render_parameters = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--demo" if !selected_mode => {
                options.mode = Mode::Demo;
                selected_mode = true;
            }
            "--snapshot" if !selected_mode => {
                options.snapshot =
                    Some(args.next().ok_or("--snapshot requires a file path")?.into());
                options.mode = Mode::Snapshot;
                selected_mode = true;
            }
            "--endpoint" => options.endpoint = args.next().ok_or("--endpoint requires a URL")?,
            "--allow-control" => options.allow_control = true,
            "--render-once" => options.render_once = true,
            "--render-svg" => options.render_svg = true,
            "--reduce-motion" => options.reduced_motion = true,
            "--width" => {
                options.width = args
                    .next()
                    .ok_or("--width requires a column count")?
                    .parse()?;
                if !(1..=300).contains(&options.width) {
                    return Err("Render width must be between 1 and 300 columns".into());
                }
                render_parameters = true;
            }
            "--height" => {
                options.height = args
                    .next()
                    .ok_or("--height requires a row count")?
                    .parse()?;
                if !(1..=120).contains(&options.height) {
                    return Err("Render height must be between 1 and 120 rows".into());
                }
                render_parameters = true;
            }
            "--frame" => {
                options.frame = args
                    .next()
                    .ok_or("--frame requires a frame number")?
                    .parse()?;
                render_parameters = true;
            }
            "--help" | "-h" => {
                println!(
                    "mage-console [--endpoint URL] [--allow-control] [--reduce-motion]\nmage-console --demo [--render-once | --render-svg]\nmage-console --snapshot FILE [--render-once | --render-svg]\n\nOffline render options: --width 150 --height 44 --frame 0\nUse --reduce-motion or press m to stop activity animation.\nLive reads use MAGE_SERVICE_READ_TOKEN or MAGE_SERVICE_TOKEN. Controls need MAGE_SERVICE_TOKEN.\nHTTP is allowed on loopback; remote connections require HTTPS."
                );
                return Ok(None);
            }
            _ => return Err(format!("Unknown or conflicting option: {}", clean(&arg, 100)).into()),
        }
    }
    if options.allow_control && options.mode != Mode::Live {
        return Err("--allow-control requires live mode".into());
    }
    if options.render_once && options.render_svg {
        return Err("Choose --render-once or --render-svg".into());
    }
    if render_parameters && !(options.render_once || options.render_svg) {
        return Err("--width, --height and --frame require --render-once or --render-svg".into());
    }
    if (options.render_once || options.render_svg) && options.mode == Mode::Live {
        return Err("Offline rendering requires --demo or --snapshot FILE".into());
    }
    Ok(Some(options))
}

fn required_token(name: &str) -> Result<String, Box<dyn Error>> {
    let token = std::env::var(name).map_err(|_| format!("Set {name} for this operation"))?;
    if token.is_empty() || token.len() > 4_096 || token.chars().any(char::is_control) {
        return Err(format!("{name} must contain 1..4096 bytes without control characters").into());
    }
    Ok(token)
}

fn validate_endpoint(endpoint: &str) -> Result<String, Box<dyn Error>> {
    let url = reqwest::Url::parse(endpoint)?;
    let loopback = matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1")
    );
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err("Remote endpoints require HTTPS; HTTP is restricted to loopback".into());
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(
            "Endpoint must be an origin URL without credentials, path, query, or fragment".into(),
        );
    }
    Ok(endpoint.trim_end_matches('/').to_owned())
}

fn bounded_read(reader: impl Read) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err("Snapshot or response exceeds 1 MiB".into());
    }
    Ok(bytes)
}

fn read_snapshot(client: &Client, endpoint: &str, token: &str) -> Result<Snapshot, String> {
    let response = client
        .get(format!("{endpoint}/v1/snapshot"))
        .bearer_auth(token)
        .send()
        .map_err(|error| clean(&error.to_string(), 200))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("Snapshot HTTP {}", status.as_u16()));
    }
    let bytes = bounded_read(response).map_err(|error| error.to_string())?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn submit_control(
    client: &Client,
    endpoint: &str,
    token: &str,
    request: &ControlRequest,
) -> ControlOutcome {
    match client
        .post(format!("{endpoint}/v1/control"))
        .bearer_auth(token)
        .json(request)
        .send()
    {
        Ok(response) if response.status().is_success() => {
            match bounded_read(response)
                .map_err(|error| error.to_string())
                .and_then(|bytes| validate_control_receipt(&bytes, request))
            {
                Ok(()) => ControlOutcome::Accepted,
                Err(error) => ControlOutcome::Unknown(error),
            }
        }
        Ok(response)
            if response.status().is_server_error()
                || response.status().is_redirection()
                || response.status() == reqwest::StatusCode::REQUEST_TIMEOUT =>
        {
            ControlOutcome::Unknown(format!("HTTP {}", response.status().as_u16()))
        }
        Ok(response) => ControlOutcome::Rejected(response_error(response)),
        Err(error) => ControlOutcome::Unknown(clean(&error.to_string(), 160)),
    }
}

fn validate_control_receipt(bytes: &[u8], request: &ControlRequest) -> Result<(), String> {
    let envelope: ControlReceipt = serde_json::from_slice(bytes)
        .map_err(|error| format!("Invalid control receipt: {error}"))?;
    if envelope.request_id != request.request_id {
        return Err("Control receipt answers another request".into());
    }
    let receipt = envelope.pipeline;
    let expected_revision = request
        .expected_config_revision
        .checked_add(1)
        .ok_or("Control revision overflow")?;
    if receipt.id != request.pipeline_id || receipt.config_revision != expected_revision {
        return Err("Control receipt does not match the reviewed pipeline and revision".into());
    }
    let applied = match request.command {
        ControlCommand::Pause {} => receipt.status == "paused",
        ControlCommand::Resume {} => receipt.status != "paused",
        ControlCommand::SetConcurrency { max_in_flight } => receipt.max_in_flight == max_in_flight,
    };
    if !applied {
        return Err("Control receipt does not contain the reviewed configuration change".into());
    }
    Ok(())
}

fn response_error(response: Response) -> String {
    let status = response.status().as_u16();
    let bytes = bounded_read(response).unwrap_or_default();
    let detail = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|value| value.as_str())
                .map(|value| clean(value, 160))
        })
        .unwrap_or_default();
    format!("HTTP {status} {detail}")
}

fn worker_loop(
    client: Client,
    endpoint: String,
    read_token: String,
    control_token: Option<String>,
    commands: Receiver<ControlRequest>,
    events: SyncSender<WorkerMessage>,
    stopping: Arc<AtomicBool>,
) {
    let mut next_poll = Instant::now();
    while !stopping.load(Ordering::Relaxed) {
        if let Ok(command) = commands.recv_timeout(Duration::from_millis(50)) {
            let outcome = match &control_token {
                Some(token) => submit_control(&client, &endpoint, token, &command),
                None => ControlOutcome::Rejected("Control credential is unavailable".into()),
            };
            if events
                .send(WorkerMessage::Control(command, outcome))
                .is_err()
            {
                return;
            }
            next_poll = Instant::now();
        }
        if Instant::now() >= next_poll {
            let snapshot = read_snapshot(&client, &endpoint, &read_token);
            let _ = events.try_send(WorkerMessage::Snapshot(snapshot));
            next_poll = Instant::now() + Duration::from_secs(1);
        }
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn run_terminal(
    app: &mut App,
    events: &Receiver<WorkerMessage>,
    commands: &SyncSender<ControlRequest>,
    stopping: &AtomicBool,
) -> Result<(), Box<dyn Error>> {
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        previous_hook(info);
    }));
    enable_raw_mode()?;
    let _guard = TerminalGuard;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut dirty = true;
    let mut last_draw = Instant::now();
    loop {
        if stopping.load(Ordering::Relaxed) {
            return Ok(());
        }
        while let Ok(message) = events.try_recv() {
            dirty = true;
            match message {
                WorkerMessage::Snapshot(Ok(snapshot)) => {
                    if let Err(error) = app.update(snapshot) {
                        app.disconnect(&error);
                    }
                }
                WorkerMessage::Snapshot(Err(error)) => app.disconnect(&error),
                WorkerMessage::Control(request, outcome) => app.control_result(request, outcome),
            }
        }
        let now = Instant::now();
        dirty |= app.advance_animation(now);
        if dirty || now.duration_since(last_draw) >= Duration::from_secs(1) {
            terminal.draw(|frame| ui::draw(frame, app))?;
            dirty = false;
            last_draw = now;
        }
        let wait = if app.motion_active() {
            ANIMATION_INTERVAL
        } else {
            Duration::from_millis(250)
        };
        if event::poll(wait)? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => {
                    if key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        return Ok(());
                    }
                    dirty = true;
                    if let Some(request) = app.handle_key(key.code)
                        && let Err(error) = commands.try_send(request)
                    {
                        app.in_progress = false;
                        app.message = format!(
                            "Request was not submitted: {}",
                            clean(&error.to_string(), 120)
                        );
                    }
                }
                Event::Resize(_, _) => dirty = true,
                _ => {}
            }
        }
    }
}

fn render_once(app: &App, render_svg: bool, width: u16, height: u16) -> Result<(), Box<dyn Error>> {
    let mut terminal = Terminal::new(TestBackend::new(width, height))?;
    terminal.draw(|frame| ui::draw(frame, app))?;
    if render_svg {
        print!("{}", mage_console::svg::render(terminal.backend().buffer()));
        return Ok(());
    }
    for y in 0..height {
        let mut row = String::new();
        for x in 0..width {
            row.push_str(terminal.backend().buffer()[(x, y)].symbol());
        }
        println!("{}", row.trim_end());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mage_service_core::protocol::PipelineSnapshot;

    fn parse(values: &[&str]) -> Result<Options, Box<dyn Error>> {
        parse_options(values.iter().map(|value| (*value).to_owned()))?
            .ok_or_else(|| "Options were not returned".into())
    }

    #[test]
    fn offline_render_accepts_dimensions_and_frame() {
        let options = parse(&[
            "--demo",
            "--render-svg",
            "--width",
            "96",
            "--height",
            "28",
            "--frame",
            "7",
            "--reduce-motion",
        ])
        .unwrap();
        assert_eq!((options.width, options.height, options.frame), (96, 28, 7));
        assert!(options.reduced_motion);
        assert!(options.render_svg);
        let defaults = parse(&["--demo", "--render-once"]).unwrap();
        assert_eq!(
            (defaults.width, defaults.height, defaults.frame),
            (150, 44, 0)
        );
    }

    #[test]
    fn offline_render_dimensions_are_bounded() {
        for arguments in [
            vec!["--demo", "--render-svg", "--width", "0"],
            vec!["--demo", "--render-svg", "--width", "301"],
            vec!["--demo", "--render-svg", "--height", "0"],
            vec!["--demo", "--render-svg", "--height", "121"],
            vec!["--demo", "--render-svg", "--frame", "-1"],
            vec!["--demo", "--render-svg", "--height"],
            vec!["--demo", "--render-svg", "--width", "many"],
        ] {
            assert!(parse(&arguments).is_err(), "{arguments:?}");
        }
    }

    #[test]
    fn rendering_parameters_require_offline_render_mode() {
        assert!(parse(&["--width", "100"]).is_err());
        assert!(parse(&["--demo", "--frame", "7"]).is_err());
        assert!(parse(&["--render-once"]).is_err());
        assert!(parse(&["--demo", "--allow-control"]).is_err());
        assert!(parse(&["--demo", "--render-once", "--render-svg"]).is_err());
        assert!(parse(&["--reduce-motion"]).unwrap().reduced_motion);
    }

    #[test]
    fn endpoint_does_not_send_credentials_over_remote_http() {
        assert!(validate_endpoint("http://127.0.0.1:8090").is_ok());
        assert!(validate_endpoint("https://pipeline.example.org").is_ok());
        assert!(validate_endpoint("http://pipeline.example.org").is_err());
        assert!(validate_endpoint("https://user:secret@pipeline.example.org").is_err());
        assert!(validate_endpoint("https://pipeline.example.org?token=secret").is_err());
    }

    #[test]
    fn response_size_is_bounded() {
        assert!(bounded_read(&b"small"[..]).is_ok());
        assert!(bounded_read(io::repeat(b'x')).is_err());
    }

    #[test]
    fn control_receipt_requires_expected_pipeline_and_revision() {
        let mut receipt = demo_snapshot().pipelines.remove(0);
        let request = ControlRequest {
            pipeline_id: receipt.id.clone(),
            request_id: "receipt-test".into(),
            expected_config_revision: receipt.config_revision,
            command: ControlCommand::Pause {},
        };
        let envelope = |pipeline: &PipelineSnapshot, id: &str| {
            serde_json::to_vec(&ControlReceipt {
                request_id: id.into(),
                pipeline: pipeline.clone(),
            })
            .unwrap()
        };
        assert!(validate_control_receipt(b"", &request).is_err());
        assert!(validate_control_receipt(&envelope(&receipt, "receipt-test"), &request).is_err());
        receipt.config_revision += 1;
        assert!(validate_control_receipt(&envelope(&receipt, "receipt-test"), &request).is_err());
        receipt.status = "paused".into();
        assert!(validate_control_receipt(&envelope(&receipt, "receipt-test"), &request).is_ok());
        assert!(validate_control_receipt(&envelope(&receipt, "other"), &request).is_err());
        receipt.id = "another-pipeline".into();
        assert!(validate_control_receipt(&envelope(&receipt, "receipt-test"), &request).is_err());
    }
}
