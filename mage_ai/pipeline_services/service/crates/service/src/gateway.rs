//! `mage-service gateway`: a reverse proxy in front of the MageML server (Tornado), the
//! first step of moving the API to Rust (ADR-0007). Every request passes through as it
//! came, Host included: bodies stream both ways without buffering, and a WebSocket upgrade
//! becomes a byte tunnel once the server agrees, so kernels and terminals work unchanged.
//! Route families move here one at a time later; until then the gateway answers nothing
//! itself, except 502 when the server does not answer, and compares its own answers for
//! the routes given with --shadow (gateway_shadow.rs).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;

use crate::gateway_shadow::Shadow;

type Body = BoxBody<Bytes, hyper::Error>;

/// Headers that describe one hop and never pass on (RFC 9110, section 7.6.1).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Clone)]
struct Upstream {
    authority: String,
    client: Client<HttpConnector, Incoming>,
    /// Reads a shadowed route's answer again (gateway_shadow.rs).
    rereads: Client<HttpConnector, Empty<Bytes>>,
    shadow: Option<Arc<Shadow>>,
}

const SCHEDULER_SHADOW_EVERY: Duration = Duration::from_secs(15);

/// The gateway's own page: its shadow reads, from loopback only.
const SHADOW_REPORT: &str = "/__gateway/shadow";

/// Exits once the process that started the gateway is gone: a stopped or killed MageML
/// server leaves no gateway holding its port.
pub fn exit_with_parent() {
    #[cfg(unix)]
    {
        let parent = std::os::unix::process::parent_id();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(500));
                if std::os::unix::process::parent_id() != parent {
                    crate::log::info("The MageML server stopped; the gateway exits.");
                    std::process::exit(0);
                }
            }
        });
    }
}

pub async fn run(
    listen: SocketAddr,
    upstream: String,
    shadow: Option<Shadow>,
) -> Result<(), String> {
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("Could not listen on {listen}: {e}"))?;
    serve(listener, upstream, shadow, HEADER_TIMEOUT).await
}

/// How long a client may take to send a request's headers, the next request's on a kept
/// connection included; a client that trickles them (slowloris) is cut off then.
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn serve(
    listener: TcpListener,
    upstream: String,
    shadow: Option<Shadow>,
    header_timeout: Duration,
) -> Result<(), String> {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_keepalive(Some(Duration::from_secs(60)));
    connector.set_connect_timeout(Some(CONNECT_TIMEOUT));
    let upstream = Upstream {
        authority: upstream,
        client: Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(90))
            .build(connector.clone()),
        rereads: Client::builder(TokioExecutor::new()).build(connector),
        shadow: shadow.map(Arc::new),
    };
    if let Some(shadow) = upstream.shadow.clone().filter(|s| s.shadows_scheduler()) {
        tokio::spawn(async move {
            // The scheduler looks every 10 s; every 15 s is enough to see its runs.
            let mut every = tokio::time::interval(SCHEDULER_SHADOW_EVERY);
            loop {
                every.tick().await;
                shadow.scheduler_tick().await;
            }
        });
    }
    crate::log::info(&format!(
        "Gateway on {} for the MageML server at {}",
        listener.local_addr().map_err(|e| e.to_string())?,
        upstream.authority
    ));
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                crate::log::warn(&format!("Gateway accept: {error}"));
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let upstream = upstream.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let upstream = upstream.clone();
                async move { Ok::<_, Infallible>(forward(&upstream, peer, request).await) }
            });
            // Errors here are clients that went away; nothing to answer.
            let _ = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(header_timeout)
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
    }
}

async fn forward(
    upstream: &Upstream,
    peer: SocketAddr,
    mut request: Request<Incoming>,
) -> Response<Body> {
    if request.uri().path() == SHADOW_REPORT {
        return match &upstream.shadow {
            Some(shadow) if peer.ip().is_loopback() => json_answer(&shadow.report()),
            _ => answer(StatusCode::NOT_FOUND, "Not found."),
        };
    }
    let shadowed = upstream
        .shadow
        .as_ref()
        .and_then(|shadow| shadow.route_for(request.method(), request.uri().path()));
    let query = request.uri().query().unwrap_or_default().to_string();
    let route_path = request.uri().path().to_string();
    let reread_uri = request.uri().clone();
    if let (Some(route), Some(shadow)) = (shadowed, &upstream.shadow)
        && let Some(own) = shadow
            .serve(route, &route_path, &query, request.headers())
            .await
    {
        if shadow.check_served(route) {
            // The server's answer for the same request, compared in the background.
            let mut headers = request.headers().clone();
            strip_hop_by_hop(&mut headers, false);
            headers.remove(header::ACCEPT_ENCODING);
            let (shadow, path, query) = (shadow.clone(), route_path.clone(), query.clone());
            let reread = reread(upstream, &reread_uri, &headers);
            tokio::spawn(async move {
                if let Some(answer) = reread().await {
                    shadow
                        .compare(route, &path, &query, &answer, &headers, reread)
                        .await;
                }
            });
        }
        return served_json(&own, request.headers());
    }
    let upgrade = wants_upgrade(request.headers());
    let client_side = upgrade.then(|| hyper::upgrade::on(&mut request));
    let (mut parts, body) = request.into_parts();
    if shadowed.is_some() {
        // Compared as JSON, so the server sends it uncompressed.
        parts.headers.remove(header::ACCEPT_ENCODING);
    }
    let reread_headers = parts.headers.clone();
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    parts.uri = match Uri::try_from(format!("http://{}{path}", upstream.authority)) {
        Ok(uri) => uri,
        Err(error) => return answer(StatusCode::BAD_REQUEST, &format!("{error}")),
    };
    strip_hop_by_hop(&mut parts.headers, upgrade);
    forwarded_for(&mut parts.headers, peer);

    let mut response = match upstream
        .client
        .request(Request::from_parts(parts, body))
        .await
    {
        Ok(response) => response,
        Err(error) => {
            crate::log::warn(&format!(
                "Gateway: the MageML server did not answer: {error}"
            ));
            return answer(
                StatusCode::BAD_GATEWAY,
                "The MageML server behind the gateway did not answer.",
            );
        }
    };
    if let Some(client_side) = client_side
        && response.status() == StatusCode::SWITCHING_PROTOCOLS
    {
        let server_side = hyper::upgrade::on(&mut response);
        tokio::spawn(async move {
            if let (Ok(client), Ok(server)) = tokio::join!(client_side, server_side) {
                let (mut client, mut server) = (TokioIo::new(client), TokioIo::new(server));
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            }
        });
        let (parts, body) = response.into_parts();
        return Response::from_parts(parts, body.boxed());
    }
    let (mut parts, body) = response.into_parts();
    strip_hop_by_hop(&mut parts.headers, false);
    if let (Some(route), Some(shadow)) = (shadowed, &upstream.shadow)
        && parts.status == StatusCode::OK
    {
        let served = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(error) => {
                crate::log::warn(&format!("Gateway: reading the server's answer: {error}"));
                return answer(
                    StatusCode::BAD_GATEWAY,
                    "The MageML server's answer broke off.",
                );
            }
        };
        let (shadow, compared) = (shadow.clone(), served.clone());
        let reread = reread(upstream, &reread_uri, &reread_headers);
        tokio::spawn(async move {
            shadow
                .compare(
                    route,
                    &route_path,
                    &query,
                    &compared,
                    &reread_headers,
                    reread,
                )
                .await
        });
        return Response::from_parts(
            parts,
            Full::new(served).map_err(|never| match never {}).boxed(),
        );
    }
    Response::from_parts(parts, body.boxed())
}

/// The same GET again, with the same headers, for a shadow read to compare once more.
fn reread(upstream: &Upstream, uri: &Uri, headers: &HeaderMap) -> crate::gateway_shadow::Reread {
    let (client, headers) = (upstream.rereads.clone(), headers.clone());
    let target = format!(
        "http://{}{}",
        upstream.authority,
        uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
    );
    Box::new(move || {
        let (client, headers, target) = (client.clone(), headers.clone(), target.clone());
        Box::pin(async move {
            let mut request = Request::get(target).body(Empty::new()).ok()?;
            *request.headers_mut() = headers;
            let response = client.request(request).await.ok()?;
            if response.status() != StatusCode::OK {
                return None;
            }
            Some(
                response
                    .into_body()
                    .collect()
                    .await
                    .ok()?
                    .to_bytes()
                    .to_vec(),
            )
        })
    })
}

/// An answer of the gateway's own, with the headers MageML's server sends with JSON: its
/// CORS headers and an ETag (the body's SHA-1, as Tornado makes it), 304 when it matches.
fn served_json(value: &serde_json::Value, request: &HeaderMap) -> Response<Body> {
    let body = value.to_string();
    let digest = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, body.as_bytes());
    let etag = format!(
        "\"{}\"",
        digest
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let not_modified = request
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|tag| tag.trim().trim_start_matches("W/") == etag)
        });
    let mut response = if not_modified {
        let mut response = Response::new(
            Full::new(Bytes::new())
                .map_err(|never| match never {})
                .boxed(),
        );
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
    } else {
        json_answer(value)
    };
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        "access-control-allow-headers",
        HeaderValue::from_static("*"),
    );
    headers.insert(
        "access-control-allow-methods",
        HeaderValue::from_static("DELETE, GET, PATCH, POST, PUT, OPTIONS"),
    );
    headers.insert("access-control-allow-origin", HeaderValue::from_static("*"));
    if let Ok(value) = HeaderValue::try_from(etag) {
        headers.insert(header::ETAG, value);
    }
    response
}

fn json_answer(value: &serde_json::Value) -> Response<Body> {
    let mut response = answer(StatusCode::OK, "");
    *response.body_mut() = Full::new(Bytes::from(value.to_string()))
        .map_err(|never| match never {})
        .boxed();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

fn wants_upgrade(headers: &HeaderMap) -> bool {
    headers.contains_key(header::UPGRADE)
        && headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
}

/// Drops the hop-by-hop headers and those the Connection header names; an upgrade keeps
/// Connection and Upgrade, which ask the server to switch.
fn strip_hop_by_hop(headers: &mut HeaderMap, upgrade: bool) {
    let named: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|token| HeaderName::try_from(token.trim()).ok())
        .collect();
    for name in named {
        if !(upgrade && name == header::UPGRADE) {
            headers.remove(name);
        }
    }
    for name in HOP_BY_HOP {
        if upgrade && (*name == "connection" || *name == "upgrade") {
            continue;
        }
        headers.remove(*name);
    }
}

/// X-Forwarded-For gains the client's address and X-Real-Ip is that address, replacing
/// what the client sent, so the server can log who asked.
fn forwarded_for(headers: &mut HeaderMap, peer: SocketAddr) {
    let ip = peer.ip().to_string();
    let chain = match headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        Some(earlier) => format!("{earlier}, {ip}"),
        None => ip.clone(),
    };
    if let Ok(value) = HeaderValue::try_from(chain) {
        headers.insert("x-forwarded-for", value);
    }
    if let Ok(value) = HeaderValue::try_from(ip) {
        headers.insert("x-real-ip", value);
    }
}

fn answer(status: StatusCode, message: &str) -> Response<Body> {
    let mut response = Response::new(
        Full::new(Bytes::from(format!("{message}\n")))
            .map_err(|never| match never {})
            .boxed(),
    );
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_by_hop_headers_stop_and_an_upgrade_keeps_its_two() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-private"),
        );
        headers.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        headers.insert("x-private", HeaderValue::from_static("1"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(header::COOKIE, HeaderValue::from_static("a=1"));
        strip_hop_by_hop(&mut headers, false);
        assert_eq!(headers.keys().collect::<Vec<_>>(), [header::COOKIE]);

        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        assert!(wants_upgrade(&headers));
        strip_hop_by_hop(&mut headers, true);
        assert!(headers.contains_key(header::CONNECTION));
        assert!(headers.contains_key(header::UPGRADE));
    }

    use std::time::Instant;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// A server that echoes the request, streams /slow in two chunks half a second apart,
    /// and switches /tunnel to a protocol that answers each read in capitals.
    async fn upstream() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service =
                        hyper::service::service_fn(|mut request: Request<Incoming>| async move {
                            let path = request.uri().path().to_string();
                            if path == "/tunnel" {
                                let upgraded = hyper::upgrade::on(&mut request);
                                tokio::spawn(async move {
                                    let mut io = TokioIo::new(upgraded.await.unwrap());
                                    let mut buffer = [0u8; 64];
                                    while let Ok(n) = io.read(&mut buffer).await {
                                        if n == 0 {
                                            break;
                                        }
                                        let loud = buffer[..n].to_ascii_uppercase();
                                        io.write_all(&loud).await.unwrap();
                                    }
                                });
                                let mut response = Response::new(
                                    Full::new(Bytes::new())
                                        .map_err(|never| match never {})
                                        .boxed(),
                                );
                                *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
                                response.headers_mut().insert(
                                    header::CONNECTION,
                                    HeaderValue::from_static("upgrade"),
                                );
                                response
                                    .headers_mut()
                                    .insert(header::UPGRADE, HeaderValue::from_static("shout"));
                                return Ok::<_, Infallible>(response);
                            }
                            if path == "/slow" {
                                let (sender, receiver) = tokio::sync::mpsc::channel(2);
                                tokio::spawn(async move {
                                    let _ = sender.send(Bytes::from("first;")).await;
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                    let _ = sender.send(Bytes::from("second")).await;
                                });
                                let body = ChannelBody(receiver);
                                return Ok(Response::new(body.boxed()));
                            }
                            let head = format!(
                                "{} {} host={} real={} forwarded={} body=",
                                request.method(),
                                request.uri(),
                                request.headers()[header::HOST].to_str().unwrap(),
                                request.headers()["x-real-ip"].to_str().unwrap(),
                                request.headers()["x-forwarded-for"].to_str().unwrap(),
                            );
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            let mut text = head.into_bytes();
                            text.extend_from_slice(&body);
                            Ok(Response::new(
                                Full::new(Bytes::from(text))
                                    .map_err(|never| match never {})
                                    .boxed(),
                            ))
                        });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        address
    }

    /// A body whose chunks arrive on a channel, as a server writes them.
    struct ChannelBody(tokio::sync::mpsc::Receiver<Bytes>);

    impl hyper::body::Body for ChannelBody {
        type Data = Bytes;
        type Error = hyper::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, hyper::Error>>> {
            self.0
                .poll_recv(cx)
                .map(|chunk| chunk.map(|bytes| Ok(hyper::body::Frame::data(bytes))))
        }
    }

    async fn gateway(upstream: SocketAddr) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve(
            listener,
            upstream.to_string(),
            None,
            Duration::from_millis(300),
        ));
        address
    }

    async fn exchange(address: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).await.unwrap();
        text
    }

    #[tokio::test]
    async fn a_request_passes_through_with_its_host_body_and_the_clients_address() {
        let through = gateway(upstream().await).await;
        let text = exchange(through, "POST /api/runs?x=1 HTTP/1.1\r\nHost: mage.example\r\nX-Real-Ip: 6.6.6.6\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello").await;
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(
            text.ends_with(
                "POST /api/runs?x=1 host=mage.example real=127.0.0.1 forwarded=127.0.0.1 body=hello"
            ),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_response_streams_without_waiting_for_its_end() {
        let through = gateway(upstream().await).await;
        let mut stream = TcpStream::connect(through).await.unwrap();
        stream
            .write_all(b"GET /slow HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let start = Instant::now();
        let mut seen = Vec::new();
        let mut buffer = [0u8; 256];
        while !String::from_utf8_lossy(&seen).contains("first;") {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0, "{}", String::from_utf8_lossy(&seen));
            seen.extend_from_slice(&buffer[..n]);
        }
        assert!(
            start.elapsed() < Duration::from_millis(400),
            "the first chunk waited for the last"
        );
        stream.read_to_end(&mut seen).await.unwrap();
        assert!(String::from_utf8_lossy(&seen).contains("second"));
    }

    #[tokio::test]
    async fn an_upgrade_becomes_a_tunnel() {
        let through = gateway(upstream().await).await;
        let mut stream = TcpStream::connect(through).await.unwrap();
        stream
            .write_all(
                b"GET /tunnel HTTP/1.1\r\nHost: h\r\nConnection: Upgrade\r\nUpgrade: shout\r\n\r\n",
            )
            .await
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("HTTP/1.1 101"), "{head}");
        assert!(
            head.to_ascii_lowercase().contains("upgrade: shout"),
            "{head}"
        );
        for word in ["kernel", "terminal"] {
            stream.write_all(word.as_bytes()).await.unwrap();
            let mut echoed = vec![0u8; word.len()];
            stream.read_exact(&mut echoed).await.unwrap();
            assert_eq!(echoed, word.to_ascii_uppercase().as_bytes());
        }
    }

    #[tokio::test]
    async fn a_client_that_trickles_its_headers_is_cut_off() {
        let through = gateway(upstream().await).await;
        let mut stream = TcpStream::connect(through).await.unwrap();
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: h\r\n")
            .await
            .unwrap();
        let start = Instant::now();
        let mut rest = Vec::new();
        // The gateway answers 408 or closes; either way the read ends.
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut rest)).await;
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "the connection stayed open"
        );
    }

    #[tokio::test]
    async fn a_server_that_does_not_answer_is_a_502() {
        let closed = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let through = gateway(closed).await;
        let text = exchange(
            through,
            "GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(text.starts_with("HTTP/1.1 502"), "{text}");
        assert!(text.contains("did not answer"));
    }

    #[test]
    fn the_client_address_replaces_a_claimed_one() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("10.0.0.1"));
        headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.9"));
        forwarded_for(&mut headers, "192.0.2.4:5000".parse().unwrap());
        assert_eq!(headers["x-real-ip"], "192.0.2.4");
        assert_eq!(headers["x-forwarded-for"], "203.0.113.9, 192.0.2.4");
    }
}
