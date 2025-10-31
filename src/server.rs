use std::io;
use std::sync::Arc;
use std::thread;

use log::{debug, error, info, warn};
use percent_encoding::percent_decode_str;
use rusqlite::{ToSql, types::FromSql};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tiny_http::{Header, Method, Response, Server, StatusCode};

use crate::kv::{KvError, KvHandle};

#[derive(serde::Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned"))]
struct PutRequest<T> {
    key: String,
    value: T,
}

pub fn start_http_service<T>(
    addr: &str,
    handle: KvHandle<T>,
) -> std::io::Result<thread::JoinHandle<()>>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    let addr = addr.to_string();
    let handle = Arc::new(handle);
    let thread = thread::Builder::new()
        .name(format!("kv-http-{addr}"))
        .spawn(move || {
            let server = match Server::http(&addr) {
                Ok(server) => server,
                Err(err) => {
                    error!("failed to bind http endpoint {addr}: {err}");
                    return;
                }
            };
            info!("http endpoint listening on {addr}");

            for request in server.incoming_requests() {
                let method = request.method().clone();
                let url = request.url().to_string();
                let handle = Arc::clone(&handle);
                if let Err(err) = handle_request(request, method.clone(), url.clone(), handle) {
                    warn!("request handling error for {} {}: {err}", method, url);
                }
            }
        })?;
    Ok(thread)
}

fn handle_request<T>(
    request: tiny_http::Request,
    method: Method,
    url: String,
    handle: Arc<KvHandle<T>>,
) -> Result<(), String>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    debug!("incoming http request {} {}", method, url);
    match (method, url.as_str()) {
        (Method::Get, "/raft/leader") => respond_leader(handle, request),
        (Method::Get, path) if path.starts_with("/kv") => {
            let query = path.split_once('?').map(|x| x.1);
            if let Some(q) = query {
                if let Some(key) = parse_query_key(q) {
                    respond_with_key(handle, request, key)
                } else {
                    respond_bad_request(request, "missing key parameter")
                }
            } else {
                respond_with_snapshot(handle, request)
            }
        }
        (Method::Put, "/kv") => respond_put(handle, request),
        (Method::Get, "/health") => {
            let response = Response::from_string("ok").with_status_code(StatusCode(200));
            request.respond(response).map_err(|e| e.to_string())
        }
        (method, path) => {
            let response = Response::from_string(format!("unknown route {method:?} {path}"))
                .with_status_code(StatusCode(404));
            request.respond(response).map_err(|e| e.to_string())
        }
    }
}

fn respond_with_snapshot<T>(
    handle: Arc<KvHandle<T>>,
    request: tiny_http::Request,
) -> Result<(), String>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    match handle.snapshot() {
        Ok(entries) => {
            let payload = json!({
                "entries": entries
            })
            .to_string();
            let response = Response::from_string(payload)
                .with_status_code(StatusCode(200))
                .with_header(header("Content-Type", "application/json"));
            request.respond(response).map_err(|e| e.to_string())
        }
        Err(err) => respond_internal_error(request, err.to_string()),
    }
}

fn respond_with_key<T>(
    handle: Arc<KvHandle<T>>,
    request: tiny_http::Request,
    key: String,
) -> Result<(), String>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    match handle.get(&key) {
        Ok(Some(value)) => {
            let payload = json!({ "key": key, "value": value }).to_string();
            let response = Response::from_string(payload)
                .with_status_code(StatusCode(200))
                .with_header(header("Content-Type", "application/json"));
            request.respond(response).map_err(|e| e.to_string())
        }
        Ok(None) => {
            let response = Response::from_string("not found").with_status_code(StatusCode(404));
            request.respond(response).map_err(|e| e.to_string())
        }
        Err(err) => respond_internal_error(request, err.to_string()),
    }
}

fn respond_put<T>(handle: Arc<KvHandle<T>>, mut request: tiny_http::Request) -> Result<(), String>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    let mut body = String::new();
    let mut reader = request.as_reader();
    io::Read::read_to_string(&mut reader, &mut body).map_err(|e| e.to_string())?;
    debug!("received PUT payload: {}", body);
    let put: PutRequest<T> = serde_json::from_str(&body).map_err(|e| e.to_string())?;

    match handle.put(put.key, put.value) {
        Ok(()) => {
            let response = Response::from_string("ok")
                .with_status_code(StatusCode(200))
                .with_header(header("Content-Type", "text/plain"));
            request.respond(response).map_err(|e| e.to_string())
        }
        Err(KvError::ChannelClosed) => respond_internal_error(request, "raft unavailable".into()),
        Err(err) => respond_internal_error(request, err.to_string()),
    }
}

fn respond_leader<T>(handle: Arc<KvHandle<T>>, request: tiny_http::Request) -> Result<(), String>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    let payload = json!({
        "node_id": handle.node_id(),
        "leader": handle.leader_id(),
        "linearizable": handle.is_linearizable(),
    })
    .to_string();
    let response = Response::from_string(payload)
        .with_status_code(StatusCode(200))
        .with_header(header("Content-Type", "application/json"));
    request.respond(response).map_err(|e| e.to_string())
}

fn respond_bad_request(request: tiny_http::Request, msg: &str) -> Result<(), String> {
    let response = Response::from_string(msg).with_status_code(StatusCode(400));
    request.respond(response).map_err(|e| e.to_string())
}

fn respond_internal_error(request: tiny_http::Request, msg: String) -> Result<(), String> {
    let response = Response::from_string(msg).with_status_code(StatusCode(500));
    request.respond(response).map_err(|e| e.to_string())
}

fn parse_query_key(query: &str) -> Option<String> {
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if let Some(name) = it.next() {
            if name == "key" {
                if let Some(value) = it.next() {
                    return Some(percent_decode(value));
                }
            }
        }
    }
    None
}

fn percent_decode(input: &str) -> String {
    percent_decode_str(input)
        .decode_utf8()
        .map(|cow| cow.to_string())
        .unwrap_or_else(|_| input.to_string())
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).expect("valid header")
}
