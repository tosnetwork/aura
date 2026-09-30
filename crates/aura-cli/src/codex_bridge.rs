//! Local Codex app-server client for AURA's signed-in, resident agent path.

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio_tungstenite::tungstenite::Message;

const MAX_INPUT: usize = 64 * 1024;
const MAX_LINE: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 64 * 1024;

struct Connection {
    reader: Box<dyn AsyncBufRead + Unpin>,
    writer: Box<dyn AsyncWrite + Unpin>,
    child: Option<Child>,
    relay: Option<tokio::task::JoinHandle<Result<()>>>,
}

#[derive(Args, Debug)]
pub struct CodexArgs {
    /// Prompt text. When absent, read it from stdin (up to 64 KiB).
    #[arg(long, conflicts_with = "input_file")]
    query: Option<String>,

    /// Read the prompt from a file instead of stdin.
    #[arg(long)]
    input_file: Option<PathBuf>,

    /// Existing app-server Unix socket. Otherwise launch a local app-server
    /// using the current user's saved Codex login.
    #[arg(long, env = "AURA_CODEX_SOCKET")]
    socket: Option<PathBuf>,

    /// Private file holding this AURA conversation's Codex thread ID.
    #[arg(long, env = "AURA_CODEX_THREAD_FILE")]
    thread_file: Option<PathBuf>,

    /// JSON Schema for the final assistant message.
    #[arg(long)]
    output_schema: Option<PathBuf>,

    /// Maximum time for the entire Codex turn.
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..=600))]
    timeout_seconds: u64,
}

pub fn run(args: &CodexArgs) -> Result<()> {
    let _thread_lock = args
        .thread_file
        .as_ref()
        .map(|path| ThreadLock::acquire(path))
        .transpose()?;
    let prompt = match (&args.query, &args.input_file) {
        (Some(query), _) => query.clone(),
        (None, Some(path)) => read_bounded(path, MAX_INPUT)?,
        (None, None) => {
            let mut input = String::new();
            io::stdin()
                .take((MAX_INPUT + 1) as u64)
                .read_to_string(&mut input)?;
            input
        }
    };
    ensure!(!prompt.trim().is_empty(), "Codex prompt is empty");
    ensure!(prompt.len() <= MAX_INPUT, "Codex prompt exceeds 64 KiB");
    let schema = args
        .output_schema
        .as_ref()
        .map(|path| -> Result<Value> { Ok(serde_json::from_str(&read_bounded(path, MAX_INPUT)?)?) })
        .transpose()?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let answer = rt
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(args.timeout_seconds),
                request(args, &prompt, schema),
            )
            .await
        })
        .context("Codex turn timed out")??;
    print!("{answer}");
    io::stdout().flush()?;
    Ok(())
}

fn read_bounded(path: &Path, limit: usize) -> Result<String> {
    let mut text = String::new();
    fs::File::open(path)
        .with_context(|| format!("opening {}", path.display()))?
        .take((limit + 1) as u64)
        .read_to_string(&mut text)?;
    ensure!(text.len() <= limit, "input exceeds {} bytes", limit);
    Ok(text)
}

async fn request(args: &CodexArgs, prompt: &str, schema: Option<Value>) -> Result<String> {
    let mut connection = if let Some(socket) = &args.socket {
        let metadata = fs::metadata(socket)?;
        ensure!(
            metadata.file_type().is_socket()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "Codex socket must be owned by this user and private"
        );
        let stream = UnixStream::connect(socket)
            .await
            .with_context(|| format!("connecting to Codex socket {}", socket.display()))?;
        let (websocket, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
            .await
            .context("Codex Unix WebSocket handshake failed")?;
        let (to_relay, from_client) = tokio::io::duplex(MAX_LINE + 1);
        let (to_client, from_relay) = tokio::io::duplex(MAX_LINE + 1);
        let relay = tokio::spawn(relay_websocket(websocket, from_client, to_client));
        Connection {
            reader: Box::new(BufReader::new(from_relay)),
            writer: Box::new(to_relay),
            child: None,
            relay: Some(relay),
        }
    } else {
        let mut process = Command::new("codex")
            .arg("app-server")
            .env_remove("OPENAI_API_KEY")
            .env_remove("CODEX_API_KEY")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .context("launching local Codex app-server")?;
        let read = process.stdout.take().context("Codex stdout unavailable")?;
        let write = process.stdin.take().context("Codex stdin unavailable")?;
        Connection {
            reader: Box::new(BufReader::new(read)),
            writer: Box::new(write),
            child: Some(process),
            relay: None,
        }
    };
    let result = converse(
        &mut *connection.reader,
        &mut *connection.writer,
        args,
        prompt,
        schema,
    )
    .await;
    if let Some(process) = connection.child.as_mut() {
        let _ = process.start_kill();
        let _ = process.wait().await;
    }
    if let Some(relay) = connection.relay {
        relay.abort();
    }
    result
}

async fn relay_websocket(
    websocket: tokio_tungstenite::WebSocketStream<UnixStream>,
    outbound: tokio::io::DuplexStream,
    mut inbound: tokio::io::DuplexStream,
) -> Result<()> {
    let (mut sender, mut receiver) = websocket.split();
    let mut outbound = BufReader::new(outbound).lines();
    loop {
        tokio::select! {
            line = outbound.next_line() => {
                let Some(line) = line? else { break };
                ensure!(line.len() <= MAX_LINE, "Codex request exceeds 1 MiB");
                sender.send(Message::Text(line.into())).await?;
            }
            frame = receiver.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        ensure!(text.len() <= MAX_LINE, "Codex response exceeds 1 MiB");
                        inbound.write_all(text.as_bytes()).await?;
                        inbound.write_all(b"\n").await?;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(error)) => return Err(error.into()),
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn send(writer: &mut (dyn AsyncWrite + Unpin), value: Value) -> Result<()> {
    writer
        .write_all(serde_json::to_string(&value)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn recv(reader: &mut (dyn AsyncBufRead + Unpin)) -> Result<Value> {
    let mut line = Vec::new();
    let size = reader
        .take((MAX_LINE + 1) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    ensure!(size > 0, "Codex app-server closed the connection");
    ensure!(
        size <= MAX_LINE && line.ends_with(b"\n"),
        "Codex protocol line exceeds 1 MiB"
    );
    serde_json::from_slice(&line).context("invalid Codex app-server JSON")
}

async fn response(reader: &mut (dyn AsyncBufRead + Unpin), id: i64) -> Result<Value> {
    loop {
        let message = recv(reader).await?;
        if message.get("id").and_then(Value::as_i64) != Some(id) {
            continue;
        }
        if let Some(error) = message.get("error") {
            bail!("Codex app-server error: {error}");
        }
        return message
            .get("result")
            .cloned()
            .context("Codex response has no result");
    }
}

async fn converse(
    reader: &mut (dyn AsyncBufRead + Unpin),
    writer: &mut (dyn AsyncWrite + Unpin),
    args: &CodexArgs,
    prompt: &str,
    schema: Option<Value>,
) -> Result<String> {
    send(writer, json!({"id":1,"method":"initialize","params":{"clientInfo":{"name":"aura-local-codex","title":"AURA local Codex","version":env!("CARGO_PKG_VERSION")}}})).await?;
    response(reader, 1).await?;
    send(writer, json!({"method":"initialized"})).await?;

    let old_thread = args
        .thread_file
        .as_ref()
        .map(|path| read_thread_id(path))
        .transpose()?
        .flatten();
    let method = if old_thread.is_some() {
        "thread/resume"
    } else {
        "thread/start"
    };
    let mut params = json!({"approvalPolicy":"never","sandbox":"read-only",
        "developerInstructions":"You are AURA's read-only analyst. Analyze only the supplied evidence. Do not run tools, modify files, contact services, or propose an automated action. State unknowns explicitly."});
    if let Some(id) = &old_thread {
        params["threadId"] = json!(id);
    } else {
        params["cwd"] = json!(std::env::current_dir()?.to_string_lossy());
    }
    send(writer, json!({"id":2,"method":method,"params":params})).await?;
    let thread = response(reader, 2).await?;
    let thread_id = thread
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .context("Codex thread ID missing")?;
    if let Some(id) = &old_thread {
        ensure!(id == thread_id, "Codex resumed a different thread");
    }
    let mut turn = json!({"threadId":thread_id,"input":[{"type":"text","text":prompt}],
        "approvalPolicy":"never","sandboxPolicy":{"type":"readOnly","networkAccess":false}});
    let enforce_json = schema.is_some();
    if let Some(schema) = schema {
        turn["outputSchema"] = schema;
    }
    send(writer, json!({"id":3,"method":"turn/start","params":turn})).await?;
    let start = response(reader, 3).await?;
    let turn_id = start
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .context("Codex turn ID missing")?;
    let mut streamed = String::new();
    loop {
        let event = recv(reader).await?;
        if event.get("id").is_some() && event.get("method").is_some() {
            bail!("Codex requested an interactive tool or approval");
        }
        let Some(method) = event.get("method").and_then(Value::as_str) else {
            continue;
        };
        let params = &event["params"];
        if params.get("threadId").and_then(Value::as_str) != Some(thread_id) {
            continue;
        }
        if method == "item/agentMessage/delta"
            && params.get("turnId").and_then(Value::as_str) == Some(turn_id)
            && let Some(delta) = params.get("delta").and_then(Value::as_str)
        {
            ensure!(
                streamed.len() + delta.len() <= MAX_OUTPUT,
                "Codex output exceeds 64 KiB"
            );
            streamed.push_str(delta);
        }
        if method != "turn/completed"
            || params.pointer("/turn/id").and_then(Value::as_str) != Some(turn_id)
        {
            continue;
        }
        ensure!(
            params.pointer("/turn/status").and_then(Value::as_str) == Some("completed"),
            "Codex turn did not complete successfully"
        );
        let answer = params
            .pointer("/turn/items")
            .and_then(Value::as_array)
            .and_then(|items| {
                items
                    .iter()
                    .rev()
                    .find(|item| item.get("type").and_then(Value::as_str) == Some("agentMessage"))
            })
            .and_then(|item| item.get("text").and_then(Value::as_str))
            .unwrap_or(&streamed);
        ensure!(
            !answer.trim().is_empty(),
            "Codex completed without an answer"
        );
        ensure!(answer.len() <= MAX_OUTPUT, "Codex output exceeds 64 KiB");
        if enforce_json {
            serde_json::from_str::<Value>(answer).context("Codex answer is not valid JSON")?;
        }
        if let Some(path) = &args.thread_file {
            save_thread_id(path, thread_id)?;
        }
        return Ok(answer.to_string());
    }
}

fn read_thread_id(path: &Path) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::geteuid() }
            && metadata.permissions().mode() & 0o077 == 0,
        "Codex thread file must be private, regular, and owned by this user"
    );
    let mut id = String::new();
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?
        .take(129)
        .read_to_string(&mut id)?;
    ensure!(id.len() <= 128, "Codex thread file is too large");
    let id = id.trim().to_string();
    ensure!(
        uuid::Uuid::parse_str(&id).is_ok(),
        "invalid Codex thread ID"
    );
    Ok(Some(id))
}

fn save_thread_id(path: &Path, id: &str) -> Result<()> {
    if path.exists() {
        ensure!(
            read_thread_id(path)?.as_deref() == Some(id),
            "Codex thread file changed during the turn"
        );
        return Ok(());
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(id.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

struct ThreadLock(fs::File);

impl ThreadLock {
    fn acquire(path: &Path) -> Result<Self> {
        let lock_path = path.with_extension("thread.lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(lock_path)?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "Codex thread lock must be private and owned by this user"
        );
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        ensure!(result == 0, "Codex thread is already in use");
        Ok(Self(file))
    }
}

impl Drop for ThreadLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    fn args(thread_file: PathBuf) -> CodexArgs {
        CodexArgs {
            query: None,
            input_file: None,
            socket: None,
            thread_file: Some(thread_file),
            output_schema: None,
            timeout_seconds: 5,
        }
    }

    #[tokio::test]
    async fn completed_turn_returns_answer_and_saves_private_thread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("thread");
        let (client, server) = duplex(8192);
        let (client_read, mut client_write) = tokio::io::split(client);
        let mut client_read = BufReader::new(client_read);
        let (server_read, mut server_write) = tokio::io::split(server);
        let mut server_read = BufReader::new(server_read);
        let server = tokio::spawn(async move {
            let mut line = String::new();
            server_read.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"initialize\""));
            server_write
                .write_all(b"{\"id\":1,\"result\":{}}\n")
                .await
                .unwrap();
            line.clear();
            server_read.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"initialized\""));
            line.clear();
            server_read.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"sandbox\":\"read-only\""));
            server_write.write_all(b"{\"id\":2,\"result\":{\"thread\":{\"id\":\"019939ac-b636-70b1-a101-111111111111\"}}}\n").await.unwrap();
            line.clear();
            server_read.read_line(&mut line).await.unwrap();
            assert!(line.contains("\"networkAccess\":false"));
            server_write
                .write_all(b"{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-1\"}}}\n")
                .await
                .unwrap();
            server_write.write_all(b"{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"019939ac-b636-70b1-a101-111111111111\",\"turnId\":\"turn-1\",\"delta\":\"ok\"}}\n").await.unwrap();
            server_write.write_all(b"{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"019939ac-b636-70b1-a101-111111111111\",\"turn\":{\"id\":\"turn-1\",\"status\":\"completed\",\"items\":[]}}}\n").await.unwrap();
        });
        let answer = converse(
            &mut client_read,
            &mut client_write,
            &args(path.clone()),
            "hello",
            None,
        )
        .await
        .unwrap();
        assert_eq!(answer, "ok");
        assert_eq!(
            read_thread_id(&path).unwrap().as_deref(),
            Some("019939ac-b636-70b1-a101-111111111111")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_turn_does_not_save_thread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("thread");
        let (client, server) = duplex(8192);
        let (client_read, mut client_write) = tokio::io::split(client);
        let mut client_read = BufReader::new(client_read);
        let (server_read, mut server_write) = tokio::io::split(server);
        let mut server_read = BufReader::new(server_read);
        let server = tokio::spawn(async move {
            let mut line = String::new();
            for _ in 0..4 {
                line.clear();
                server_read.read_line(&mut line).await.unwrap();
                let reply = if line.contains("\"id\":1") {
                    Some("{\"id\":1,\"result\":{}}\n")
                } else if line.contains("\"id\":2") {
                    Some(
                        "{\"id\":2,\"result\":{\"thread\":{\"id\":\"019939ac-b636-70b1-a101-111111111111\"}}}\n",
                    )
                } else if line.contains("\"id\":3") {
                    Some("{\"id\":3,\"result\":{\"turn\":{\"id\":\"turn-1\"}}}\n")
                } else {
                    None
                };
                if let Some(reply) = reply {
                    server_write.write_all(reply.as_bytes()).await.unwrap();
                }
            }
            server_write.write_all(b"{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"019939ac-b636-70b1-a101-111111111111\",\"turn\":{\"id\":\"turn-1\",\"status\":\"failed\",\"items\":[]}}}\n").await.unwrap();
        });
        let error = converse(
            &mut client_read,
            &mut client_write,
            &args(path.clone()),
            "hello",
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("did not complete"));
        assert!(!path.exists());
        server.await.unwrap();
    }
}
