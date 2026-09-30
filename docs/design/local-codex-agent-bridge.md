# Local Codex agent bridge

AURA's `codex` command sends a bounded prompt to a locally signed-in Codex
app-server. It does not read or require an OpenAI API key. For a resident
app-server, set `AURA_CODEX_SOCKET` to its private Unix socket. Unix app-server
connections use WebSocket frames over the socket; a local child app-server uses
JSONL over stdio when no socket is configured. Both paths use the app-server's
`initialize`, `thread/start` or `thread/resume`, and `turn/start` protocol.

The bridge keeps each conversation in a private `AURA_CODEX_THREAD_FILE`.
The file holds only the Codex thread ID and is locked while a turn runs. The
Codex session and login stay under the local Codex account. The thread uses
read-only sandboxing, no network access, and no approval prompts. Codex may
still use local read-only tools, so run the app-server under an account with
access only to intended evidence. A failed,
interrupted, timed-out, or empty turn is an error; it never becomes a healthy
monitoring result. Prompt, protocol line, response, and turn time are capped.

Example for a local operator, with an existing private app-server socket:

```sh
install -d -m 700 "$HOME/.local/state/aura-codex"
export AURA_CODEX_SOCKET="$HOME/.codex/app-server-control/app-server-control.sock"
export AURA_CODEX_THREAD_FILE="$HOME/.local/state/aura-codex/validator.thread"
aura codex --input-file /path/to/bounded-redacted-evidence.json \
  --output-schema /path/to/diagnosis.schema.json
```

The input file should contain AURA's already authorized, redacted evidence
with source IDs, clock quality, and missing fields. This command is a model
bridge, not an authorization grant: it does not create NHM broker grants or
read private Q/M databases. Keep the data acquisition and final diagnosis
schema validation in the calling monitoring workflow. The bridge does not
enable automatic remediation or replace AURA's existing Rig providers.

Use `cargo test -p aura-cli --no-default-features codex_bridge` for the
transport contract tests. The CLI's no-default-features build includes this
command and avoids optional standalone agent dependencies.
