# Local Codex agent bridge

AURA's `codex` command sends a bounded prompt to a locally signed-in Codex
app-server and prints the agent's final answer. It does not read or require an
OpenAI API key. The bridge speaks the app-server protocol (`initialize`,
`thread/start` or `thread/resume`, `turn/start`, `turn/interrupt`) as JSONL over
stdio or as WebSocket frames over a private Unix socket.

## Isolation

Codex runs as a read-only analyst, but a read-only sandbox only prevents
writes: shell commands can still read every file the app-server's account can
read, and MCP tools run outside the sandbox and ignore its network setting.
Evidence can carry text from peers and logs, so a prompt injection could make
the model read a secret and return it as its "diagnosis". The bridge is
therefore meant for a dedicated app-server:

- its own OS account, with read access only to intended evidence;
- its own `CODEX_HOME`, signed in, with no MCP servers or connectors;
- an empty private working directory (`--workdir`, mode 0700).

Do not point the bridge at an operator's own resident app-server; it is refused
when MCP servers are configured.

Before any turn runs, the bridge checks what the app-server actually applied,
because the app-server ignores unknown or misspelled request fields and falls
back to its own configuration. The request is refused unless:

- `mcpServerStatus/list` reports no MCP servers;
- the thread sandbox is `readOnly` without network access and the approval
  policy is `never`;
- no instruction file (for example `AGENTS.md`) was loaded into the thread,
  and the thread runs in the configured working directory.

Threads are started with web search disabled and project instruction files
turned off (`project_doc_max_bytes = 0`). Any server request that would grant a
tool, approval, input, or elicitation fails the turn. Requests that only ask
for data (token refresh, attestation) are declined and the turn continues.

## Turns and threads

A failed, interrupted, timed-out, or empty turn is an error; it never becomes a
healthy monitoring result. When a turn ends in any error, including the
timeout, the bridge sends `turn/interrupt` and waits for the app-server to
confirm it, so a resident server does not keep running the turn. A thread that
is still running an earlier turn when it is resumed has that turn interrupted
before the new one starts; otherwise `turn/start` would steer the old turn and
mix two sets of evidence.

The printed answer is the agent message marked `final_answer`. Commentary
(progress narration) is never part of it. Models that do not report a message
phase contribute their last message, never a concatenation of all of them.

`--thread-file` keeps a conversation's thread across requests. The file records
the thread ID and the number of completed turns, is replaced atomically, and is
locked while a turn runs. After `--max-thread-turns` completed turns (default
8) the next request starts a new thread, so earlier evidence stops shaping new
diagnoses; use one thread file per incident for independent analyses. A thread
that cannot be resumed is replaced by a new one.

## Usage

The dedicated app-server either listens on a private socket
(`--socket` / `AURA_CODEX_SOCKET`), or the bridge launches one per request with
`--spawn-app-server --codex-home <dedicated CODEX_HOME>`. Without one of these
the command fails.

Example, run as the dedicated analyst account:

```sh
install -d -m 700 "$HOME/.local/state/aura-codex" "$HOME/.local/state/aura-codex/work"
export AURA_CODEX_SOCKET="$HOME/.codex/app-server-control/app-server-control.sock"
export AURA_CODEX_WORKDIR="$HOME/.local/state/aura-codex/work"
export AURA_CODEX_THREAD_FILE="$HOME/.local/state/aura-codex/incident-42.thread"
aura codex --input-file /path/to/bounded-redacted-evidence.json \
  --output-schema /path/to/diagnosis.schema.json
```

The input file should contain AURA's already authorized, redacted evidence
with source IDs, clock quality, and missing fields. This command is a model
bridge, not an authorization grant: it does not create NHM broker grants or
read private Q/M databases. Keep the data acquisition and final diagnosis
schema validation in the calling monitoring workflow; the bridge only checks
that a schema-constrained answer is valid JSON. The bridge does not enable
automatic remediation or replace AURA's existing Rig providers.

Use `cargo test -p aura-cli --no-default-features codex_bridge` for the
transport contract tests. The CLI's no-default-features build includes this
command and avoids optional standalone agent dependencies.
