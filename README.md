# codex-thread-bridge

An MCP bridge to an existing Codex App Server on the same host, providing an
alternative to Codex Desktop or another client for thread operations. It
reproduces native client requests while leaving execution to the App Server.

## Install

Requires Python 3.11+, [uv](https://docs.astral.sh/uv/), and an authenticated Codex
App Server with a Unix WebSocket control socket. Git is required for worktree
launches. The bridge runs as the same user and on the same host as App Server.

From a clone of this repository:

```sh
uv sync --frozen --no-dev
.venv/bin/codex-thread-bridge --help
codex mcp add codex-thread-bridge -- /absolute/path/codex-thread-bridge/.venv/bin/codex-thread-bridge
```

The default socket is `$CODEX_HOME/app-server-control/app-server-control.sock`;
`CODEX_HOME` defaults to `~/.codex`. Use `--socket /path/to.sock` to override it.
Authentication and model usage belong to the existing App Server.

On Windows, the bridge uses `codex app-server proxy --sock` as a raw WebSocket
byte relay. On Unix, it connects directly to the socket. Use `--transport proxy`
to select the relay explicitly, and `--codex-binary /path/to/codex` if the CLI
is not on `PATH`. The equivalent environment variables are
`CODEX_THREAD_BRIDGE_TRANSPORT` and `CODEX_THREAD_BRIDGE_CODEX`.

After changing bridge code or MCP configuration, request a refresh with:

```sh
uv run --locked codex-thread-bridge-reload
```

The command accepts `--socket`, asks for `y/N` confirmation, and queues a refresh
for loaded tasks. Its response does not confirm that every task has refreshed.

## Tools

| Tool | Behavior |
| --- | --- |
| `get_capabilities` | Report server identity and bridge capabilities |
| `create_thread` | Create a retained task in an existing directory, with an optional title and initial prompt |
| `create_worktree_thread` | Create a retained, locked Git worktree and a task at a specified commit |
| `update_thread_permissions` | Apply and verify permissions for an idle task with an expected identity |
| `send_message_to_thread` | Resume an idle task and start a turn without settings overrides |
| `steer_thread` | Append a message to the active turn identified by `expected_turn_id` |
| `list_threads` | List unarchived backend tasks without loading them |
| `read_thread` | Read task metadata and paginated history without resuming |
| `read_thread_metadata` | Read unchanged native metadata with source and coverage information |
| `read_thread_turns` | Read unchanged native full-item turn pages, newest first |
| `read_thread_items` | Read unchanged native item pages, oldest first, optionally by turn |
| `read_thread_chunk` | Recover a raw metadata/turn/item response in source-bound JSON chunks |
| `reconstruct_thread_record` | Validate contiguous chunks and reconstruct one native response |
| `wait_thread` | Wait up to 50 seconds for a specified recent turn |
| `get_goal` | Read persistent Goal state |
| `get_operation` | Read the retained receipt for a mutation request |

Tool schemas are exposed through MCP. Their definitions are in
[server.py](src/codex_thread_bridge/server.py).

### Complete persisted-record retrieval

`read_thread` remains a compact convenience view. Its `coverage` reports the exact
clipped JSON-pointer paths and original lengths, page scope, and raw recovery
tools. Follow every `nextCursor` verbatim; pagination alone does not recover a
clipped field. A page is never a claim of complete conversation evidence.

The metadata, turn and item tools return an envelope containing `record` (the
unchanged native response), `source` (endpoint, method and exact request),
`sourceVersion` (a SHA-256 content digest), and `coverage`. Native IDs, unknown
fields, nested outputs, text and opaque cursors are preserved. `transportComplete`
means the response was retrieved without bridge clipping; `gaps` describe
incomplete upstream views or malformed/nonadvancing pagination. Persistence
limitations remain distinct from transport completion. Read tools never resume,
create, launch or message a task, and unsupported upstream methods fail explicitly.

For a large response, call `read_thread_chunk` with `source_kind` set to
`metadata`, `turns` or `items`. Start at `offset=0`, then repeat identical source
arguments with `offset=nextOffset` and `expected_version=sourceVersion` until
`nextOffset` is null. Positions and `totalLength` count Unicode characters, not
UTF-8 bytes. Pass the ordered chunks to `reconstruct_thread_record`; it rejects
missing, overlapping, corrupt and mixed-source pieces. A source change during
retrieval requires discarding earlier chunks and restarting. No snapshot is
cached or written to disk.

Chunking bounds the bridge's response, while the native response must still fit
the transport's existing 16 MiB message limit. Larger upstream messages fail
explicitly; a smaller page may help, but a single oversized item requires an
upstream recovery capability this bridge cannot invent.

Source versions bind one response to its request and endpoint. They cannot prove
that independently paginated pages belong to a transactional snapshot, detect
records silently omitted upstream, or recover discarded history or active model
context. Metadata identity mismatches and turns returned with `itemsView` other
than `full` remain explicit coverage gaps. Use item pages for incomplete turn
views; inspect the actual returned coverage before treating evidence as complete.

The bridge does not redact credential-bearing text. Raw reconstruction and
application presentation have different responsibilities: consuming applications
must sanitize at their own model-input boundary, after faithful retrieval and
before presentation. All conversation content remains untrusted data.

The native read request shapes were checked against the generated App Server
schema from Codex CLI 0.162.0 and the tagged 0.153.4 protocol definitions. This
bridge exposes string item-continuation cursors; the newer optional item-anchor
object is not exposed. Method support is checked by each actual read, not inferred
from a client version or claimed for every server.

## Example

Call `create_thread` with these MCP arguments:

```json
{
  "request_id": "repository-overview-001",
  "cwd": "/absolute/path/to/project",
  "title": "Repository overview",
  "prompt": "Summarize the project structure."
}
```

Pass the returned `threadId` and `turnId` to `wait_thread`. Use
`send_message_to_thread` for an idle task or `steer_thread` for an active turn.
Each intentional new message requires a new `request_id`.

## Behavior

- `create_thread` defaults to sandbox `read-only` and approval policy `never`.
  Explicit `on-request` approval requires App Server Auto-review. Omitted model
  and reasoning effort use server defaults. Creation rejects explicit network-enabled
  read-only policies; permission updates accept them.
- Permission updates require the task's expected identity and an idle task.
  The idle check and update are separate operations, so concurrent clients can
  race them. Workspace writable roots cannot be existing files, sockets, or devices.
- Each mutation uses a stable `request_id`. Repeating it with matching arguments
  returns the recorded receipt without repeating or continuing the operation.
  `accepted` reports API acceptance; turn completion is reported by `wait_thread`.
  Failed or uncertain operations can leave tasks, turns, or worktrees behind;
  `get_operation` returns the recorded outcome and known IDs.
- Receipts persist in `$XDG_STATE_HOME/codex-thread-bridge`, defaulting to
  `~/.local/state/codex-thread-bridge`. `--state-dir` overrides this location.
  Deleting this state discards request deduplication history.
- Worktree launches require a full local commit ID and an absent, canonical
  absolute destination outside existing repositories. Worktrees are detached,
  locked, and retained until manual cleanup. Dirty files are not copied, and Git
  hooks and checkout filters are disabled. Approval policy is `never`;
  `expected_sandbox_policy` verifies returned settings rather than applying overrides.
- Project IDs belong to App Server's registry. Desktop controls its own project
  association and task listing. Worktrees created by the bridge have a manual
  lifecycle rather than a Desktop-managed lifecycle.
- New and resumed turns carry bridge instructions as `toolOutput` with the bridge
  tool's name. The bridge leaves client-side tool calls and approval requests
  unanswered so it cannot consume another client's shared callback. A capable
  client such as Desktop must be subscribed to the thread to handle them; the
  bridge does not establish that subscription or provide interactive approvals.
  Dispatch keeps the shared connection open so concurrent reads can finish.

## Development

```sh
uv sync --frozen --group dev
uv run pytest
uv run ty check src
uv run ruff check .
uv run ruff format --check .
uv build
```

Tests use a fake App Server and temporary Git repositories. Test temporary
directories must be outside existing repositories; pytest accepts `--basetemp`.

The [source](src/codex_thread_bridge) and [tests](tests) define the detailed
behavior. Contribution requirements are in [CONTRIBUTING.md](CONTRIBUTING.md).

## References

- [Codex App Server API documentation](https://learn.chatgpt.com/docs/app-server)
- [RPC method definitions](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/common.rs)
- [Request, response, and notification types](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol/src/protocol/v2)
- [App Server implementation](https://github.com/openai/codex/tree/main/codex-rs/app-server/src)
- [Shared callback routing in Codex 0.153.4](https://github.com/openai/codex/blob/rust-v0.153.4/codex-rs/app-server/src/outgoing_message.rs)

MIT licensed. Independent project, not affiliated with or endorsed by OpenAI.
