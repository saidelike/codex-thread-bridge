# codex-thread-bridge

An MCP server for creating, messaging, and steering Codex tasks through a running
App Server on the same host.

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
| `wait_thread` | Wait up to 50 seconds for a specified recent turn |
| `get_goal` | Read persistent Goal state |
| `get_operation` | Read the retained receipt for a mutation request |

Tool schemas are exposed through MCP. Their definitions are in
[server.py](src/codex_thread_bridge/server.py).

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
