# Contributing

Use the development commands in the README. Keep the bridge independent of a
specific repository, skill collection, Desktop installation, or user's paths.

The modules have six responsibilities:

- `rpc.py`: multiplexed JSON-RPC over an existing Unix WebSocket connection.
- `ledger.py`: durable mutation receipts and duplicate-request suppression.
- `bridge.py`: behavior of the tools, including partial failures and bounded reads.
- `records.py`: faithful record envelopes, source-bound chunks and coverage disclosure.
- `worktrees.py`: validated, retained Git checkout preparation; no Desktop lifecycle.
- `server.py`: MCP schemas, lifecycle, and command-line configuration.

Changes to mutation handling need tests for response loss, cancellation, duplicate
request IDs, and partial completion. Preserve the distinction between a received
API response and completed agent work. Do not infer Desktop project membership
from an App Server project ID.

Use a fake server for automated tests. Live tests create real retained sessions
and use the configured model; run them only with explicit authorization and an
exact proposed prompt. Keep personal paths, thread IDs, transcripts, config files,
credentials, and operation databases out of commits.

When reporting bugs, include bridge and Codex versions, host OS, the failed API
method, and a redacted receipt. Check whether the failure occurs at MCP discovery,
connection, dispatch, or Desktop visibility before proposing a workaround.
