"""Tool behavior, independent of MCP transport and the installed client."""

import asyncio
from pathlib import Path

from .ledger import Ledger
from .rpc import AppServer, RpcError
from .worktrees import Worktree, WorktreeError


def nonempty(value: str, name: str, maximum: int = 100_000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must contain 1–{maximum} characters")


def absolute_directory(cwd: str):
    path = Path(cwd)
    if not path.is_absolute() or not path.is_dir():
        raise ValueError("cwd must be an existing absolute directory on the App Server host")
    return str(path.resolve())


DISPLAY_FIELDS = frozenset({"text", "preview", "summary", "objective", "aggregatedOutput"})
SANDBOX_POLICY_TYPES = {
    "read-only": "readOnly",
    "workspace-write": "workspaceWrite",
    "danger-full-access": "dangerFullAccess",
}


def validate_sandbox_policy(policy: dict):
    fields = {
        "readOnly": {"type", "networkAccess"},
        "dangerFullAccess": {"type"},
        "workspaceWrite": {
            "type",
            "networkAccess",
            "writableRoots",
            "excludeTmpdirEnvVar",
            "excludeSlashTmp",
        },
    }
    if set(policy) != fields.get(policy.get("type")):
        raise ValueError("Expected sandbox policy must contain all and only its protocol fields")
    for key in ("networkAccess", "excludeTmpdirEnvVar", "excludeSlashTmp"):
        if key in policy and type(policy[key]) is not bool:
            raise ValueError("Expected sandbox policy flags must be booleans")
    if "writableRoots" in policy and (
        not isinstance(policy["writableRoots"], list)
        or any(not isinstance(p, str) or not Path(p).is_absolute() for p in policy["writableRoots"])
    ):
        raise ValueError("Expected sandbox policy writableRoots must be absolute paths")


def validate_workspace_roots(policy):
    for value in policy.get("writableRoots", []):
        path = Path(value)
        if (path.exists() or path.is_symlink()) and not path.is_dir():
            raise ValueError("writableRoots must be directories, not files, sockets or devices")


def validate_execution_policy(approval_policy, approvals_reviewer):
    if approval_policy not in {"never", "on-request"}:
        raise ValueError("Supported policies are never or on-request with App Server Auto-review")
    if approvals_reviewer not in {"user", "auto_review"}:
        raise ValueError("Unsupported approvals reviewer")
    if approval_policy != "never" and approvals_reviewer != "auto_review":
        raise ValueError("Interactive client approvals unsupported; use App Server Auto-review")


def identity(settings):
    return {
        "thread_id": settings["thread"]["id"],
        "cwd": settings["cwd"],
        "model": settings["model"],
        "reasoning_effort": settings["reasoningEffort"],
    }


def clipped(value, limit: int, *, display_text: bool = False):
    """Bound display content while preserving opaque protocol fields verbatim."""
    if display_text and isinstance(value, str) and len(value) > limit:
        return value[:limit] + f"\n[truncated; original length {len(value)} characters]"
    if isinstance(value, list):
        return [clipped(item, limit, display_text=display_text) for item in value]
    if isinstance(value, dict):
        return {
            key: clipped(item, limit, display_text=key in DISPLAY_FIELDS)
            for key, item in value.items()
        }
    return value


class Bridge:
    def __init__(self, rpc: AppServer, ledger: Ledger):
        self.rpc = rpc
        self.ledger = ledger
        self._mutation_lock = asyncio.Lock()

    async def capabilities(self):
        await self.rpc.connect()
        return {
            "server": self.rpc.info,
            "transport": "same-host Unix WebSocket",
            "socket": str(self.rpc.socket_path),
            "capabilities": {
                "createThread": True,
                "sendMessage": True,
                "steerActiveTurn": True,
                "listReadWait": True,
                "goalRead": True,
                "goalSet": False,
                "desktopManagedWorktrees": False,
                "bridgeManagedWorktrees": True,
                "desktopProjectRegistry": False,
                "clientSideToolsAndApprovals": False,
            },
            "desktopVisibility": "Observed on Codex 0.153.4 with an existing project checkout; "
            "verify actual Desktop listing for each launch. Backend project IDs are separate.",
        }

    async def _mutate(
        self, request_id, method, params, action, *, validate_fresh=None, legacy_params=None
    ):
        async with self._mutation_lock:
            retained = self.ledger.lookup(request_id, method, params, legacy_params=legacy_params)
            if retained is not None:
                return {**retained, "replayed": True}
            if validate_fresh is not None:
                validate_fresh()
            fresh, receipt = self.ledger.begin(
                request_id, method, params, legacy_params=legacy_params
            )
            if not fresh:
                return {**receipt, "replayed": True}
            try:
                await action(receipt)
                receipt["status"] = "accepted"
            except RpcError as error:
                receipt.update(status="failed", error=str(error), rpcError=error.error)
            except WorktreeError as error:
                receipt.update(status="failed", error=str(error))
            except asyncio.CancelledError:
                self.ledger.save({**receipt, "status": "outcome_unknown"})
                raise
            except Exception as error:
                receipt.update(status="outcome_unknown", error=f"{type(error).__name__}: {error}")
            return self.ledger.save(receipt)

    async def create_thread(
        self,
        request_id: str,
        cwd: str,
        prompt: str | None = None,
        title: str | None = None,
        sandbox: str = "read-only",
        model: str | None = None,
        app_server_project_id: str | None = None,
        sandbox_policy: dict | None = None,
        approval_policy: str = "never",
        approvals_reviewer: str = "auto_review",
        reasoning_effort: str | None = None,
    ):
        validate_execution_policy(approval_policy, approvals_reviewer)
        if reasoning_effort is not None:
            nonempty(reasoning_effort, "reasoning_effort", 128)
        if sandbox_policy is not None:
            validate_sandbox_policy(sandbox_policy)
            if sandbox_policy["type"] != SANDBOX_POLICY_TYPES.get(sandbox):
                raise ValueError("sandbox and sandbox_policy.type must agree")
        nonempty(cwd, "cwd")
        if not Path(cwd).is_absolute():
            raise ValueError("cwd must be an existing absolute directory on the App Server host")
        if sandbox not in SANDBOX_POLICY_TYPES:
            raise ValueError("Unsupported sandbox")
        for name, value in [("prompt", prompt), ("title", title), ("model", model)]:
            if value is not None:
                nonempty(value, name, 100_000 if name == "prompt" else 500)
        params = {"cwd": cwd, "sandbox": sandbox, "approvalPolicy": "never", "ephemeral": False}
        if approval_policy != "never":
            params.update(approvalPolicy=approval_policy, approvalsReviewer=approvals_reviewer)
        if model is not None:
            params["model"] = model
        if app_server_project_id is not None:
            nonempty(app_server_project_id, "app_server_project_id", 128)
            params["projectId"] = app_server_project_id

        config_overrides: dict[str, object] = {}
        if sandbox_policy is not None:
            params["approvalsReviewer"] = approvals_reviewer
            if sandbox_policy["type"] == "workspaceWrite":
                config_overrides = {
                    "sandbox_workspace_write": {
                        "writable_roots": sandbox_policy["writableRoots"],
                        "network_access": sandbox_policy["networkAccess"],
                        "exclude_tmpdir_env_var": sandbox_policy["excludeTmpdirEnvVar"],
                        "exclude_slash_tmp": sandbox_policy["excludeSlashTmp"],
                    }
                }
            elif sandbox_policy["type"] == "readOnly" and sandbox_policy["networkAccess"]:
                raise ValueError(
                    "Network-enabled read-only creation unsupported; use a workspace policy"
                )
        if reasoning_effort is not None:
            config_overrides["model_reasoning_effort"] = reasoning_effort
        if config_overrides:
            params["config"] = config_overrides
        launch_params = dict(params)
        request_params = {**params, "prompt": prompt, "title": title}
        if sandbox_policy is not None:
            request_params["sandbox_policy"] = sandbox_policy
        request_params["approvals_reviewer"] = approvals_reviewer

        def legacy_params(version):
            previous = dict(request_params)
            if sandbox_policy is None:
                previous.pop("approvals_reviewer")
            yield previous
            # Try the original spelling before resolving a possibly changed path.
            # Only version 1 also accepted resolved cwd; version 2 used the supplied path.
            if version == 1:
                yield {**previous, "cwd": str(Path(cwd).resolve())}

        def validate_fresh():
            # The fingerprint uses the supplied path, not mutable symlink resolution.
            launch_params["cwd"] = absolute_directory(cwd)
            if sandbox_policy is not None:
                validate_workspace_roots(sandbox_policy)

        async def action(receipt):
            if app_server_project_id is not None:
                await self.rpc.call("project/read", {"projectId": app_server_project_id})
            created = await self.rpc.call("thread/start", launch_params)
            thread_id = created["thread"]["id"]
            receipt.update(threadId=thread_id, creation=created)
            self.ledger.save(receipt)  # Retain the ID even if naming or the first turn fails.
            actual = created.get("sandbox", {}).get("type")
            expected = SANDBOX_POLICY_TYPES[sandbox]
            if (
                created.get("cwd") != launch_params["cwd"]
                or created.get("approvalPolicy") != approval_policy
                or actual != expected
                or (model is not None and created.get("model") != model)
                or (
                    reasoning_effort is not None
                    and created.get("reasoningEffort") != reasoning_effort
                )
            ):
                raise RpcError(
                    "thread/start",
                    {
                        "code": "environment_mismatch",
                        "message": "Created environment differs; initial prompt withheld. "
                        "Inspect creation receipt. The thread remains retained.",
                    },
                )
            if (sandbox_policy is not None and created.get("sandbox") != sandbox_policy) or (
                (sandbox_policy is not None or approval_policy != "never")
                and created.get("approvalsReviewer") != approvals_reviewer
            ):
                raise RpcError(
                    "thread/start",
                    {
                        "code": "environment_mismatch",
                        "message": "Effective permission policy differs; prompt withheld",
                    },
                )
            if sandbox_policy is not None:
                receipt["permissionsAfter"] = created
                receipt["permissionUpdateState"] = "verified"
                self.ledger.save(receipt)
            if title is not None:
                await self.rpc.call("thread/name/set", {"threadId": thread_id, "name": title})
                receipt["title"] = title
                self.ledger.save(receipt)
            if prompt is not None:
                turn = await self.rpc.call(
                    "turn/start",
                    {
                        "threadId": thread_id,
                        "input": [],
                        "toolOutput": {
                            "name": "create_thread",
                            "namespace": "codex_thread_bridge",
                            "output": prompt,
                        },
                    },
                )
                receipt["turnId"] = turn["turn"]["id"]
            receipt["desktopProjectAssociation"] = "unverified; check Desktop listing"

        return await self._mutate(
            request_id,
            "create_thread",
            request_params,
            action,
            validate_fresh=validate_fresh,
            legacy_params=legacy_params,
        )

    async def create_worktree_thread(
        self,
        request_id: str,
        source_repository: str,
        starting_revision: str,
        destination: str,
        worktree_mode: str,
        sandbox: str,
        expected_sandbox_policy: dict,
        prompt: str | None = None,
        title: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        app_server_project_id: str | None = None,
    ):
        if worktree_mode != "bridge-managed-retained":
            raise ValueError("Explicit bridge-managed-retained worktree ownership is required")
        validate_sandbox_policy(expected_sandbox_policy)
        if (
            sandbox not in SANDBOX_POLICY_TYPES
            or expected_sandbox_policy.get("type") != SANDBOX_POLICY_TYPES[sandbox]
        ):
            raise ValueError("sandbox and expected_sandbox_policy.type must agree")
        for name, value in [
            ("source_repository", source_repository),
            ("starting_revision", starting_revision),
            ("destination", destination),
            ("prompt", prompt),
            ("title", title),
            ("model", model),
            ("reasoning_effort", reasoning_effort),
            ("app_server_project_id", app_server_project_id),
        ]:
            if value is not None:
                nonempty(value, name)
        params = {
            "source_repository": source_repository,
            "starting_revision": starting_revision,
            "destination": destination,
            "worktree_mode": worktree_mode,
            "sandbox": sandbox,
            "expected_sandbox_policy": expected_sandbox_policy,
            "prompt": prompt,
            "title": title,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "app_server_project_id": app_server_project_id,
        }

        async def action(receipt):
            def checkpoint(phase, **fields):
                receipt.update(phase=phase, **fields)
                self.ledger.save(receipt)

            checkpoint(
                "validating",
                recoveryRequired=True,
                recovery="Inspect this receipt, the destination and Git worktree list, and "
                "backend/Desktop tasks before manual recovery. Retain all artifacts; do not "
                "retry with a new request ID. Unknown thread/turn outcomes need reconciliation.",
                requestedCheckout=destination,
                initialPrompt={"state": "not_sent" if prompt is not None else "not_requested"},
                desktopProjectAssociation={
                    "status": "unverified",
                    "sourceRepository": source_repository,
                },
            )
            worktree = await Worktree.validate(source_repository, starting_revision, destination)
            if app_server_project_id is not None:
                await self.rpc.call("project/read", {"projectId": app_server_project_id})
            checkpoint("reserving_destination", worktree=worktree.receipt())
            worktree.reserve()
            receipt["worktree"]["state"] = "reserved"
            checkpoint("creating_worktree")
            await worktree.create()
            receipt["worktree"]["state"] = "registered"
            checkpoint("checking_out_worktree")
            await worktree.checkout()
            receipt["worktree"]["state"] = "created"
            checkpoint("checking_worktree")
            actual = await worktree.inspect()
            receipt["worktree"].update(actual)
            checkpoint("worktree_checked")
            if not worktree.matches(actual):
                raise WorktreeError("Worktree placement/base mismatch; initial prompt withheld")

            launch = {
                "cwd": str(worktree.destination),
                "sandbox": sandbox,
                "approvalPolicy": "never",
                "ephemeral": False,
                "runtimeWorkspaceRoots": [str(worktree.destination)],
            }
            if model is not None:
                launch["model"] = model
            if reasoning_effort is not None:
                launch["config"] = {"model_reasoning_effort": reasoning_effort}
            if app_server_project_id is not None:
                launch["projectId"] = app_server_project_id
            checkpoint("creating_thread")
            created = await self.rpc.call("thread/start", launch)
            checkpoint(
                "checking_environment",
                threadId=created["thread"]["id"],
                creation=created,
                permissionReceipt={
                    key: created.get(key)
                    for key in (
                        "approvalPolicy",
                        "sandbox",
                        "activePermissionProfile",
                        "runtimeWorkspaceRoots",
                    )
                },
                desktopProjectAssociation={
                    "status": "unverified",
                    "sourceRepository": source_repository,
                    "checkout": created.get("cwd"),
                    "appServerProjectId": created["thread"].get("projectId"),
                },
            )
            if (
                created.get("cwd") != str(worktree.destination)
                or created["thread"].get("cwd") != str(worktree.destination)
                or created.get("runtimeWorkspaceRoots") != [str(worktree.destination)]
                or created.get("approvalPolicy") != "never"
                or created.get("sandbox") != expected_sandbox_policy
                or (model is not None and created.get("model") != model)
                or (
                    reasoning_effort is not None
                    and created.get("reasoningEffort") != reasoning_effort
                )
                or (
                    app_server_project_id is not None
                    and created["thread"].get("projectId") != app_server_project_id
                )
            ):
                raise WorktreeError(
                    "Created environment differs; initial prompt withheld. Inspect creation receipt"
                )
            if title is not None:
                checkpoint("naming_thread")
                await self.rpc.call(
                    "thread/name/set", {"threadId": receipt["threadId"], "name": title}
                )
                checkpoint("thread_named", title=title)
            # Thread startup and naming can take time; recheck placement just before dispatch.
            actual = await worktree.inspect()
            checkpoint("checking_before_dispatch", checkoutBeforeDispatch=actual)
            if not worktree.matches(actual):
                raise WorktreeError(
                    "Worktree changed during thread startup; initial prompt withheld"
                )
            if prompt is not None:
                checkpoint("dispatching_initial_prompt", initialPrompt={"state": "outcome_unknown"})
                turn = await self.rpc.call(
                    "turn/start",
                    {
                        "threadId": receipt["threadId"],
                        "input": [],
                        "toolOutput": {
                            "name": "create_worktree_thread",
                            "namespace": "codex_thread_bridge",
                            "output": prompt,
                        },
                    },
                )
                checkpoint(
                    "initial_prompt_accepted",
                    turnId=turn["turn"]["id"],
                    initialPrompt={"state": "accepted"},
                )
            checkpoint("complete", recoveryRequired=False)

        return await self._mutate(request_id, "create_worktree_thread", params, action)

    async def _idle_settings(self, thread_id):
        state = (await self.rpc.call("thread/read", {"threadId": thread_id}))["thread"]
        if state.get("status", {}).get("type") not in {"idle", "notLoaded"}:
            raise RpcError(
                "thread/read",
                {"code": "thread_busy", "message": "Task is not idle; no settings changed"},
            )
        settings = await self.rpc.call(
            "thread/resume", {"threadId": thread_id, "excludeTurns": True}
        )
        if settings["thread"].get("status", {}).get("type") != "idle":
            raise RpcError(
                "thread/resume",
                {"code": "thread_busy", "message": "Task became active; no settings changed"},
            )
        return settings

    async def _apply_permissions(
        self,
        receipt,
        thread_id,
        sandbox_policy,
        approval_policy,
        approvals_reviewer,
        expected_identity,
    ):
        before = await self._idle_settings(thread_id)
        receipt["permissionsBefore"] = before
        self.ledger.save(receipt)
        if identity(before) != expected_identity:
            raise RpcError(
                "thread/resume",
                {
                    "code": "identity_mismatch",
                    "message": "Task identity/settings differ; no update sent",
                },
            )
        state = (await self.rpc.call("thread/read", {"threadId": thread_id}))["thread"]
        if state.get("status", {}).get("type") != "idle":
            raise RpcError(
                "thread/read",
                {"code": "thread_busy", "message": "Task became active; no update sent"},
            )
        receipt["permissionUpdateState"] = "outcome_unknown"
        self.ledger.save(receipt)
        await self.rpc.call(
            "thread/settings/update",
            {
                "threadId": thread_id,
                "sandboxPolicy": sandbox_policy,
                "approvalPolicy": approval_policy,
                "approvalsReviewer": approvals_reviewer,
            },
        )
        after = await self.rpc.call("thread/resume", {"threadId": thread_id, "excludeTurns": True})
        receipt["permissionsAfter"] = after
        self.ledger.save(receipt)
        if (
            identity(after) != expected_identity
            or after.get("runtimeWorkspaceRoots") != before.get("runtimeWorkspaceRoots")
            or after.get("sandbox") != sandbox_policy
            or after.get("approvalPolicy") != approval_policy
            or after.get("approvalsReviewer") != approvals_reviewer
        ):
            raise RpcError(
                "thread/settings/update",
                {
                    "code": "permission_verification_failed",
                    "message": "Effective settings differ; inspect receipt before further actions",
                },
            )
        receipt["permissionUpdateState"] = "verified"

    async def update_thread_permissions(
        self,
        request_id,
        thread_id,
        sandbox_policy,
        expected_identity,
        approval_policy="never",
        approvals_reviewer="auto_review",
    ):
        nonempty(thread_id, "thread_id", 128)
        validate_sandbox_policy(sandbox_policy)
        validate_execution_policy(approval_policy, approvals_reviewer)
        if (
            set(expected_identity) != {"thread_id", "cwd", "model", "reasoning_effort"}
            or expected_identity["thread_id"] != thread_id
        ):
            raise ValueError(
                "Expected identity must contain this thread_id, cwd, model, reasoning_effort"
            )
        params = {
            "threadId": thread_id,
            "sandboxPolicy": sandbox_policy,
            "expectedIdentity": expected_identity,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer,
        }

        async def action(receipt):
            receipt["threadId"] = thread_id
            await self._apply_permissions(
                receipt,
                thread_id,
                sandbox_policy,
                approval_policy,
                approvals_reviewer,
                expected_identity,
            )

        return await self._mutate(
            request_id,
            "update_thread_permissions",
            params,
            action,
            validate_fresh=lambda: validate_workspace_roots(sandbox_policy),
        )

    async def send_message_to_thread(self, request_id: str, thread_id: str, message: str):
        nonempty(thread_id, "thread_id", 128)
        nonempty(message, "message")

        async def action(receipt):
            receipt["threadId"] = thread_id
            self.ledger.save(receipt)
            state = await self.rpc.call("thread/read", {"threadId": thread_id})
            if state["thread"].get("status", {}).get("type") == "active":
                raise RpcError(
                    "thread/read",
                    {
                        "code": "thread_busy",
                        "message": "Thread is active; message withheld. Wait for completion.",
                    },
                )
            # Resume is an explicit part of messaging, never part of discovery.
            # No cwd, model, sandbox, or reasoning overrides are supplied.
            resumed = await self.rpc.call(
                "thread/resume",
                {
                    "threadId": thread_id,
                    "excludeTurns": True,
                },
            )
            receipt["resumed"] = resumed
            self.ledger.save(receipt)
            if resumed.get("approvalPolicy") != "never" and not (
                resumed.get("approvalPolicy") == "on-request"
                and resumed.get("approvalsReviewer") == "auto_review"
            ):
                raise RpcError(
                    "thread/resume",
                    {
                        "code": "unsupported_approval_policy",
                        "message": "Client-side approvals unsupported; message withheld. "
                        "Continue the thread in Desktop.",
                    },
                )
            turn = await self.rpc.call(
                "turn/start",
                {
                    "threadId": thread_id,
                    "input": [],
                    "toolOutput": {
                        "name": "send_message_to_thread",
                        "namespace": "codex_thread_bridge",
                        "output": message,
                    },
                },
            )
            receipt["turnId"] = turn["turn"]["id"]

        return await self._mutate(
            request_id,
            "send_message_to_thread",
            {"threadId": thread_id, "message": message},
            action,
        )

    async def steer_thread(
        self, request_id: str, thread_id: str, expected_turn_id: str, message: str
    ):
        nonempty(thread_id, "thread_id", 128)
        nonempty(expected_turn_id, "expected_turn_id", 128)
        nonempty(message, "message")

        async def action(receipt):
            receipt["threadId"] = thread_id
            receipt["expectedTurnId"] = expected_turn_id
            self.ledger.save(receipt)
            result = await self.rpc.call(
                "turn/steer",
                {
                    "threadId": thread_id,
                    "expectedTurnId": expected_turn_id,
                    "input": [{"type": "text", "text": message}],
                },
            )
            receipt["turnId"] = result["turnId"]
            if receipt["turnId"] != expected_turn_id:
                raise ValueError(
                    "App Server returned a different turn ID; delivery outcome unknown"
                )

        return await self._mutate(
            request_id,
            "steer_thread",
            {"threadId": thread_id, "expectedTurnId": expected_turn_id, "message": message},
            action,
        )

    async def get_goal(self, thread_id: str):
        nonempty(thread_id, "thread_id", 128)
        return clipped(await self.rpc.call("thread/goal/get", {"threadId": thread_id}), 4000)

    async def list_threads(self, cwd=None, limit=20, cursor=None):
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        params = {"limit": limit, "useStateDbOnly": True}
        if cwd is not None:
            params["cwd"] = absolute_directory(cwd)
        if cursor is not None:
            params["cursor"] = cursor
        return clipped(await self.rpc.call("thread/list", params), 4000)

    async def read_thread(self, thread_id: str, limit=10, cursor=None, max_text_chars=4000):
        nonempty(thread_id, "thread_id", 128)
        if not 1 <= limit <= 100 or not 100 <= max_text_chars <= 20_000:
            raise ValueError("limit must be 1–100 and max_text_chars must be 100–20000")
        metadata = await self.rpc.call("thread/read", {"threadId": thread_id})
        params = {"threadId": thread_id, "limit": limit, "itemsView": "full"}
        if cursor is not None:
            params["cursor"] = cursor
        page = await self.rpc.call("thread/turns/list", params)
        return clipped({"thread": metadata["thread"], "turnsPage": page}, max_text_chars)

    async def wait_thread(self, thread_id: str, turn_id: str, timeout_seconds=20):
        nonempty(thread_id, "thread_id", 128)
        nonempty(turn_id, "turn_id", 128)
        if not 0 <= timeout_seconds <= 50:
            raise ValueError("timeout_seconds must be between 0 and 50")
        latest = None

        async def inspect():
            # Search recent turns only; never confuse a different completed turn with the target.
            page = await self.rpc.call(
                "thread/turns/list",
                {
                    "threadId": thread_id,
                    "limit": 100,
                    "itemsView": "summary",
                },
            )
            return next((turn for turn in page["data"] if turn["id"] == turn_id), None)

        if timeout_seconds == 0:
            latest = await inspect()
        else:
            try:
                async with asyncio.timeout(timeout_seconds):
                    while True:
                        latest = await inspect()
                        if latest and latest.get("status") in {
                            "completed",
                            "failed",
                            "interrupted",
                        }:
                            break
                        await asyncio.sleep(0.5)
            except TimeoutError:
                pass
        terminal = latest is not None and latest.get("status") in {
            "completed",
            "failed",
            "interrupted",
        }
        return clipped(
            {
                "threadId": thread_id,
                "turnId": turn_id,
                "timedOut": not terminal,
                "turn": latest,
                "observation": "found" if latest else "not observed in latest 100 turns",
            },
            4000,
        )
