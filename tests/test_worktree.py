# Tests intentionally inspect local temporary artifacts synchronously.
# ruff: noqa: ASYNC240
import asyncio
import json
import subprocess
import tempfile
from pathlib import Path

import pytest
from websockets.asyncio.server import unix_serve

from codex_thread_bridge.bridge import Bridge
from codex_thread_bridge.ledger import Ledger
from codex_thread_bridge.rpc import AppServer


def git(cwd, *args):
    return subprocess.check_output(
        ["git", "-C", str(cwd), *args], stderr=subprocess.PIPE, text=True
    ).strip()


@pytest.fixture
def repository(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init")
    git(source, "config", "user.email", "test@example.invalid")
    git(source, "config", "user.name", "Test")
    (source / "tracked").write_text("base\n")
    (source / ".gitignore").write_text("ignored\n")
    git(source, "add", ".")
    git(source, "commit", "-m", "base")
    revision = git(source, "rev-parse", "HEAD")
    (source / "tracked").write_text("staged\n")
    git(source, "add", "tracked")
    (source / "tracked").write_text("unstaged\n")
    (source / "untracked").write_text("untracked\n")
    (source / "ignored").write_text("ignored\n")
    return {
        "request_id": "isolated",
        "source_repository": str(source),
        "starting_revision": revision,
        "destination": str(tmp_path / "isolated"),
        "worktree_mode": "bridge-managed-retained",
        "sandbox": "read-only",
        "expected_sandbox_policy": {"type": "readOnly", "networkAccess": False},
    }


@pytest.mark.parametrize("operation", ["create", "worktree", "send"])
async def test_dispatch_does_not_disconnect_concurrent_read(repository, tmp_path, operation):
    read_received = asyncio.Event()
    release_read = asyncio.Event()

    async def handler(ws):
        held_read = None
        async for raw in ws:
            message = json.loads(raw)
            method, params = message["method"], message.get("params", {})
            if method == "initialized":
                continue
            if method == "thread/list":
                held_read = message["id"]
                read_received.set()
                continue
            result = {}
            if method == "thread/start":
                result = {
                    "thread": {"id": "thread", "cwd": params["cwd"]},
                    "cwd": params["cwd"],
                    "runtimeWorkspaceRoots": [params["cwd"]],
                    "approvalPolicy": "never",
                    "sandbox": {"type": "readOnly", "networkAccess": False},
                }
            elif method == "thread/read":
                result = {"thread": {"status": {"type": "idle"}}}
            elif method == "thread/resume":
                result = {"approvalPolicy": "never"}
            elif method == "turn/start":
                result = {"turn": {"id": "turn"}}
            await ws.send(json.dumps({"id": message["id"], "result": result}))
            if method == "turn/start":
                await release_read.wait()
                await ws.send(json.dumps({"id": held_read, "result": {"data": []}}))

    with tempfile.TemporaryDirectory(prefix="ctb-concurrent-") as directory:
        path = Path(directory) / "app.sock"
        async with unix_serve(handler, str(path)):
            rpc = AppServer(path, timeout=5)
            ledger = Ledger(tmp_path / "concurrent.sqlite3")
            bridge = Bridge(rpc, ledger)
            read = asyncio.create_task(bridge.list_threads())
            try:
                await asyncio.wait_for(read_received.wait(), 5)
                if operation == "create":
                    receipt = await bridge.create_thread("create", str(tmp_path), prompt="hello")
                elif operation == "worktree":
                    receipt = await bridge.create_worktree_thread(**repository, prompt="hello")
                else:
                    receipt = await bridge.send_message_to_thread("send", "thread", "hello")
                assert receipt["status"] == "accepted", receipt
                release_read.set()
                assert await read == {"data": []}
            finally:
                release_read.set()
                await rpc.close()
                await asyncio.gather(read, return_exceptions=True)
                ledger.close()


async def test_readiness_launch_retains_exact_base_without_carrying_dirty_changes(
    bridge, fake_server, repository
):
    from pathlib import Path

    source = Path(repository["source_repository"])
    before = git(source, "status", "--porcelain=v1", "--ignored")
    index_before = git(source, "diff", "--cached", "--binary")
    receipt = await bridge.create_worktree_thread(**repository)
    assert receipt["status"] == "accepted", receipt
    assert receipt["worktree"]["initialRevision"] == repository["starting_revision"]
    assert receipt["worktree"]["ownership"] == "bridge-managed"
    assert receipt["worktree"]["lifecycle"] == "retained-until-manual-cleanup"
    checkout = Path(receipt["worktree"]["checkout"])
    assert checkout == Path(repository["destination"])
    assert (checkout / "tracked").read_text() == "base\n"
    assert not (checkout / "untracked").exists() and not (checkout / "ignored").exists()
    assert git(source, "status", "--porcelain=v1", "--ignored") == before
    assert git(source, "diff", "--cached", "--binary") == index_before
    assert (source / "tracked").read_text() == "unstaged\n"
    assert (source / "untracked").read_text() == "untracked\n"
    assert (source / "ignored").read_text() == "ignored\n"
    assert "locked" in git(source, "worktree", "list", "--porcelain")
    assert receipt["creation"]["cwd"] == str(checkout)
    assert receipt["permissionReceipt"]["sandbox"] == repository["expected_sandbox_policy"]
    assert receipt["creation"]["model"] == "configured-default"
    assert receipt["desktopProjectAssociation"]["status"] == "unverified"
    assert (await bridge.read_thread(receipt["threadId"]))["turnsPage"]["data"] == []
    assert (await bridge.get_goal(receipt["threadId"]))["goal"] is None


@pytest.mark.parametrize("field", ["source_repository", "destination"])
@pytest.mark.parametrize("suffix", [" ", "\t", "\n\n"], ids=["space", "tab", "newlines"])
async def test_launch_preserves_trailing_whitespace_in_paths(bridge, repository, field, suffix):
    from pathlib import Path

    args = {**repository, field: repository[field] + suffix, "prompt": "READY"}
    if field == "source_repository":
        Path(repository[field]).rename(args[field])
    receipt = await bridge.create_worktree_thread(**args)
    assert receipt["status"] == "accepted", receipt
    assert receipt["worktree"]["sourceRepository"] == args["source_repository"]
    assert receipt["worktree"]["checkout"] == args["destination"]
    assert receipt["checkoutBeforeDispatch"]["checkout"] == args["destination"]
    assert receipt["creation"]["cwd"] == args["destination"]
    assert (Path(args["destination"]) / "tracked").read_text() == "base\n"
    assert (Path(args["source_repository"]) / "tracked").read_text() == "unstaged\n"
    repeated = await bridge.create_worktree_thread(**args)
    assert repeated["replayed"] and repeated["threadId"] == receipt["threadId"]
    turns = (await bridge.read_thread(receipt["threadId"]))["turnsPage"]["data"]
    assert len(turns) == 1 and turns[0]["items"][0]["text"] == "READY"


@pytest.mark.parametrize(
    "policy",
    [
        {"type": "readOnly"},
        {"type": "readOnly", "networkAccess": "false"},
        {"type": "readOnly", "networkAccess": False, "unknown": True},
    ],
)
async def test_invalid_permission_contract_has_no_filesystem_or_api_effects(
    bridge, fake_server, repository, policy
):
    from pathlib import Path

    with pytest.raises(ValueError, match="sandbox policy"):
        await bridge.create_worktree_thread(**{**repository, "expected_sandbox_policy": policy})
    assert not Path(repository["destination"]).exists()
    assert not fake_server[0].threads


@pytest.mark.parametrize(
    "collision", ["directory", "file", "symlink", "inside_source", "inside_other_repo"]
)
async def test_path_collisions_leave_existing_content_untouched(
    bridge, fake_server, repository, tmp_path, collision
):
    from pathlib import Path

    target = Path(repository["destination"])
    if collision == "directory":
        target.mkdir()
    elif collision == "file":
        target.write_text("preserve")
    elif collision == "symlink":
        target.symlink_to(tmp_path / "missing")
    elif collision == "inside_source":
        target = Path(repository["source_repository"]) / "nested"
    else:
        other = tmp_path / "other"
        other.mkdir()
        git(other, "init")
        target = other / "nested"
    receipt = await bridge.create_worktree_thread(**{**repository, "destination": str(target)})
    assert receipt["status"] == "failed", receipt
    assert not fake_server[0].threads
    if collision == "file":
        assert target.read_text() == "preserve"
    elif collision == "symlink":
        assert target.is_symlink()
    elif collision == "directory":
        assert list(target.iterdir()) == []
    else:
        assert not target.exists()


@pytest.mark.parametrize("revision", ["HEAD", "main", "HEAD~1", "a" * 12, "0" * 40])
async def test_only_available_full_commit_ids_are_accepted(
    bridge, fake_server, repository, revision
):
    from pathlib import Path

    receipt = await bridge.create_worktree_thread(**{**repository, "starting_revision": revision})
    assert receipt["status"] == "failed"
    assert not Path(repository["destination"]).exists()
    assert not fake_server[0].threads


@pytest.mark.parametrize("stage", ["thread/start", "thread/name/set", "turn/start"])
async def test_lost_responses_replay_after_restart_without_duplicate_artifacts(
    bridge, fake_server, repository, tmp_path, stage
):
    from pathlib import Path

    from codex_thread_bridge.bridge import Bridge
    from codex_thread_bridge.ledger import Ledger

    fake, _ = fake_server
    fake.drop_after = stage
    args = {**repository, "prompt": "  exact\ninitial  ", "title": "Retained"}
    first = await bridge.create_worktree_thread(**args)
    assert first["status"] == "outcome_unknown"
    assert first["recoveryRequired"]
    assert Path(first["worktree"]["checkout"]).is_dir()
    assert first["worktree"]["initialRevision"] == args["starting_revision"]
    if stage != "thread/start":
        assert first["threadId"] in fake.threads
    if stage == "turn/start":
        assert first["initialPrompt"]["state"] == "outcome_unknown"
    else:
        assert first["initialPrompt"]["state"] == "not_sent"
    worktrees = git(args["source_repository"], "worktree", "list", "--porcelain")
    calls_before = list(fake.calls)
    fake.drop_after = None
    ledger = Ledger(tmp_path / "state" / "operations.sqlite3")
    try:
        restarted = Bridge(bridge.rpc, ledger)
        repeated = await restarted.create_worktree_thread(**args)
        assert repeated["replayed"] and repeated["worktree"] == first["worktree"]
        assert ledger.get(args["request_id"])["status"] == "outcome_unknown"
        assert fake.calls == calls_before
        assert git(args["source_repository"], "worktree", "list", "--porcelain") == worktrees
        with pytest.raises(ValueError, match="different arguments"):
            await restarted.create_worktree_thread(**{**args, "prompt": "changed"})
    finally:
        ledger.close()


async def test_mcp_isolated_launch_and_followup_are_durable(fake_server, repository, tmp_path):
    import sys

    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    fake, socket = fake_server
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "mcp-state"),
        ],
    )
    args = {
        **repository,
        "prompt": "  exact\ninitial  ",
        "model": "explicit-model",
        "reasoning_effort": "high",
    }
    receipts = []
    for _ in range(2):
        async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("create_worktree_thread", args)
            assert not result.isError, result
            receipt = result.structuredContent
            assert receipt["status"] == "accepted", receipt
            receipts.append(receipt)
            followup = await session.call_tool(
                "send_message_to_thread",
                {"request_id": "followup", "thread_id": receipt["threadId"], "message": "FOLLOWUP"},
            )
            assert followup.structuredContent["status"] == "accepted"
            state = await session.call_tool("get_operation", {"request_id": args["request_id"]})
            assert state.structuredContent["worktree"] == receipt["worktree"]
    assert receipts[1]["replayed"]
    assert len(fake.threads) == 1
    thread = fake.threads[receipts[0]["threadId"]]
    assert [t["items"][0]["text"] for t in thread["turns"]] == [args["prompt"], "FOLLOWUP"]
    initial = next(params for method, params in fake.calls if method == "turn/start")
    assert initial["input"] == []
    assert initial["toolOutput"] == {
        "name": "create_worktree_thread",
        "namespace": "codex_thread_bridge",
        "output": args["prompt"],
    }
    assert receipts[0]["creation"]["reasoningEffort"] == "high"


async def test_git_hooks_filters_and_fsmonitor_are_not_run(bridge, repository, tmp_path):
    from pathlib import Path

    source = Path(repository["source_repository"])
    marker = tmp_path / "unexpected-effect"
    script = tmp_path / "side-effect"
    script.write_text(f"#!/bin/sh\ntouch '{marker}'\ncat\n")
    script.chmod(0o755)
    (source / ".gitattributes").write_text("tracked filter=example\n")
    git(source, "add", ".gitattributes")
    git(source, "commit", "-m", "attributes")
    args = {**repository, "starting_revision": git(source, "rev-parse", "HEAD")}
    hook = source / ".git" / "hooks" / "post-checkout"
    hook.write_bytes(script.read_bytes())
    hook.chmod(0o755)
    git(source, "config", "filter.example.smudge", str(script))
    git(source, "config", "filter.example.clean", str(script))
    git(source, "config", "filter.example.required", "true")
    git(source, "config", "core.fsmonitor", str(script))
    receipt = await bridge.create_worktree_thread(**args)
    assert receipt["status"] == "accepted", receipt
    assert not marker.exists()


@pytest.mark.parametrize("stage", ["thread/start", "turn/start"])
@pytest.mark.parametrize("interruption", ["cancel", "timeout"])
async def test_interrupted_dispatch_is_retained_and_never_repeated(
    bridge, fake_server, repository, stage, interruption
):
    import asyncio

    fake, _ = fake_server
    fake.pause_after = stage
    args = {**repository, "prompt": "READY"}
    if interruption == "timeout":
        bridge.rpc.timeout = 0.1
    task = asyncio.create_task(bridge.create_worktree_thread(**args))
    try:
        await asyncio.wait_for(fake.paused.wait(), 5)
        if interruption == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert (await task)["status"] == "outcome_unknown"
        receipt = bridge.ledger.get(args["request_id"])
        assert receipt["worktree"]["state"] == "created"
        assert receipt["status"] == "outcome_unknown"
        before = list(fake.calls)
        assert (await bridge.create_worktree_thread(**args))["replayed"]
        assert fake.calls == before
    finally:
        fake.release.set()


@pytest.mark.parametrize(
    "override",
    [
        {"cwd": "/wrong"},
        {"runtimeWorkspaceRoots": ["/wrong"]},
        {"approvalPolicy": "on-request"},
        {"sandbox": {"type": "readOnly", "networkAccess": True}},
        {"model": "different"},
        {"reasoningEffort": "different"},
    ],
)
async def test_environment_mismatch_retains_actual_receipt_and_withholds_prompt(
    bridge, fake_server, repository, override
):
    fake, _ = fake_server
    fake.override_creation = override
    receipt = await bridge.create_worktree_thread(
        **repository, prompt="WITHHOLD", model="expected", reasoning_effort="high"
    )
    assert receipt["status"] == "failed"
    assert receipt["initialPrompt"]["state"] == "not_sent"
    assert receipt["recoveryRequired"]
    assert all(receipt["creation"][k] == v for k, v in override.items())
    assert fake.threads[receipt["threadId"]]["turns"] == []


async def test_checkout_change_during_thread_start_withholds_prompt(
    bridge, fake_server, repository
):
    import asyncio

    fake, _ = fake_server
    fake.pause_after = "thread/start"
    task = asyncio.create_task(bridge.create_worktree_thread(**repository, prompt="WITHHOLD"))
    try:
        await asyncio.wait_for(fake.paused.wait(), 5)
        git(repository["destination"], "checkout", "-b", "unexpected")
    finally:
        fake.release.set()
    receipt = await task
    assert receipt["status"] == "failed"
    assert not receipt["checkoutBeforeDispatch"]["detached"]
    assert fake.threads[receipt["threadId"]]["turns"] == []


@pytest.mark.parametrize("git_stage", ["add", "checkout-index"])
async def test_cancellation_after_git_creation_retains_checkout_without_starting_task(
    bridge, fake_server, repository, tmp_path, monkeypatch, git_stage
):
    import asyncio
    import os
    import shutil
    import sys
    from pathlib import Path

    real_git = shutil.which("git")
    scripts = tmp_path / "bin"
    scripts.mkdir()
    marker = tmp_path / "git-created"
    wrapper = scripts / "git"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import os, subprocess, sys, time\n"
        "from pathlib import Path\n"
        "result = subprocess.run([os.environ['CTB_TEST_GIT'], *sys.argv[1:]])\n"
        "if result.returncode == 0 and os.environ['CTB_TEST_STAGE'] in sys.argv:\n"
        "    Path(os.environ['CTB_TEST_MARKER']).touch()\n"
        "    time.sleep(60)\n"
        "sys.exit(result.returncode)\n"
    )
    wrapper.chmod(0o755)
    monkeypatch.setenv("CTB_TEST_STAGE", git_stage)
    monkeypatch.setenv("CTB_TEST_GIT", real_git)
    monkeypatch.setenv("CTB_TEST_MARKER", str(marker))
    monkeypatch.setenv("PATH", str(scripts) + os.pathsep + os.environ["PATH"])
    task = asyncio.create_task(bridge.create_worktree_thread(**repository, prompt="WITHHOLD"))
    try:
        async with asyncio.timeout(5):
            while not marker.exists():  # noqa: ASYNC110 - observing a separate process
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    receipt = bridge.ledger.get(repository["request_id"])
    assert receipt["status"] == "outcome_unknown"
    assert receipt["phase"] == (
        "creating_worktree" if git_stage == "add" else "checking_out_worktree"
    )
    assert receipt["worktree"]["requestedRevision"] == repository["starting_revision"]
    tracked = Path(repository["destination"]) / "tracked"
    if git_stage == "add":
        assert not tracked.exists()
    else:
        assert tracked.read_text() == "base\n"
    assert not fake_server[0].threads
    assert (await bridge.create_worktree_thread(**repository, prompt="WITHHOLD"))["replayed"]


async def test_known_thread_failure_retains_worktree_and_prevents_retry(
    bridge, fake_server, repository
):
    from pathlib import Path

    fake, _ = fake_server
    fake.reject["thread/start"] = {"code": -32602, "message": "rejected"}
    receipt = await bridge.create_worktree_thread(**repository, prompt="WITHHOLD")
    assert receipt["status"] == "failed"
    assert receipt["phase"] == "creating_thread"
    assert Path(receipt["worktree"]["checkout"]).is_dir()
    assert not fake.threads
    assert (await bridge.create_worktree_thread(**repository, prompt="WITHHOLD"))["replayed"]


async def test_replay_survives_removed_checkout_and_source_paths(
    bridge, fake_server, repository, tmp_path
):
    from pathlib import Path

    first = await bridge.create_worktree_thread(**repository)
    Path(repository["destination"]).rename(tmp_path / "moved-checkout")
    Path(repository["source_repository"]).rename(tmp_path / "moved-source")
    before = list(fake_server[0].calls)
    repeated = await bridge.create_worktree_thread(**repository)
    assert repeated["replayed"] and repeated["threadId"] == first["threadId"]
    assert repeated["worktree"] == first["worktree"]
    assert fake_server[0].calls == before


async def test_concurrent_requests_cannot_adopt_same_destination(bridge, fake_server, repository):
    import asyncio

    receipts = await asyncio.gather(
        *[
            bridge.create_worktree_thread(**{**repository, "request_id": request_id})
            for request_id in ["first", "first", "second"]
        ]
    )
    assert receipts[0]["status"] == "accepted"
    assert receipts[1]["replayed"] and receipts[1]["threadId"] == receipts[0]["threadId"]
    assert receipts[2]["status"] == "failed"
    assert len(fake_server[0].threads) == 1


async def test_inherited_git_environment_cannot_redirect_checkout(
    bridge, repository, tmp_path, monkeypatch
):
    from pathlib import Path

    monkeypatch.setenv("GIT_DIR", str(tmp_path / "wrong-git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(tmp_path / "wrong-tree"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(tmp_path / "wrong-index"))
    receipt = await bridge.create_worktree_thread(**repository)
    assert receipt["status"] == "accepted", receipt
    assert (Path(receipt["worktree"]["checkout"]) / "tracked").read_text() == "base\n"
    assert not (tmp_path / "wrong-tree").exists()
    assert not (tmp_path / "wrong-index").exists()


async def test_destination_conditional_filters_are_disabled_before_checkout(
    bridge, repository, tmp_path
):
    from pathlib import Path

    source = Path(repository["source_repository"])
    (source / ".gitattributes").write_text("tracked filter=conditional\n")
    git(source, "add", ".gitattributes")
    git(source, "commit", "-m", "conditional attributes")
    marker = tmp_path / "unexpected-filter-effect"
    config = tmp_path / "conditional-config"
    config.write_text(f'[filter "conditional"]\n    smudge = "touch {marker}; cat"\n')
    git(source, "config", f"includeIf.gitdir:{source}/.git/worktrees/.path", str(config))
    args = {**repository, "starting_revision": git(source, "rev-parse", "HEAD")}
    receipt = await bridge.create_worktree_thread(**args)
    assert receipt["status"] == "accepted", receipt
    assert not marker.exists()
