import pytest

from codex_thread_bridge.bridge import identity

POLICY = {
    "type": "workspaceWrite",
    "writableRoots": ["/tmp/cache"],
    "networkAccess": True,
    "excludeTmpdirEnvVar": False,
    "excludeSlashTmp": False,
}


async def setup(bridge, tmp_path):
    created = await bridge.create_thread("create", str(tmp_path))
    return created["threadId"], identity(created["creation"])


@pytest.mark.parametrize(
    "reviewer, changed_reviewer", [("user", "auto_review"), ("auto_review", "user")]
)
async def test_creation_replay_rejects_changed_reviewer_with_default_permissions(
    bridge, fake_server, tmp_path, reviewer, changed_reviewer
):
    original = await bridge.create_thread(
        "reviewer", str(tmp_path), prompt="hello", approvals_reviewer=reviewer
    )
    assert original["status"] == "accepted"
    replay = await bridge.create_thread(
        "reviewer", str(tmp_path), prompt="hello", approvals_reviewer=reviewer
    )
    assert replay == {**original, "replayed": True}
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread(
            "reviewer", str(tmp_path), prompt="hello", approvals_reviewer=changed_reviewer
        )
    fake, _ = fake_server
    assert fake.count("thread/start") == 1 and fake.count("turn/start") == 1


async def test_update_preserves_identity_and_messages_have_no_overrides(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    tid, expected = await setup(bridge, tmp_path)
    result = await bridge.update_thread_permissions("update", tid, POLICY, expected, "on-request")
    assert result["status"] == "accepted"
    assert result["permissionUpdateState"] == "verified"
    assert identity(result["permissionsAfter"]) == expected
    assert result["permissionsAfter"]["sandbox"] == POLICY
    update = next(p for m, p in fake.calls if m == "thread/settings/update")
    assert set(update) == {"threadId", "sandboxPolicy", "approvalPolicy", "approvalsReviewer"}
    before = len(fake.calls)
    sent = await bridge.send_message_to_thread("message", tid, "inspect")
    assert sent["status"] == "accepted"
    for method, params in fake.calls[before:]:
        if method == "thread/resume":
            assert params == {"threadId": tid, "excludeTurns": True}
        elif method == "turn/start":
            assert params == {
                "threadId": tid,
                "input": [],
                "toolOutput": {
                    "name": "send_message_to_thread",
                    "namespace": "codex_thread_bridge",
                    "output": "inspect",
                },
            }
    replay = await bridge.update_thread_permissions("update", tid, POLICY, expected, "on-request")
    assert replay["replayed"] and fake.count("thread/settings/update") == 1


@pytest.mark.parametrize("busy", [True, False])
async def test_busy_or_changed_identity_never_updates(bridge, fake_server, tmp_path, busy):
    fake, _ = fake_server
    tid, expected = await setup(bridge, tmp_path)
    if busy:
        fake.threads[tid]["status"] = {"type": "active"}
    else:
        expected["model"] = "different-model"
    result = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert result["status"] == "failed"
    assert fake.count("thread/settings/update") == 0
    assert fake.count("turn/start") == 0
    if busy:
        assert fake.count("thread/resume") == 0


async def test_lost_update_response_is_not_retried(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    tid, expected = await setup(bridge, tmp_path)
    fake.drop_after = "thread/settings/update"
    result = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert result["status"] == "outcome_unknown"
    fake.drop_after = None
    replay = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert replay["replayed"] and fake.count("thread/settings/update") == 1
    assert fake.settings[tid]["sandbox"] == POLICY


async def test_wrong_effective_policy_fails_verification(
    bridge, fake_server, tmp_path, monkeypatch
):
    tid, expected = await setup(bridge, tmp_path)
    original = bridge.rpc.call

    async def changed(method, params):
        result = await original(method, params)
        if method == "thread/settings/update":
            fake_server[0].settings[tid]["sandbox"] = {"type": "dangerFullAccess"}
        return result

    monkeypatch.setattr(bridge.rpc, "call", changed)
    result = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert result["status"] == "failed"
    assert result["permissionsAfter"]["sandbox"] == {"type": "dangerFullAccess"}
    assert fake_server[0].count("turn/start") == 0


async def test_creation_checks_full_policy_before_prompt(bridge, fake_server, tmp_path):
    result = await bridge.create_thread(
        "configured",
        str(tmp_path),
        prompt="hello",
        sandbox="workspace-write",
        sandbox_policy=POLICY,
        approval_policy="on-request",
    )
    assert result["status"] == "accepted"
    assert result["permissionsAfter"]["sandbox"] == POLICY
    methods = [m for m, _ in fake_server[0].calls]
    assert "thread/resume" not in methods
    assert "thread/settings/update" not in methods
    assert methods.index("thread/start") < methods.index("turn/start")


async def test_interactive_client_policy_is_rejected_before_creation(bridge, fake_server, tmp_path):
    with pytest.raises(ValueError, match="Interactive"):
        await bridge.create_thread(
            "bad", str(tmp_path), approval_policy="on-request", approvals_reviewer="user"
        )
    assert fake_server[0].count("thread/start") == 0


async def test_creation_mismatch_withholds_prompt(bridge, fake_server, tmp_path):
    fake_server[0].override_creation = {"sandbox": {**POLICY, "networkAccess": False}}
    result = await bridge.create_thread(
        "configured",
        str(tmp_path),
        prompt="hello",
        sandbox="workspace-write",
        sandbox_policy=POLICY,
    )
    assert result["status"] == "failed"
    assert fake_server[0].count("turn/start") == 0


async def test_file_workspace_roots_are_rejected_before_mutation(bridge, fake_server, tmp_path):
    target = tmp_path / "device"
    target.write_text("preserve")
    policy = {**POLICY, "writableRoots": [str(target)]}
    with pytest.raises(ValueError, match="directories"):
        await bridge.create_thread(
            "invalid", str(tmp_path), sandbox="workspace-write", sandbox_policy=policy
        )
    assert fake_server[0].count("thread/start") == 0
    assert target.read_text() == "preserve"


async def test_becomes_active_before_update_is_refused(bridge, fake_server, tmp_path, monkeypatch):
    fake, _ = fake_server
    tid, expected = await setup(bridge, tmp_path)
    original = bridge.rpc.call

    async def changed(method, params):
        result = await original(method, params)
        if method == "thread/resume":
            fake.threads[tid]["status"] = {"type": "active"}
        return result

    monkeypatch.setattr(bridge.rpc, "call", changed)
    result = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert result["status"] == "failed"
    assert fake.count("thread/settings/update") == 0


async def test_cancelled_update_retains_receipt_without_replay(bridge, fake_server, tmp_path):
    import asyncio

    fake, _ = fake_server
    tid, expected = await setup(bridge, tmp_path)
    fake.pause_after = "thread/settings/update"
    task = asyncio.create_task(bridge.update_thread_permissions("update", tid, POLICY, expected))
    await asyncio.wait_for(fake.paused.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    fake.release.set()
    replay = await bridge.update_thread_permissions("update", tid, POLICY, expected)
    assert replay["status"] == "outcome_unknown" and replay["replayed"]
    assert fake.count("thread/settings/update") == 1


@pytest.mark.parametrize("operation", ["create", "update"])
async def test_retained_permissions_replay_after_root_becomes_file(
    bridge, fake_server, tmp_path, operation
):
    fake, _ = fake_server
    cache = tmp_path / "cache"
    cache.mkdir()
    policy = {**POLICY, "writableRoots": [str(cache)]}
    if operation == "create":

        async def invoke(request_id):
            return await bridge.create_thread(
                request_id, str(tmp_path), sandbox="workspace-write", sandbox_policy=policy
            )
    else:
        tid, expected = await setup(bridge, tmp_path)

        async def invoke(request_id):
            return await bridge.update_thread_permissions(request_id, tid, policy, expected)

    original = await invoke("stable-permissions")
    assert original["status"] == "accepted"
    calls = list(fake.calls)
    cache.rmdir()
    cache.write_text("replacement must survive")
    replay = await invoke("stable-permissions")
    assert replay == {**original, "replayed": True}
    assert fake.calls == calls
    with pytest.raises(ValueError, match="directories"):
        await invoke("new-permissions")
    assert fake.calls == calls
    assert cache.read_text() == "replacement must survive"
