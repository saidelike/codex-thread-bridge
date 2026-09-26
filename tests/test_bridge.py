import asyncio

import pytest

from codex_thread_bridge.ledger import Ledger


async def test_create_and_followup_preserve_defaults_and_exact_messages(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    first = await bridge.create_thread(
        "create", str(tmp_path), prompt="  exact\nmessage  ", title="Demo"
    )
    assert first["status"] == "accepted"
    assert first["creation"]["model"] == "configured-default"
    creation_params = next(p for name, p in fake.calls if name == "thread/start")
    assert "model" not in creation_params and "config" not in creation_params
    assert "projectId" not in creation_params
    assert not any(name.startswith("thread/goal/") for name, _ in fake.calls)
    assert fake.threads[first["threadId"]]["turns"][0]["items"][0]["text"] == "  exact\nmessage  "
    initial = next(params for name, params in fake.calls if name == "turn/start")
    assert initial["input"] == []
    assert initial["toolOutput"] == {
        "name": "create_thread",
        "namespace": "codex_thread_bridge",
        "output": "  exact\nmessage  ",
    }
    followup = await bridge.send_message_to_thread("send", first["threadId"], "followup")
    assert followup["status"] == "accepted" and followup["turnId"] == "turn-2"
    sent = [params for name, params in fake.calls if name == "turn/start"][-1]
    assert sent["input"] == []
    assert sent["toolOutput"] == {
        "name": "send_message_to_thread",
        "namespace": "codex_thread_bridge",
        "output": "followup",
    }
    assert next(p for name, p in fake.calls if name == "thread/resume") == {
        "threadId": first["threadId"],
        "excludeTurns": True,
    }
    result = await bridge.wait_thread(first["threadId"], followup["turnId"], 0)
    assert result["turn"]["items"][0]["text"] == "followup"


async def test_empty_creation_does_not_dispatch_or_set_goal(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    result = await bridge.create_thread("empty", str(tmp_path))
    assert "turnId" not in result
    assert fake.count("turn/start") == 0
    assert fake.count("thread/goal/set") == 0


async def test_duplicate_create_and_message_do_not_dispatch_twice(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    results = await asyncio.gather(
        *[bridge.create_thread("same", str(tmp_path), prompt="hello") for _ in range(3)]
    )
    assert len({r["threadId"] for r in results}) == 1
    assert fake.count("thread/start") == 1 and fake.count("turn/start") == 1
    tid = results[0]["threadId"]
    for _ in range(3):
        await bridge.send_message_to_thread("same-send", tid, "hello again")
    assert fake.count("turn/start") == 2


async def test_conflicting_request_id_fails_without_mutation(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    await bridge.create_thread("same", str(tmp_path), prompt="first")
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("same", str(tmp_path), prompt="second")
    assert fake.count("thread/start") == 1


async def test_replay_after_cwd_removal_returns_retained_receipt(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    cwd = tmp_path / "checkout"
    cwd.mkdir()
    first = await bridge.create_thread("create", str(cwd))
    cwd.rmdir()
    calls = len(fake.calls)
    repeated = await bridge.create_thread("create", str(cwd))
    assert repeated["replayed"] and repeated["threadId"] == first["threadId"]
    assert len(fake.calls) == calls
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("create", str(cwd), prompt="different")


async def test_cwd_symlink_retargeting_does_not_change_request_identity(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    original, other = tmp_path / "original", tmp_path / "other"
    original.mkdir()
    other.mkdir()
    alias = tmp_path / "checkout"
    alias.symlink_to(original, target_is_directory=True)
    first = await bridge.create_thread("create", str(alias))
    assert first["creation"]["cwd"] == str(original)
    alias.unlink()
    alias.symlink_to(other, target_is_directory=True)
    repeated = await bridge.create_thread("create", str(alias))
    assert repeated["replayed"] and repeated["threadId"] == first["threadId"]
    alias.unlink()
    assert (await bridge.create_thread("create", str(alias)))["replayed"]
    assert fake.count("thread/start") == 1


@pytest.mark.parametrize("cwd_state", ["missing", "symlink", "symlink_loop"])
async def test_old_canonical_cwd_fingerprint_replays_after_path_changes(
    bridge, fake_server, tmp_path, cwd_state
):
    fake, _ = fake_server
    checkout = tmp_path / "removed-before-upgrade"
    cwd = str(checkout)
    # Version 0.1.0 used this resolved-cwd payload, with no schema version field.
    _, receipt = bridge.ledger.begin(
        "old-create",
        "create_thread",
        {
            "cwd": cwd,
            "sandbox": "read-only",
            "approvalPolicy": "never",
            "ephemeral": False,
            "prompt": None,
            "title": None,
        },
    )
    receipt.pop("fingerprintVersion")
    bridge.ledger.save({**receipt, "status": "accepted", "threadId": "retained-thread"})
    if cwd_state == "symlink":
        replacement = tmp_path / "other-checkout"
        replacement.mkdir()
        checkout.symlink_to(replacement, target_is_directory=True)
    elif cwd_state == "symlink_loop":
        checkout.symlink_to(checkout, target_is_directory=True)
    repeated = await bridge.create_thread("old-create", cwd)
    assert repeated["replayed"] and repeated["threadId"] == "retained-thread"
    assert not fake.calls


async def test_legacy_cwd_symlink_replay_uses_old_fingerprint_only_for_legacy_receipts(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    _, receipt = bridge.ledger.begin(
        "legacy",
        "create_thread",
        {
            "cwd": str(target),
            "sandbox": "read-only",
            "approvalPolicy": "never",
            "ephemeral": False,
            "prompt": None,
            "title": None,
        },
    )
    receipt.pop("fingerprintVersion")  # Exact receipt format from the old implementation.
    bridge.ledger.save({**receipt, "threadId": "legacy-thread", "status": "accepted"})
    recovered = await bridge.create_thread("legacy", str(alias))
    assert recovered["replayed"] and recovered["threadId"] == "legacy-thread"
    assert not fake.calls
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("legacy", str(alias), prompt="changed")
    await bridge.create_thread("new", str(target))
    # A new-format request cannot use the legacy escape hatch with different raw arguments.
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("new", str(alias))
    assert fake.count("thread/start") == 1


@pytest.mark.parametrize("reviewer", ["user", "auto_review"])
@pytest.mark.parametrize("status", ["accepted", "outcome_unknown"])
async def test_previous_creation_receipts_replay_after_upgrade_with_missing_cwd(
    bridge, fake_server, tmp_path, reviewer, status
):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    _, receipt = bridge.ledger.begin(
        "previous-create",
        "create_thread",
        {
            "cwd": str(alias),
            "sandbox": "read-only",
            "approvalPolicy": "never",
            "ephemeral": False,
            "prompt": None,
            "title": None,
        },
    )
    original = bridge.ledger.save({**receipt, "fingerprintVersion": 2, "status": status})
    # Version 2 hashed the supplied path and could omit the requested reviewer.
    alias.unlink()
    replay = await bridge.create_thread("previous-create", str(alias), approvals_reviewer=reviewer)
    assert replay == {**original, "replayed": True}
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("previous-create", str(target), approvals_reviewer=reviewer)
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.create_thread("previous-create", str(alias), prompt="changed")
    assert not fake_server[0].calls


async def test_lost_creation_response_is_never_retried(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.drop_after = "thread/start"
    result = await bridge.create_thread("lost", str(tmp_path), prompt="hello")
    assert result["status"] == "outcome_unknown"
    assert "threadId" not in result
    fake.drop_after = None
    repeat = await bridge.create_thread("lost", str(tmp_path), prompt="hello")
    assert repeat["replayed"] and repeat["status"] == "outcome_unknown"
    assert fake.count("thread/start") == 1 and fake.count("turn/start") == 0


async def test_partial_failure_retains_created_id(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.reject["thread/name/set"] = {"code": -32602, "message": "name rejected"}
    result = await bridge.create_thread("partial", str(tmp_path), prompt="hello", title="Demo")
    assert result["status"] == "failed" and result["threadId"] == "thread-1"
    assert fake.count("turn/start") == 0
    assert bridge.ledger.get("partial")["threadId"] == "thread-1"


async def test_lost_initial_turn_response_retains_id_without_resend(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.drop_after = "turn/start"
    first = await bridge.create_thread("lost-turn", str(tmp_path), prompt="hello")
    assert first["status"] == "outcome_unknown" and first["threadId"] == "thread-1"
    await bridge.create_thread("lost-turn", str(tmp_path), prompt="hello")
    assert fake.count("turn/start") == 1


async def test_environment_mismatch_withholds_prompt(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.override_creation = {"sandbox": {"type": "dangerFullAccess"}}
    result = await bridge.create_thread("mismatch", str(tmp_path), prompt="hello")
    assert result["status"] == "failed" and result["threadId"] == "thread-1"
    assert fake.count("turn/start") == 0


async def test_desktop_project_id_not_found_stops_before_creation(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.reject["project/read"] = {"code": -32602, "message": "project not found"}
    result = await bridge.create_thread(
        "project", str(tmp_path), app_server_project_id="desktop-id"
    )
    assert result["status"] == "failed" and fake.count("thread/start") == 0


async def test_busy_thread_is_not_resumed_or_messaged(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path))
    fake.threads[created["threadId"]]["status"] = {"type": "active"}
    result = await bridge.send_message_to_thread("busy", created["threadId"], "hello")
    assert result["status"] == "failed"
    assert fake.count("thread/resume") == 0 and fake.count("turn/start") == 0


async def test_steer_exact_active_turn_once_without_resuming(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path), prompt="initial")
    tid = created["threadId"]
    turn_id = created["turnId"]
    fake.threads[tid]["status"] = {"type": "active"}
    fake.threads[tid]["turns"][-1]["status"] = "inProgress"

    sent = await bridge.steer_thread("steer", tid, turn_id, "change direction")
    replayed = await bridge.steer_thread("steer", tid, turn_id, "change direction")
    assert sent["status"] == "accepted" and sent["turnId"] == turn_id
    assert replayed["replayed"] and fake.count("turn/steer") == 1
    assert fake.count("thread/resume") == 0 and fake.count("turn/start") == 1
    assert fake.threads[tid]["turns"][-1]["items"][-1]["text"] == "change direction"


async def test_steer_rejects_stale_or_idle_turn_without_delivery(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path), prompt="initial")
    tid = created["threadId"]
    fake.threads[tid]["status"] = {"type": "active"}
    fake.threads[tid]["turns"][-1]["status"] = "inProgress"

    stale = await bridge.steer_thread("stale", tid, "wrong-turn", "do not deliver")
    assert stale["status"] == "failed"
    assert fake.threads[tid]["turns"][-1]["items"] == [{"type": "agentMessage", "text": "initial"}]
    fake.threads[tid]["status"] = {"type": "idle"}
    idle = await bridge.steer_thread("idle", tid, created["turnId"], "also do not deliver")
    assert idle["status"] == "failed" and fake.count("turn/steer") == 2


@pytest.mark.parametrize("interruption", ["response_loss", "cancellation"])
async def test_interrupted_steering_retains_unknown_receipt_without_redelivery(
    bridge, fake_server, tmp_path, interruption
):
    fake, _ = fake_server
    fake.complete_turns = False
    created = await bridge.create_thread("create", str(tmp_path), prompt="initial")
    tid, turn_id = created["threadId"], created["turnId"]
    fake.threads[tid]["status"] = {"type": "active"}
    if interruption == "response_loss":
        fake.drop_after = "turn/steer"
        result = await bridge.steer_thread("steer", tid, turn_id, "change direction")
        assert result["status"] == "outcome_unknown"
        fake.drop_after = None
    else:
        fake.pause_after = "turn/steer"
        task = asyncio.create_task(bridge.steer_thread("steer", tid, turn_id, "change direction"))
        try:
            await asyncio.wait_for(fake.paused.wait(), 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            fake.release.set()

    replay = await bridge.steer_thread("steer", tid, turn_id, "change direction")
    assert replay["replayed"] and replay["status"] == "outcome_unknown"
    assert replay["threadId"] == tid and replay["expectedTurnId"] == turn_id
    assert replay["retrySafe"] is False
    with pytest.raises(ValueError, match="different arguments"):
        await bridge.steer_thread("steer", tid, turn_id, "different instruction")
    history = await bridge.read_thread(tid)
    assert history["turnsPage"]["data"][0]["items"] == [
        {"type": "agentMessage", "text": "initial"},
        {"type": "userMessage", "text": "change direction"},
    ]
    assert fake.count("turn/steer") == 1


async def test_interactive_approval_policy_withholds_message(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path))
    fake.approval_policy = "on-request"
    result = await bridge.send_message_to_thread("send", created["threadId"], "hello")
    assert result["status"] == "failed" and "resumed" in result
    assert fake.count("turn/start") == 0


async def test_reads_and_waits_do_not_resume_or_use_other_completed_turn(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path), prompt="a" * 300)
    count = len(fake.calls)
    read = await bridge.read_thread(created["threadId"], max_text_chars=100)
    assert "truncated" in read["turnsPage"]["data"][0]["items"][0]["text"]
    result = await bridge.wait_thread(created["threadId"], "not-this-turn", 0.02)
    assert result["timedOut"] and result["turn"] is None
    await bridge.list_threads()
    assert all(
        name in {"initialize", "initialized", "thread/read", "thread/turns/list", "thread/list"}
        for name, _ in fake.calls[count:]
    )


async def test_history_pagination(bridge, tmp_path):
    created = await bridge.create_thread("create", str(tmp_path), prompt="first")
    await bridge.send_message_to_thread("send", created["threadId"], "second")
    page1 = await bridge.read_thread(created["threadId"], limit=1)
    page2 = await bridge.read_thread(
        created["threadId"], limit=1, cursor=page1["turnsPage"]["nextCursor"]
    )
    assert page1["turnsPage"]["data"][0]["id"] == "turn-2"
    assert page2["turnsPage"]["data"][0]["id"] == "turn-1"


async def test_low_text_limit_preserves_page_cursors_and_protocol_fields(
    bridge, fake_server, tmp_path
):
    fake, _ = fake_server
    created = await bridge.create_thread("create", str(tmp_path), prompt="first" * 100)
    await bridge.send_message_to_thread("send", created["threadId"], "second" * 100)
    long_path = "/" + "directory/" * 30
    fake.threads[created["threadId"]]["cwd"] = long_path
    newest = fake.threads[created["threadId"]]["turns"][-1]
    newest["items"][0]["id"] = "item-" + "x" * 120
    page1 = await bridge.read_thread(created["threadId"], limit=1, max_text_chars=100)
    assert page1["thread"]["cwd"] == long_path
    assert page1["turnsPage"]["nextCursor"] == fake.cursor(1)
    assert page1["turnsPage"]["backwardsCursor"] == fake.cursor(0)
    item = page1["turnsPage"]["data"][0]["items"][0]
    assert item["id"] == newest["items"][0]["id"]
    assert item["text"].startswith("second" * 16) and "truncated" in item["text"]
    page2 = await bridge.read_thread(
        created["threadId"], limit=1, cursor=page1["turnsPage"]["nextCursor"], max_text_chars=100
    )
    assert page2["turnsPage"]["data"][0]["id"] == "turn-1"


async def test_list_preserves_long_cursor(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    fake.cursor_padding = 5000
    await bridge.create_thread("first", str(tmp_path))
    await bridge.create_thread("second", str(tmp_path))
    first = await bridge.list_threads(limit=1)
    assert first["nextCursor"] == fake.cursor(1)
    second = await bridge.list_threads(limit=1, cursor=first["nextCursor"])
    assert first["data"][0]["id"] != second["data"][0]["id"]


async def test_cancellation_keeps_unknown_receipt_and_prevents_retry(bridge, fake_server, tmp_path):
    fake, _ = fake_server
    started = asyncio.Event()
    original = bridge.rpc.call

    async def slow(method, params):
        if method == "thread/start":
            started.set()
            await asyncio.Event().wait()
        return await original(method, params)

    bridge.rpc.call = slow
    task = asyncio.create_task(bridge.create_thread("cancel", str(tmp_path)))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bridge.ledger.get("cancel")["status"] == "outcome_unknown"
    repeat = await bridge.create_thread("cancel", str(tmp_path))
    assert repeat["replayed"] and fake.count("thread/start") == 0


def test_ledger_survives_restarts_and_is_private(tmp_path):
    path = tmp_path / "private" / "state.sqlite3"
    first = Ledger(path)
    fresh, receipt = first.begin("key", "create", {"cwd": "/example"})
    assert fresh
    first.save({**receipt, "threadId": "known-id"})
    first.close()
    second = Ledger(path)
    try:
        fresh, receipt = second.begin("key", "create", {"cwd": "/example"})
        assert not fresh and receipt["threadId"] == "known-id"
        assert receipt["status"] == "in_progress_or_unknown"
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        second.close()


async def test_goal_read_validates_id_and_bounds_text_without_mutation(bridge, fake_server):
    fake, _ = fake_server
    with pytest.raises(ValueError, match="thread_id"):
        await bridge.get_goal(" ")
    assert not fake.calls
    objective = "exact objective\nwith whitespace  "
    fake.goal = {"objective": objective, "status": "active"}
    assert (await bridge.get_goal("thread-1"))["goal"]["objective"] == objective
    fake.goal = {"objective": "x" * 5000, "status": "active"}
    result = await bridge.get_goal("thread-1")
    assert result["goal"]["objective"] == (
        "x" * 4000 + "\n[truncated; original length 5000 characters]"
    )
    assert result["goal"]["status"] == "active"
    assert [name for name, _ in fake.calls if name not in {"initialize", "initialized"}] == [
        "thread/goal/get",
        "thread/goal/get",
    ]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cwd": "relative"},
        {"cwd": "/does-not-exist-ctb"},
        {"prompt": " "},
        {"sandbox": "made-up"},
        {"request_id": ""},
    ],
)
async def test_invalid_create_has_no_api_effects(bridge, fake_server, tmp_path, kwargs):
    fake, _ = fake_server
    params = {"request_id": "valid", "cwd": str(tmp_path), **kwargs}
    with pytest.raises(ValueError):
        await bridge.create_thread(**params)
    assert not fake.calls
