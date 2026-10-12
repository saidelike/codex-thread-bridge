"""Faithful persisted-record envelopes; presentation sanitization belongs to callers."""

import hashlib
import json

LIMITATIONS = [
    "Only persisted App Server protocol records are available; discarded history, active model "
    "context, and fields absent from the upstream representation cannot be recovered.",
    "Pages are independent reads, not a transactional history snapshot. A source version binds "
    "one response and its request, not an entire paginated conversation.",
]


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def source_version(source, text):
    digest = hashlib.sha256((canonical(source) + "\n" + text).encode("utf-8")).hexdigest()
    return "sha256:" + digest


def record_envelope(record, source):
    method, params = source["method"], source["params"]
    gaps = []
    paginated = method != "thread/read"
    if not paginated:
        thread = record.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != params["threadId"]:
            gaps.append("upstream metadata identity does not match the requested thread")
    if paginated:
        if not isinstance(record.get("data"), list):
            gaps.append("upstream page has no data array")
        if "nextCursor" not in record:
            gaps.append("upstream page has no nextCursor; pagination completion unknown")
        elif record["nextCursor"] is not None and not isinstance(record["nextCursor"], str):
            gaps.append("upstream nextCursor is not an opaque string; cannot continue")
        elif record["nextCursor"] is not None and record["nextCursor"] == params.get("cursor"):
            gaps.append("upstream nextCursor repeats the requested cursor; cannot advance")
        if record.get("nextCursor") is not None and not record.get("data"):
            gaps.append("empty upstream page with a continuation; history coverage unknown")
        if method == "thread/turns/list":
            for turn in record.get("data", []):
                if turn.get("itemsView", "full") != "full":
                    gaps.append(
                        f"turn {turn.get('id')} itemsView is not full; read items separately"
                    )
    return {
        "record": record,
        "source": source,
        "sourceVersion": source_version(source, canonical(record)),
        "coverage": {
            "scope": {
                "thread/read": "metadata",
                "thread/turns/list": "turn-page",
                "thread/items/list": "item-page",
            }[method],
            "transportComplete": True,
            "startsAtBeginning": "cursor" not in params,
            "hasMore": record["nextCursor"] is not None if "nextCursor" in record else None,
            "gaps": gaps,
            "limitations": LIMITATIONS,
        },
    }


def chunk_record(envelope, offset, max_chars, expected_version):
    if not 100 <= max_chars <= 100_000 or offset < 0:
        raise ValueError("max_chars must be 100–100000 and offset must be nonnegative")
    if offset and not expected_version:
        raise ValueError("expected_version is required for every continuation chunk")
    if expected_version and expected_version != envelope["sourceVersion"]:
        raise ValueError("source changed; discard prior chunks and restart at offset zero")
    text = canonical(envelope["record"])
    if offset >= len(text):
        raise ValueError("offset is outside this source record")
    end = min(offset + max_chars, len(text))
    return {
        "source": envelope["source"],
        "sourceVersion": envelope["sourceVersion"],
        "coverage": {**envelope["coverage"], "transportComplete": offset == 0 and end == len(text)},
        "encoding": "json",
        "text": text[offset:end],
        "totalLength": len(text),
        "start": offset,
        "end": end,
        "nextOffset": end if end < len(text) else None,
    }


def reconstruct_record(chunks):
    """Require ordered contiguous chunks and verify the full source-bound content digest."""
    if not chunks:
        raise ValueError("missing chunks")
    first = chunks[0]
    source, version, total = first["source"], first["sourceVersion"], first["totalLength"]
    if type(total) is not int or total < 1:
        raise ValueError("invalid totalLength")
    end, pieces = 0, []
    for chunk in chunks:
        if (
            chunk["source"] != source
            or chunk["sourceVersion"] != version
            or chunk["totalLength"] != total
        ):
            raise ValueError("source changed or chunks belong to different records")
        if (
            chunk.get("encoding") != "json"
            or type(chunk["start"]) is not int
            or type(chunk["end"]) is not int
            or chunk["start"] != end
        ):
            raise ValueError("missing, overlapping or unordered chunks")
        if (
            not isinstance(chunk["text"], str)
            or chunk["end"] != end + len(chunk["text"])
            or not end < chunk["end"] <= total
            or chunk["nextOffset"] != (chunk["end"] if chunk["end"] < total else None)
        ):
            raise ValueError("invalid chunk length, position or continuation")
        pieces.append(chunk["text"])
        end = chunk["end"]
    if end != total:
        raise ValueError("missing chunks; reconstructed length differs from totalLength")
    text = "".join(pieces)
    if source_version(source, text) != version:
        raise ValueError("source version digest mismatch; chunks changed")
    return record_envelope(json.loads(text), source)


def clipping_manifest(value, limit, display_fields, path="", display_text=False):
    if display_text and isinstance(value, str) and len(value) > limit:
        return [{"path": path, "originalLength": len(value), "retainedPrefixLength": limit}]
    if isinstance(value, list):
        return [
            field
            for index, item in enumerate(value)
            for field in clipping_manifest(
                item, limit, display_fields, f"{path}/{index}", display_text
            )
        ]
    if isinstance(value, dict):
        return [
            field
            for key, item in value.items()
            for field in clipping_manifest(
                item,
                limit,
                display_fields,
                path + "/" + key.replace("~", "~0").replace("/", "~1"),
                key in display_fields,
            )
        ]
    return []
