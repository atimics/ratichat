"""Source snapshots and quote checks for worker reviews and node summaries."""

import hashlib
import json
import time


def source_text(value):
    return value if isinstance(value, str) else json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def source_hash(value):
    return hashlib.sha256(source_text(value).encode()).hexdigest()


def source_links(value):
    """Keep links supplied by the source record."""
    links = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"url", "source_url", "permalink"} and isinstance(item, str) and item.startswith(("https://", "http://")):
                links.add(item)
            elif isinstance(item, (dict, list)):
                links.update(source_links(item))
    elif isinstance(value, list):
        for item in value:
            links.update(source_links(item))
    return sorted(links)


def worker_sources(payload, checked_at):
    """Use the original request, expanded source data, and successful reads."""
    sources = {}

    def add(identity, data, **metadata):
        sources[identity] = {"text": source_text(data), "source_hash": source_hash(data),
                             "checked_at": checked_at, "links": source_links(data),
                             "trust": "untrusted_source", **metadata}

    add("request:" + payload["source_event_id"], payload["original_request"])
    for path, node in payload.get("nodes", {}).items():
        if node.get("kind") != "task" and "data" in node:
            add("node:" + path, node["data"], version=node.get("version"),
                fetched_at=node["data"].get("fetched_at") if isinstance(node["data"], dict) else None)
    for index, item in enumerate(payload.get("tool_results", [])):
        result = item.get("result")
        if isinstance(result, dict) and result.get("status") == "success":
            add(f"lookup:{index}", result, tool=item["tool"], fetched_at=item.get("fetched_at"),
                node_id=item.get("source_id"), version=item.get("version"))
    return sources


def check_worker_review(review, sources, checked_at):
    """Accept a bounded review whose supporting quotes match the source snapshots."""
    if not isinstance(review, dict) or not isinstance(review.get("claims"), list) or not 1 <= len(review["claims"]) <= 8:
        return None
    claims = []
    for claim in review["claims"]:
        if not isinstance(claim, dict) or not isinstance(claim.get("text"), str) or not 1 <= len(claim["text"].strip()) <= 800:
            return None
        refs = claim.get("evidence")
        if not isinstance(refs, list) or not 1 <= len(refs) <= 4:
            return None
        evidence = []
        for ref in refs:
            if not isinstance(ref, dict) or not isinstance(ref.get("source_id"), str):
                return None
            source, quote = sources.get(ref["source_id"]), ref.get("quote")
            if not source or not isinstance(quote, str) or not 8 <= len(quote.strip()) <= 1000 or quote not in source["text"]:
                return None
            evidence.append({"source_id": ref["source_id"], "quote": quote,
                             **{key: value for key, value in source.items() if key != "text"}})
        claims.append({"text": claim["text"].strip(), "evidence": evidence})
    return {"kind": "worker", "trust": "reviewed_worker", "review_status": "source_checked",
            "review_method": "model_review_with_exact_quotes", "checked_at": checked_at,
            "claims": claims, "content": "\n".join(claim["text"] for claim in claims)}


def summary_record(path, data, node_manager):
    """Refresh a compact summary when its source snapshot changes."""
    metadata = node_manager.get_node_metadata(path)
    fingerprint = source_hash(data)
    if metadata.summary_source_hash != fingerprint:
        current_summary = data.get("summary") if data.get("summary_version") == data.get("version") else None
        summary = current_summary or data.get("description") or source_text(data.get("data", data))[:700]
        node_manager.update_node_summary(path, summary)
        metadata.summary_source_hash = fingerprint
        metadata.summary_checked_at = time.time()
        metadata.summary_links = source_links(data)
    return {"summary": metadata.ai_summary, "source_id": path, "source_hash": fingerprint,
            "source_version": data.get("version"), "checked_at": metadata.summary_checked_at,
            "summary_checked_at": data.get("summary_checked_at", metadata.summary_checked_at),
            "fetched_at": data.get("fetched_at", data.get("data", {}).get("fetched_at") if isinstance(data.get("data"), dict) else None),
            "source_links": metadata.summary_links, "trust": "untrusted_source"}
