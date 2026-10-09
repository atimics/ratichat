from concurrent.futures import ThreadPoolExecutor

import pytest

from chatbot.core.node_system.awareness_store import AwarenessStore


ALLOWED = {"discord": {"general", "local", "other"}, "matrix": {"room", "private"}}
COMMUNITY = {"discord": {"general": "shared", "other": "other"}, "matrix": {"room": "shared"}}


@pytest.fixture
def store(tmp_path):
    result = AwarenessStore(tmp_path / "awareness.db", allowed_channels=ALLOWED, community_channels=COMMUNITY)
    yield result
    result.close()


def message(content="Python project", *, event="e1", sender="alice", **extra):
    return {"id": event, "sender": sender, "content": content, "timestamp": 100, **extra}


def task(store, **kwargs):
    return store.task_for_request("discord", "general", "alice", "request", goal="Research Python", **kwargs)


def test_shared_catalog_survives_restart_and_local_nodes_stay_local(tmp_path):
    path = tmp_path / "restart.db"
    first = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    first.ingest_message("discord", "general", message())
    first.ingest_message("discord", "local", message("Local discussion"))
    first.ingest_message("discord", "other", message("Another community"))
    actor = first.actor_id("discord", "general", "alice")
    first.close()
    second = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    catalog = second.catalog("matrix", "room", "bob")
    assert set(catalog) == {"channels.discord.general"}
    assert catalog["channels.discord.general"]["data"]["messages"][0]["content"] == "Python project"
    assert second.actor_id("discord", "general", "alice") == actor
    assert "channels.discord.local" in second.catalog("discord", "local", "alice")
    second.close()


def test_unapproved_channels_and_personal_data_are_scoped(store):
    with pytest.raises(PermissionError):
        store.ingest_message("discord", "unknown", message())
    store.put_node("people.preference", "preference", {"value": "private"}, "discord", "general", "alice", audience="personal")
    assert "people.preference" in store.catalog("discord", "general", "alice")
    assert "people.preference" not in store.catalog("discord", "general", "bob")
    assert "people.preference" not in store.catalog("matrix", "room", "alice")
    with pytest.raises(PermissionError):
        store.put_node("people.preference", "preference", {}, "discord", "general", "bob")


def test_event_keys_include_platform_and_channel_and_replay_is_idempotent(store):
    first = store.ingest_message("discord", "general", message())
    assert store.ingest_message("discord", "general", message())["changed"] is False
    store.ingest_message("matrix", "room", message("A different event"))
    store.ingest_message("discord", "local", message("Local event"))
    assert first["revision"] == 1
    assert store._db.execute("SELECT COUNT(*) FROM awareness_events").fetchone()[0] == 3


def test_observation_time_and_historical_flags_keep_the_same_source_event(store):
    store.ingest_message("matrix", "room", message(metadata={"historical": False}))
    assert not store.ingest_message("matrix", "room", message(timestamp=900, metadata={"historical": True}))["changed"]
    assert store._db.execute("SELECT COUNT(*) FROM awareness_events").fetchone()[0] == 1


def test_edit_invalidates_derived_nodes_and_stale_summaries(store):
    source = store.ingest_message("discord", "general", message())
    channel = store.catalog("discord", "general", "alice")[source["node_id"]]
    store.publish_summary(source["node_id"], channel["version"], "Project uses Python")
    topic = store.put_node("topics.python", "topic", {"uses": "Python"}, "discord", "general", "alice",
                           evidence=[{"kind": "node", "id": source["node_id"], "version": channel["version"]}])
    store.put_node("facts.project", "fact", {"language": "Python"}, "discord", "general", "alice",
                   evidence=[{"kind": "node", "id": "topics.python", "version": topic["version"]}])
    edited = store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Rust project", "source_revision": 2})
    assert edited["revision"] == 2
    catalog = store.catalog("matrix", "room", "bob")
    assert "topics.python" not in catalog and "facts.project" not in catalog
    assert catalog[source["node_id"]]["summary"] == ""
    assert store.publish_summary(source["node_id"], channel["version"], "Old summary")["conflict"]
    assert store.request_current("discord", "general", "e1", "Rust project")
    assert not store.request_current("discord", "general", "e1", "Python project")


def test_edit_keeps_author_reply_and_original_time(store):
    store.ingest_message("matrix", "room", message(reply_to="parent"))
    with pytest.raises(PermissionError):
        store.edit_message("matrix", "room", "e1", {"sender": "mallory", "content": "Wrong author"})
    with pytest.raises(ValueError):
        store.edit_message("matrix", "room", "missing", {"sender": "alice", "content": "Unknown"})
    store.edit_message("matrix", "room", "e1", {"sender": "alice", "content": "Updated", "reply_to": None, "timestamp": 999, "source_revision": 2})
    saved = store.catalog("matrix", "room", "alice")["channels.matrix.room"]["data"]["messages"][0]
    assert saved["reply_to"] == "parent" and saved["timestamp"] == 100


def test_older_edit_and_original_replay_keep_latest_content(store):
    original = message()
    store.ingest_message("discord", "general", original)
    store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Latest", "source_revision": 20})
    assert not store.ingest_message("discord", "general", original)["changed"]
    assert not store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Older", "source_revision": 10})["changed"]
    assert store.request_current("discord", "general", "e1", "Latest")


def test_equal_time_matrix_edits_use_event_id_tie_break(store):
    store.ingest_message("matrix", "room", message())
    store.edit_message("matrix", "room", "e1", {"sender": "alice", "content": "First", "source_revision": 2, "metadata": {"edit_event_id": "$b"}})
    assert not store.edit_message("matrix", "room", "e1", {"sender": "alice", "content": "Older tie", "source_revision": 2, "metadata": {"edit_event_id": "$a"}})["changed"]
    assert store.edit_message("matrix", "room", "e1", {"sender": "alice", "content": "Newer tie", "source_revision": 2, "metadata": {"edit_event_id": "$c"}})["changed"]


@pytest.mark.parametrize("known", [True, False])
def test_deletion_tombstone_survives_replay_and_restart(tmp_path, known):
    path = tmp_path / "delete.db"
    first = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    if known:
        first.ingest_message("matrix", "room", message())
        first.edit_message("matrix", "room", "e1", {"sender": "alice", "content": "Edit", "source_revision": 2})
    assert first.delete_message("matrix", "room", "e1")["deleted"]
    first.close()
    second = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    assert not second.ingest_message("matrix", "room", message())["changed"]
    assert not second.request_current("matrix", "room", "e1", "Python project")
    assert second.catalog("matrix", "room", "alice")["channels.matrix.room"]["data"]["messages"] == []
    second.close()


def test_personal_evidence_keeps_its_audience(store):
    personal = store.put_node("personal", "preference", {}, "discord", "general", "alice", audience="personal")
    with pytest.raises(PermissionError):
        store.put_node("public", "fact", {}, "discord", "general", "alice", evidence=[{"kind": "node", "id": "personal", "version": personal["version"]}])
    local = store.ingest_message("discord", "local", message())
    local_node = store.catalog("discord", "local", "alice")[local["node_id"]]
    with pytest.raises(PermissionError):
        store.put_node("public", "fact", {}, "discord", "general", "alice", evidence=[{"kind": "node", "id": local["node_id"], "version": local_node["version"]}])


def test_views_are_saved_independently_with_compare_and_swap(store):
    store.ingest_message("discord", "general", message())
    node = "channels.discord.general"
    assert store.load_view("discord-view", "discord", "general", "alice")["revision"] == 0
    saved = store.save_view("discord-view", "discord", "general", "alice", expanded=[node], pins=[node])
    assert saved["saved"] and saved["revision"] == 1
    assert store.save_view("discord-view", "discord", "general", "alice", collapsed=[node])["conflict"]
    store.save_view("matrix-view", "matrix", "room", "bob", collapsed=[node])
    assert store.load_view("discord-view", "discord", "general", "alice")["expanded"] == [node]
    assert store.load_view("matrix-view", "matrix", "room", "bob")["collapsed"] == [node]
    with pytest.raises(PermissionError):
        store.load_view("discord-view", "matrix", "room", "bob")
    with pytest.raises(PermissionError):
        store.save_view("bad-view", "matrix", "room", "bob", expanded=["missing"])


def test_verified_three_stage_link_shares_personal_memory_and_tasks(store):
    source_actor = store.actor_id("discord", "general", "alice")
    target_actor = store.actor_id("matrix", "room", "@alice:rati.chat")
    assert source_actor != target_actor
    store.put_node("personal", "preference", {"value": "Quiet"}, "discord", "general", "alice", audience="personal")
    private_task = store.task_for_request("discord", "local", "alice", "personal-request", audience="personal")
    link = store.begin_link("discord", "general", "alice", "matrix", "@alice:rati.chat")
    with pytest.raises(PermissionError):
        store.confirm_link(link["link_id"], "discord", "general", "alice")
    with pytest.raises(PermissionError):
        store.prove_link(link["link_id"], link["code"], "matrix", "room", "mallory")
    store.prove_link(link["link_id"], link["code"], "matrix", "room", "@alice:rati.chat")
    assert "personal" not in store.catalog("matrix", "room", "@alice:rati.chat")
    with pytest.raises(PermissionError):
        store.confirm_link(link["link_id"], "discord", "general", "mallory")
    store.confirm_link(link["link_id"], "discord", "general", "alice")
    assert store.actor_id("matrix", "room", "@alice:rati.chat") == source_actor
    assert "personal" in store.catalog("matrix", "room", "@alice:rati.chat")
    assert store.continue_task(private_task["id"], "matrix", "room", "@alice:rati.chat", "follow-up")["id"] == private_task["id"]


def test_expired_link_requires_a_fresh_proof(tmp_path):
    now = [100]
    store = AwarenessStore(tmp_path / "links.db", allowed_channels=ALLOWED, clock=lambda: now[0])
    link = store.begin_link("discord", "general", "alice", "matrix", "bob")
    now[0] += 601
    with pytest.raises(PermissionError):
        store.prove_link(link["link_id"], link["code"], "matrix", "room", "bob")
    assert store.cleanup()["links"] == 1
    store.close()


def test_shared_task_continuation_preserves_route_and_request_mapping(store):
    saved = task(store)
    route = store.save_route(saved["id"], {"model": "anthropic/claude-haiku-5.5", "api_path": "chat/completions"})
    repeated = task(store, topic="Changed prompt")
    assert repeated["id"] == saved["id"] and repeated["route"]["model"] == "anthropic/claude-haiku-5.5"
    followup = store.continue_task(saved["id"], "matrix", "room", "bob", "follow-up")
    assert followup["route_id"] == route["id"]
    assert store.get_task_for_event("matrix", "room", "follow-up")["id"] == saved["id"]
    assert store.list_tasks("matrix", "room", "bob")[0]["id"] == saved["id"]
    with pytest.raises(PermissionError):
        store.continue_task(saved["id"], "discord", "other", "bob", "bad")


def test_task_event_cannot_be_reassigned(store):
    first = task(store)
    second = store.task_for_request("discord", "general", "alice", "request2")
    with pytest.raises(ValueError):
        store.continue_task(second["id"], "discord", "general", "alice", "request")
    assert store.get_task_for_event("discord", "general", "request")["id"] == first["id"]


def test_routing_placeholder_transfers_its_cost_into_continued_task(store):
    original = task(store)
    store.save_route(original["id"], {"model": "fast"})
    placeholder = store.task_for_request("matrix", "room", "bob", "follow-up")
    attempt = store.reserve_attempt(placeholder["id"], 0.001, "route:follow-up")
    store.record_result(placeholder["id"], {"kind": "route", "receipt": {}}, attempt_id=attempt["id"], cost_usd=0.000042, status="active")
    continued = store.continue_task(original["id"], "matrix", "room", "bob", "follow-up", replace_task_id=placeholder["id"])
    assert continued["spent_usd"] == 0.000042 and continued["route"]["model"] == "fast"
    assert continued["results"][0]["result"]["kind"] == "route"
    assert store.get_task_for_event("matrix", "room", "follow-up")["id"] == original["id"]
    assert store.get_task(placeholder["id"])["status"] == "retired"
    repeated = store.continue_task(original["id"], "matrix", "room", "bob", "follow-up", replace_task_id=placeholder["id"])
    assert repeated["spent_usd"] == 0.000042


def test_placeholder_transfer_checks_target_budget_and_source_attempt_kind(store):
    original = task(store, budget_usd=0.00001)
    placeholder = store.task_for_request("matrix", "room", "bob", "follow-up")
    attempt = store.reserve_attempt(placeholder["id"], 0.001, "route:follow-up")
    store.record_result(placeholder["id"], {"kind": "route"}, attempt_id=attempt["id"], cost_usd=0.000042, status="active")
    with pytest.raises(ValueError):
        store.continue_task(original["id"], "matrix", "room", "bob", "follow-up", replace_task_id=placeholder["id"])
    assert store.get_task_for_event("matrix", "room", "follow-up")["id"] == placeholder["id"]
    store.record_result(placeholder["id"], {"kind": "inference"}, status="active")
    other = store.task_for_request("discord", "general", "alice", "larger")
    with pytest.raises(ValueError):
        store.continue_task(other["id"], "matrix", "room", "bob", "follow-up", replace_task_id=placeholder["id"])


def test_route_and_attempt_restore_after_restart(tmp_path):
    path = tmp_path / "task.db"
    first = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    saved = task(first)
    first.save_route(saved["id"], {"model": "openai/gpt-6-luna", "catalog_version": "today"})
    reserved = first.reserve_attempt(saved["id"], 0.03, "work")
    first.close()
    second = AwarenessStore(path, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    assert second.get_task(saved["id"])["reserved_usd"] == 0.03
    assert second.reserve_attempt(saved["id"], 0.03, "work")["id"] == reserved["id"]
    result = second.record_result(saved["id"], {"answer": "Useful result"}, attempt_id=reserved["id"], cost_usd=0.01)
    assert result["accepted"]
    completed = second.get_task(saved["id"])
    assert completed["spent_usd"] == 0.01 and completed["reserved_usd"] == 0
    assert completed["results"][0]["result"] == {"answer": "Useful result"}
    assert second.record_result(saved["id"], {}, attempt_id=reserved["id"], cost_usd=0.02)["duplicate"]
    assert second.get_task(saved["id"])["spent_usd"] == 0.01
    second.close()


def test_concurrent_workers_reserve_one_shared_budget(tmp_path):
    path = tmp_path / "budget.db"
    first = AwarenessStore(path, allowed_channels=ALLOWED)
    second = AwarenessStore(path, allowed_channels=ALLOWED)
    saved = task(first)
    def reserve(pair):
        worker, key = pair
        try:
            return worker.reserve_attempt(saved["id"], 0.03, key)
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=2) as workers:
        outcomes = list(workers.map(reserve, [(first, "one"), (second, "two")]))
    assert sum(item is not None for item in outcomes) == 1
    assert first.get_task(saved["id"])["reserved_usd"] == 0.03
    first.close()
    second.close()


def test_children_share_root_budget_and_have_bounded_depth_and_count(store):
    root = task(store)
    children = [store.create_child(root["id"], str(i)) for i in range(3)]
    assert all(item["root_task_id"] == root["id"] for item in children)
    assert store.create_child(root["id"], "0")["id"] == children[0]["id"]
    with pytest.raises(ValueError):
        store.create_child(root["id"], "four")
    with pytest.raises(ValueError):
        store.create_child(children[0]["id"], "grandchild")
    attempt = store.reserve_attempt(children[0]["id"], 0.03, "work")
    with pytest.raises(ValueError):
        store.reserve_attempt(children[1]["id"], 0.03, "work")
    store.record_result(children[0]["id"], {"result": "Done"}, attempt_id=attempt["id"], cost_usd=0.01)
    assert store.get_task(root["id"])["spent_usd"] == 0.01
    assert store.get_task(root["id"])["reserved_usd"] == 0
    assert store.reserve_attempt(children[1]["id"], 0.03, "work")["reserved"]


def test_stale_results_keep_measured_cost_and_require_fresh_inputs(store):
    event = store.ingest_message("discord", "general", message())
    node = store.catalog("discord", "general", "alice")[event["node_id"]]
    versions = {event["node_id"]: node["version"]}
    saved = task(store)
    store.save_route(saved["id"], {"model": "fast"}, input_versions=versions)
    attempt = store.reserve_attempt(saved["id"], 0.03, "work")
    store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Changed", "source_revision": 2})
    result = store.record_result(saved["id"], {"answer": "Old answer"}, attempt_id=attempt["id"], cost_usd=0.01)
    assert result["conflict"] and not result["accepted"]
    assert store.get_task(saved["id"])["status"] == "needs_refresh"
    assert store.get_task(saved["id"])["spent_usd"] == 0.01
    assert store.save_route(saved["id"], {"model": "fast"}, input_versions=versions)["conflict"]


def test_overspend_is_saved_and_future_reservations_use_measured_cost(store):
    saved = task(store)
    attempt = store.reserve_attempt(saved["id"], 0.01, "work")
    result = store.record_result(saved["id"], {}, attempt_id=attempt["id"], cost_usd=0.06)
    assert result["budget_exceeded"] and not result["accepted"]
    assert store.get_task(saved["id"])["spent_usd"] == 0.06
    with pytest.raises(ValueError):
        store.reserve_attempt(saved["id"], 0.01, "again")


def test_active_results_keep_route_and_allow_more_work(store):
    saved = task(store)
    store.save_route(saved["id"], {"model": "fast"})
    attempt = store.reserve_attempt(saved["id"], 0.01, "plan")
    store.record_result(saved["id"], {"plan": "Read sources"}, attempt_id=attempt["id"], cost_usd=0.001, status="active")
    assert store.get_task(saved["id"])["status"] == "active"
    assert store.get_task(saved["id"])["route"]["model"] == "fast"
    assert store.reserve_attempt(saved["id"], 0.01, "worker")["reserved"]


def test_free_delivery_results_work_after_the_budget_is_spent(store):
    saved = task(store)
    paid = store.reserve_attempt(saved["id"], 0.05, "worker")
    store.record_result(saved["id"], {"draft": "Answer"}, attempt_id=paid["id"], cost_usd=0.05, status="active")
    result = store.record_result(saved["id"], {"receipt": "sent", "answer": "Answer"})
    assert result["accepted"]
    assert store.get_task(saved["id"])["spent_usd"] == 0.05
    assert store.get_task(saved["id"])["reserved_usd"] == 0
    repeated = store.record_result(saved["id"], {"receipt": "sent", "answer": "Answer"})
    assert repeated["duplicate"] and repeated["accepted"]
    with pytest.raises(ValueError):
        store.record_result(saved["id"], {"answer": "Another"}, cost_usd=0.01)


def test_edit_invalidates_completed_task_and_accepted_result_history(store):
    source = store.ingest_message("discord", "general", message())
    version = store.catalog("discord", "general", "alice")[source["node_id"]]["version"]
    saved = task(store)
    store.save_route(saved["id"], {"model": "fast"}, input_versions={source["node_id"]: version})
    assert store.record_result(saved["id"], {"answer": "Original claim"})["accepted"]
    store.delete_message("discord", "general", "e1")
    updated = store.get_task(saved["id"])
    assert updated["status"] == "needs_refresh"
    assert updated["result"] is None and updated["results"] == []


def test_event_snapshots_keep_completed_tasks_after_new_messages(store):
    store.ingest_message("discord", "general", message())
    versions = store.snapshot_versions("discord", "general", "alice", store.catalog("discord", "general", "alice"))
    assert len(versions) == 1 and next(iter(versions)).startswith("event:")
    saved = task(store)
    store.save_route(saved["id"], {"model": "fast"}, input_versions=versions)
    assert store.record_result(saved["id"], {"answer": "Research complete"})["accepted"]
    store.ingest_message("discord", "general", message("Bot reply", event="bot", sender="ratichat", timestamp=200))
    store.ingest_message("discord", "general", message("Unrelated discussion", event="later", timestamp=300))
    assert store.get_task(saved["id"])["status"] == "complete"
    assert store.get_task(saved["id"])["result"] == {"answer": "Research complete"}
    assert "tasks." + saved["id"] in store.catalog("matrix", "room", "bob")
    store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Changed project", "source_revision": 2})
    assert store.get_task(saved["id"])["status"] == "needs_refresh"
    assert store.get_task(saved["id"])["results"] == []


def test_event_snapshot_rejects_stale_or_private_inputs(store):
    store.ingest_message("discord", "local", message())
    local = store.catalog("discord", "local", "alice")
    with pytest.raises(ValueError):
        store.snapshot_versions("matrix", "room", "bob", local)
    versions = store.snapshot_versions("discord", "local", "alice", local)
    shared = task(store)
    with pytest.raises(PermissionError):
        store.save_route(shared["id"], {"model": "fast"}, input_versions=versions)
    personal = store.task_for_request("discord", "local", "alice", "personal", audience="personal")
    assert store.save_route(personal["id"], {"model": "fast"}, input_versions=versions)["saved"]
    store.delete_message("discord", "local", "e1")
    assert store.save_route(personal["id"], {"model": "fast"}, input_versions=versions)["conflict"]


@pytest.mark.parametrize("amount", [-1, float("nan"), float("inf"), 2])
def test_task_budget_values_are_bounded(store, amount):
    with pytest.raises(ValueError):
        task(store, budget_usd=amount)


def test_shared_sources_have_stable_ids_and_bounded_catalog(store):
    source = store.source_result("discord", "general", "alice", "read_news", {"source": "bbc"}, {"items": ["One"]})
    second = store.source_result("matrix", "room", "bob", "read_news", {"source": "bbc"}, {"items": ["Two"]})
    assert source["node_id"] == second["node_id"]
    assert second["version"] > source["version"]
    assert store.catalog("matrix", "room", "bob", max_chars=1) == {}
    assert len(store.catalog("matrix", "room", "bob", limit=1)) == 1


def test_large_conversation_keeps_a_bounded_shared_projection(store):
    for i in range(20):
        store.ingest_message("discord", "general", message("x" * 12000, event=str(i), timestamp=i))
    node = store.catalog("matrix", "room", "bob")["channels.discord.general"]
    assert len(node["data"]["messages"]) == 10
    assert all(len(item["content"]) == 1000 for item in node["data"]["messages"])
    assert store.request_current("discord", "general", "19", "x" * 12000)


def test_cleanup_keeps_current_knowledge_and_deletion_receipts(tmp_path):
    now = [100]
    store = AwarenessStore(tmp_path / "cleanup.db", allowed_channels=ALLOWED, clock=lambda: now[0])
    store.ingest_message("discord", "general", message())
    store.edit_message("discord", "general", "e1", {"sender": "alice", "content": "Edit", "source_revision": 2})
    store.delete_message("discord", "general", "e1")
    store.put_node("facts.known", "fact", {"value": "Lasting fact"}, "discord", "general", "alice")
    now[0] += 8 * 86400
    assert store.cleanup()["events"] == 2
    assert "facts.known" in store.catalog("discord", "general", "alice")
    assert not store.ingest_message("discord", "general", message())["changed"]
    store.close()
