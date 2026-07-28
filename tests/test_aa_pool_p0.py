from __future__ import annotations

import json
import os
import secrets
import sys
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from flask import Flask

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from api.aa_keys import aa_keys_bp
from api.parse import parse_bp
from services import aa_key_pool, book_storage
from services.redis_store import (
    get_control_redis,
    get_redis,
    get_parse_meta,
    store_parse_error,
    store_parse_pending,
    store_parse_result,
    validate_control_redis,
)
from services.task_manager import (
    _COUNTER_KEYS,
    _decr_counter_once,
    _release_task_lock,
    _reserve_queue_slot,
    is_overloaded,
)


def _runtime_secret() -> str:
    return secrets.token_urlsafe(48)


@pytest.fixture
def redis_client():
    r = get_redis()
    r.ping()
    r.flushdb()
    yield r
    r.flushdb()


@pytest.fixture
def control_redis_client():
    r = get_control_redis()
    r.ping()
    r.flushdb()
    yield r
    r.flushdb()


@pytest.fixture
def account_pool(monkeypatch, redis_client, control_redis_client):
    first = _runtime_secret()
    second = _runtime_secret()
    monkeypatch.setattr(config, "AA_SECRET_KEY", "")
    monkeypatch.setattr(config, "AA_SECRET_KEYS", f"{first},{second}")
    aa_key_pool.seed_keys_from_env()
    yield first, second
    monkeypatch.setattr(config, "AA_SECRET_KEYS", "")
    aa_key_pool.seed_keys_from_env()


def test_rotation_is_atomic_and_redis_contains_no_raw_keys(
    account_pool, redis_client, control_redis_client
):
    first, second = account_pool

    one = aa_key_pool.available_keys()
    two = aa_key_pool.available_keys()

    assert len(one) == 2
    assert len(two) == 2
    assert one[0][0] != two[0][0]

    serialized_state = []
    for client in (redis_client, control_redis_client):
        for key in client.scan_iter("aa:*"):
            serialized_state.append(key)
            key_type = client.type(key)
            if key_type == "string":
                serialized_state.append(client.get(key) or "")
            elif key_type == "set":
                serialized_state.extend(client.smembers(key))
            elif key_type == "hash":
                serialized_state.extend(client.hgetall(key).values())
    combined = "\n".join(serialized_state)
    assert first not in combined
    assert second not in combined


def test_quota_and_disabled_accounts_aggregate_to_10002(
    monkeypatch, tmp_path, account_pool
):
    quota_secret, disabled_secret = account_pool

    def fake_download(_dir, _md5, _ext, secret_key, _label):
        if secret_key == quota_secret:
            raise book_storage.AADownloadQuotaExceededError("quota")
        if secret_key == disabled_secret:
            raise book_storage.AAVipExpiredError("membership")
        raise AssertionError("unexpected account")

    monkeypatch.setattr(book_storage, "_download_from_aa", fake_download)

    with pytest.raises(book_storage.AADownloadQuotaExceededError) as caught:
        book_storage._download_from_aa_pool(
            str(tmp_path),
            "0" * 32,
            "pdf",
            aa_key_pool.available_keys(),
        )

    assert caught.value.next_probe_at > int(time.time())
    assert {item["status"] for item in aa_key_pool.list_keys()} == {
        "cooldown",
        "disabled",
    }


def test_all_disabled_accounts_aggregate_to_10001(
    monkeypatch, tmp_path, account_pool
):
    def fake_download(*_args):
        raise book_storage.AAVipExpiredError("membership")

    monkeypatch.setattr(book_storage, "_download_from_aa", fake_download)

    with pytest.raises(book_storage.AAVipExpiredError) as caught:
        book_storage._download_from_aa_pool(
            str(tmp_path),
            "1" * 32,
            "epub",
            aa_key_pool.available_keys(),
        )

    assert caught.value.next_probe_at > int(time.time())
    assert all(
        item["status"] == "disabled"
        for item in aa_key_pool.list_keys()
    )


def test_transient_error_is_not_misclassified_as_pool_business_code(
    monkeypatch, tmp_path, account_pool
):
    quota_secret, transient_secret = account_pool

    def fake_download(_dir, _md5, _ext, secret_key, _label):
        if secret_key == quota_secret:
            raise book_storage.AADownloadQuotaExceededError("quota")
        if secret_key == transient_secret:
            return None, "temporary upstream failure"
        raise AssertionError("unexpected account")

    monkeypatch.setattr(book_storage, "_download_from_aa", fake_download)
    path, error = book_storage._download_from_aa_pool(
        str(tmp_path),
        "2" * 32,
        "pdf",
        aa_key_pool.available_keys(),
    )

    assert path is None
    assert "temporary upstream failure" in error


def test_cooldown_probe_has_single_lease_and_success_reactivates(
    account_pool, control_redis_client
):
    first, _ = account_pool
    kid = aa_key_pool.key_id(first)
    aa_key_pool.mark_quota_exhausted(first, "quota")
    control_redis_client.hset(
        aa_key_pool._state_key(kid),
        "next_probe_at",
        int(time.time()) - 1,
    )

    selected = dict(aa_key_pool.available_keys())
    selected_again = dict(aa_key_pool.available_keys())

    assert kid in selected
    assert kid not in selected_again

    aa_key_pool.mark_success(first)
    assert kid in dict(aa_key_pool.available_keys())
    state = next(
        item for item in aa_key_pool.list_keys() if item["id"] == kid
    )
    assert state["status"] == "active"
    assert state["last_success_at"] > 0


def test_anna_download_hard_concurrency_is_two(
    monkeypatch, control_redis_client
):
    monkeypatch.setattr(config, "AA_DOWNLOAD_SLOT_WAIT_SECONDS", 2)
    control_redis_client.delete(aa_key_pool.DOWNLOAD_LEASES)
    start = threading.Barrier(6)
    errors = []
    active = 0
    maximum = 0
    lock = threading.Lock()

    def worker():
        nonlocal active, maximum
        try:
            start.wait(timeout=2)
            with aa_key_pool.anna_download_slot():
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.08)
                with lock:
                    active -= 1
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert not errors
    assert maximum == 2
    assert control_redis_client.zcard(aa_key_pool.DOWNLOAD_LEASES) == 0


def test_anna_download_control_plane_failure_is_fail_closed(monkeypatch):
    class UnavailableControlRedis:
        def eval(self, *_args, **_kwargs):
            raise ConnectionError("control redis unavailable")

    monkeypatch.setattr(
        aa_key_pool,
        "get_control_redis",
        lambda: UnavailableControlRedis(),
    )
    with pytest.raises(aa_key_pool.AnnaDownloadBusyError) as caught:
        with aa_key_pool.anna_download_slot():
            raise AssertionError("download must not start")

    assert "control plane" in str(caught.value)


def test_same_md5_content_lock_is_serialized(
    monkeypatch, control_redis_client
):
    monkeypatch.setattr(config, "AA_DOWNLOAD_SLOT_WAIT_SECONDS", 2)
    md5 = "a" * 32
    control_redis_client.delete(
        f"{aa_key_pool.CONTENT_LEASE_PREFIX}{md5}"
    )
    start = threading.Barrier(4)
    errors = []
    active = 0
    maximum = 0
    lock = threading.Lock()

    def worker():
        nonlocal active, maximum
        try:
            start.wait(timeout=2)
            with aa_key_pool.anna_content_lock(md5):
                with lock:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(0.04)
                with lock:
                    active -= 1
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert not errors
    assert maximum == 1
    assert not control_redis_client.exists(
        f"{aa_key_pool.CONTENT_LEASE_PREFIX}{md5}"
    )


def test_control_redis_requires_noeviction(
    monkeypatch, control_redis_client
):
    assert validate_control_redis()["maxmemory_policy"] == "noeviction"

    class EvictableControlRedis:
        def ping(self):
            return True

        def config_get(self, _name):
            return {"maxmemory-policy": "allkeys-lfu"}

    monkeypatch.setattr(
        "services.redis_store.get_control_redis",
        lambda: EvictableControlRedis(),
    )
    with pytest.raises(RuntimeError, match="noeviction"):
        validate_control_redis()


def test_no_configured_accounts_is_10001(
    monkeypatch, redis_client, control_redis_client
):
    monkeypatch.setattr(config, "AA_SECRET_KEY", "")
    monkeypatch.setattr(config, "AA_SECRET_KEYS", "")
    aa_key_pool.seed_keys_from_env()

    with pytest.raises(book_storage.AAVipExpiredError) as caught:
        book_storage.find_book_file("b" * 32, "pdf")

    assert "未配置" in str(caught.value)


def test_account_selection_happens_after_download_slot(monkeypatch):
    events = []

    @contextmanager
    def content_lock(_md5):
        events.append("content-enter")
        yield
        events.append("content-exit")

    @contextmanager
    def download_slot():
        events.append("slot-enter")
        yield
        events.append("slot-exit")

    monkeypatch.setattr(book_storage, "_find_local", lambda *_args: None)
    monkeypatch.setattr(book_storage, "anna_content_lock", content_lock)
    monkeypatch.setattr(book_storage, "anna_download_slot", download_slot)
    monkeypatch.setattr(
        book_storage,
        "available_keys",
        lambda: events.append("accounts") or [("key-id", "runtime")],
    )
    monkeypatch.setattr(
        book_storage,
        "_download_from_aa_pool",
        lambda *_args, **_kwargs: ("/tmp/book.pdf", None),
    )

    path, error = book_storage.find_book_file("c" * 32, "pdf")

    assert path == "/tmp/book.pdf"
    assert error is None
    assert events.index("slot-enter") < events.index("accounts")
    assert events.index("accounts") < events.index("slot-exit")


def test_total_inflight_backpressure_and_atomic_reservation(
    monkeypatch, redis_client
):
    monkeypatch.setattr(config, "MAX_QUEUE_DEPTH", 3)
    redis_client.set(_COUNTER_KEYS["queued"], 0)
    redis_client.set(_COUNTER_KEYS["downloading"], 2)
    redis_client.set(_COUNTER_KEYS["parsing"], 1)

    overloaded, reason = is_overloaded()

    assert overloaded is True
    assert "总在途" in reason
    assert _reserve_queue_slot(redis_client) is False


def test_attempt_generation_fences_stale_result_lock_and_counter(redis_client):
    task_id = "fenced_default"
    current_attempt = secrets.token_hex(16)
    stale_attempt = secrets.token_hex(16)
    store_parse_pending(
        redis_client,
        task_id,
        "book.pdf",
        "pdf",
        attempt_id=current_attempt,
    )
    redis_client.set(
        f"parse:lock:{task_id}",
        current_attempt,
        ex=config.PARSE_LOCK_TTL,
    )
    redis_client.set(_COUNTER_KEYS["parsing"], 1)

    stored = store_parse_result(
        redis_client,
        task_id,
        "stale text",
        "mock",
        "book.pdf",
        "3" * 32,
        10,
        1.0,
        "pdf",
        attempt_id=stale_attempt,
    )
    _release_task_lock(redis_client, task_id, stale_attempt)
    _decr_counter_once(
        redis_client, "parsing", task_id, current_attempt
    )
    _decr_counter_once(
        redis_client, "parsing", task_id, current_attempt
    )

    assert stored is False
    assert get_parse_meta(redis_client, task_id)["status"] == "pending"
    assert redis_client.get(f"parse:lock:{task_id}") == current_attempt
    assert int(redis_client.get(_COUNTER_KEYS["parsing"])) == 0

    assert store_parse_error(
        redis_client,
        task_id,
        "timeout",
        "book.pdf",
        "pdf",
        code=504,
        attempt_id=current_attempt,
    )
    late_result = store_parse_result(
        redis_client,
        task_id,
        "late text",
        "mock",
        "book.pdf",
        "3" * 32,
        10,
        1.0,
        "pdf",
        attempt_id=current_attempt,
    )
    assert late_result is False
    assert get_parse_meta(redis_client, task_id)["status"] == "failed"


def test_parse_business_codes_include_next_probe_and_retry_after(redis_client):
    app = Flask(__name__)
    app.register_blueprint(parse_bp)
    client = app.test_client()

    for task_id, error_code, expected_http in (
        ("quota_default", 10002, 429),
        ("disabled_default", 10001, 403),
        ("rate_limit_default", 429, 429),
    ):
        redis_client.setex(
            f"parse:{task_id}:meta",
            60,
            json.dumps({
                "status": "failed",
                "error": "account unavailable",
                "error_code": error_code,
                "next_probe_at": int(time.time()) + 60,
                "retry_after_seconds": 999,
                "chunks": 0,
                "total_length": 0,
            }),
        )
        response = client.get(f"/api/parse/{task_id}")
        body = response.get_json()
        assert response.status_code == expected_http
        assert body["code"] == error_code
        assert body["data"]["next_probe_at"] > int(time.time())
        retry_after = int(response.headers["Retry-After"])
        assert 1 <= retry_after <= 60
        assert body["data"]["retry_after_seconds"] == retry_after


def test_redacted_read_only_health_endpoint(
    monkeypatch, account_pool
):
    first, second = account_pool
    admin_secret = _runtime_secret()
    monkeypatch.setattr(config, "AA_KEY_ADMIN_SECRET", admin_secret)
    app = Flask(__name__)
    app.register_blueprint(aa_keys_bp)
    client = app.test_client()

    response = client.get(
        "/api/admin/aa-keys/health",
        headers={"X-Admin-Secret": admin_secret},
    )
    raw = response.get_data(as_text=True)

    assert response.status_code == 200
    assert response.get_json()["data"]["configured"] == 2
    assert first not in raw
    assert second not in raw
    assert client.post("/api/admin/aa-keys/add").status_code == 404


class _FakeAnnaHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"error": "No downloads left"}).encode()
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


class _FakeAnnaRateLimitHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"error": "Try later"}).encode()
        self.send_response(429)
        self.send_header("Content-Type", "application/json")
        self.send_header("Retry-After", "17")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


class _FakeAnnaPartialDownloadHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/dyn/api/fast_download.json"):
            body = json.dumps({
                "download_url": (
                    f"http://127.0.0.1:{self.server.server_port}/book"
                ),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = b"short"
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", "20")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def log_message(self, *_args):
        return


class _FakeAnnaDownloadHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/dyn/api/fast_download.json"):
            body = json.dumps({
                "download_url": (
                    f"http://127.0.0.1:{self.server.server_port}/book"
                ),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        body = b"%PDF-valid-content-for-atomic-publish-test"
        self.send_response(200)
        self.send_header("Content-Type", "application/pdf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


class _FakeAnnaTinyDownloadHandler(_FakeAnnaDownloadHandler):
    def do_GET(self):
        if self.path.startswith("/dyn/api/fast_download.json"):
            return super().do_GET()

        body = b"qweqwe"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _FakeAnnaUnknownForbiddenHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"error": "temporary authorization issue"}).encode()
        self.send_response(403)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def test_local_fake_annas_quota_response_is_recognized(monkeypatch, tmp_path):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeAnnaHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    runtime_key = _runtime_secret()
    try:
        with pytest.raises(book_storage.AADownloadQuotaExceededError):
            book_storage._download_from_aa(
                str(tmp_path),
                "4" * 32,
                "pdf",
                runtime_key,
                aa_key_pool.key_id(runtime_key),
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_partial_download_is_never_published_or_cached(
    monkeypatch, tmp_path
):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _FakeAnnaPartialDownloadHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    runtime_key = _runtime_secret()
    md5 = "7" * 32
    try:
        path, error = book_storage._download_from_aa(
            str(tmp_path),
            md5,
            "pdf",
            runtime_key,
            aa_key_pool.key_id(runtime_key),
        )
        assert path is None
        assert error
        assert book_storage._find_local(str(tmp_path), md5) is None
        assert list(tmp_path.glob(f"{md5}.pdf.part.*")) == []
        assert not (tmp_path / f"{md5}.pdf").exists()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_tiny_placeholder_is_never_published_or_cached(
    monkeypatch, tmp_path
):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _FakeAnnaTinyDownloadHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    runtime_key = _runtime_secret()
    md5 = "a" * 32
    try:
        path, error = book_storage._download_from_aa(
            str(tmp_path),
            md5,
            "txt",
            runtime_key,
            aa_key_pool.key_id(runtime_key),
        )
        assert path is None
        assert "内容过小" in error
        assert book_storage._find_local(str(tmp_path), md5) is None
        assert list(tmp_path.glob(f"{md5}.txt.part.*")) == []
        assert not (tmp_path / f"{md5}.txt").exists()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_stale_attempt_cannot_publish_download_cache(
    monkeypatch, tmp_path
):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _FakeAnnaDownloadHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    runtime_key = _runtime_secret()
    md5 = "8" * 32
    try:
        path, error = book_storage._download_from_aa(
            str(tmp_path),
            md5,
            "pdf",
            runtime_key,
            aa_key_pool.key_id(runtime_key),
            publish_guard=lambda: False,
        )
        assert path is None
        assert "过期" in error
        assert book_storage._find_local(str(tmp_path), md5) is None
        assert list(tmp_path.glob(f"{md5}.pdf.part.*")) == []
        assert not (tmp_path / f"{md5}.pdf").exists()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_unknown_403_is_transient_and_does_not_disable_account(
    monkeypatch, tmp_path, account_pool
):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _FakeAnnaUnknownForbiddenHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    first, _ = account_pool
    try:
        path, error = book_storage._download_from_aa_pool(
            str(tmp_path),
            "9" * 32,
            "pdf",
            [(aa_key_pool.key_id(first), first)],
        )
        state = next(
            item for item in aa_key_pool.list_keys()
            if item["id"] == aa_key_pool.key_id(first)
        )
        assert path is None
        assert "HTTP 403" in error
        assert state["status"] == "active"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_local_fake_annas_generic_rate_limit_stays_429(
    monkeypatch, tmp_path
):
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        _FakeAnnaRateLimitHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(
        book_storage,
        "AA_BASE_URL",
        f"http://127.0.0.1:{server.server_port}",
    )
    runtime_key = _runtime_secret()
    try:
        with pytest.raises(book_storage.AAUpstreamRateLimitedError) as caught:
            book_storage._download_from_aa(
                str(tmp_path),
                "5" * 32,
                "pdf",
                runtime_key,
                aa_key_pool.key_id(runtime_key),
            )
        assert caught.value.retry_after_seconds == 17
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_download_errors_redact_runtime_key(monkeypatch, tmp_path, caplog):
    runtime_key = _runtime_secret()

    def fail_request(*_args, **_kwargs):
        raise RuntimeError(f"upstream failure {runtime_key}")

    monkeypatch.setattr(book_storage.urllib.request, "urlopen", fail_request)
    path, error = book_storage._download_from_aa(
        str(tmp_path),
        "6" * 32,
        "pdf",
        runtime_key,
        aa_key_pool.key_id(runtime_key),
    )

    assert path is None
    assert runtime_key not in error
    assert runtime_key not in caplog.text
