import importlib
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pytest
import requests
from playwright.sync_api import BrowserType

import config
from services import aa_session, cover_service

MD5 = "29f0d48d3aab1648e7aa3fb69324ad1d"


@pytest.fixture(autouse=True)
def isolated_session(monkeypatch):
    session = requests.Session()
    monkeypatch.setattr(aa_session, "session", session)
    monkeypatch.setattr(aa_session, "_browser_user_agents", {})
    yield session
    session.close()


def test_restore_only_unexpired_matching_domain_cookies(isolated_session):
    jar = isolated_session.cookies
    jar.set("valid", "test", domain=".annas-archive.gl", path="/", expires=int(time.time()) + 3600,
            secure=True, rest={"HttpOnly": True, "SameSite": "strict"})
    jar.set("expired", "test", domain=".annas-archive.gl", expires=int(time.time()) - 1)
    jar.set("other", "test", domain=".annas-archive.gd")
    jar.set("wrong-suffix", "test", domain=".notannas-archive.gl")
    jar.set("host-only", "test", domain="annas-archive.gl")
    browser = Mock()

    aa_session.new_browser_context(browser, "https://zh.annas-archive.gl", viewport={"width": 1280, "height": 900})

    restored = browser.new_context.return_value.add_cookies.call_args.args[0]
    assert [cookie["name"] for cookie in restored] == ["valid"]
    assert restored[0]["httpOnly"] is True and restored[0]["secure"] is True
    assert restored[0]["sameSite"] == "Strict"
    assert "user_agent" not in browser.new_context.call_args.kwargs


def test_verified_browser_cookies_and_user_agent_are_reused(isolated_session):
    context = Mock()
    context.cookies.return_value = [
        {"name": "verified", "value": "test-only", "domain": ".annas-archive.gl", "path": "/",
         "expires": -1, "httpOnly": True, "secure": True, "sameSite": "Lax"},
        {"name": "other", "value": "test-only", "domain": ".annas-archive.gd", "path": "/", "expires": -1},
    ]
    aa_session.remember_browser_session(context, "https://zh.annas-archive.gl", "verified-browser-agent")
    browser = Mock()
    aa_session.new_browser_context(browser, "https://zh.annas-archive.gl")

    assert isolated_session.cookies.get("verified") == "test-only"
    assert isolated_session.cookies.get("other") is None
    assert isolated_session.headers["User-Agent"] == "verified-browser-agent"
    assert browser.new_context.call_args.kwargs["user_agent"] == "verified-browser-agent"
    restored = browser.new_context.return_value.add_cookies.call_args.args[0]
    assert restored[0]["httpOnly"] is True and restored[0]["sameSite"] == "Lax"
    assert "expires" not in restored[0]


def test_expired_browser_cookie_is_not_kept_as_a_session_cookie(isolated_session):
    isolated_session.cookies.set("expired", "old", domain="annas-archive.gl", path="/")
    context = Mock()
    context.cookies.return_value = [{"name": "expired", "value": "deleted", "domain": "annas-archive.gl",
                                    "path": "/", "expires": 0}]
    aa_session.remember_browser_session(context, "https://annas-archive.gl", "browser-agent")
    assert isolated_session.cookies.get("expired") is None


def test_search_then_two_captures_share_cookie_with_fresh_browsers(monkeypatch, isolated_session):
    # Import search without triggering live mirror probes or using account credentials.
    with monkeypatch.context() as offline:
        offline.setattr(config, "AA_CANDIDATE_URLS", ["https://offline.invalid"])
        offline.setattr(config, "AA_BASE_URL", "https://offline.invalid")
        offline.setattr(config, "AA_BROWSER_ENABLED", False)
        offline.setattr(config, "AA_SECRET_KEY", "")
        offline.setattr(config, "AA_SECRET_KEYS", "")
        offline.setattr(requests.Session, "get", Mock(side_effect=requests.ConnectionError("offline")))
        search = importlib.import_module("services.search_service")
    monkeypatch.setattr(search, "_session", isolated_session)
    original_launch = BrowserType.launch

    def headless_launch(browser_type, **options):
        options["headless"] = True
        return original_launch(browser_type, **options)
    monkeypatch.setattr(BrowserType, "launch", headless_launch)
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith(("/search", "/md5/", "/probe")):
                seen.append((self.path, self.headers.get("Cookie", ""), self.headers.get("User-Agent")))
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Set-Cookie", "verified=test-only; Path=/; Max-Age=3600; HttpOnly; SameSite=Lax")
                self.end_headers()
                self.wfile.write(f'''<div class="js-aarecord-list-outer"></div>
                    <div id="list_cover_aarecord_id__md5:{MD5}" style="width:220px;height:330px;background:lavender">
                    <div class="js-aarecord-list-fallback-cover"><div data-content="Test Book">Test Book</div></div></div>'''.encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setattr(config, "AA_BASE_URL", base)
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", True)
    try:
        search._refresh_browser_session(base)
        for _ in range(2):
            image, kind = cover_service.capture_cover(MD5)
            assert image.startswith(b"\x89PNG") and kind == "placeholder"
        assert isolated_session.get(base + "/probe", timeout=3).status_code == 200
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)

    assert len(seen) == 4
    assert seen[0][1] == ""
    assert all("verified=test-only" in cookie for _, cookie, _ in seen[1:])
    assert len({agent for _, _, agent in seen}) == 1


def test_failed_capture_does_not_replace_verified_cookies(monkeypatch, isolated_session):
    import playwright.sync_api
    isolated_session.cookies.set("verified", "keep", domain="annas-archive.gl")
    browser = Mock()
    runtime = Mock()
    runtime.chromium.launch.return_value = browser
    manager = Mock()
    manager.__enter__ = Mock(return_value=runtime)
    manager.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(playwright.sync_api, "sync_playwright", lambda: manager)
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", True)
    monkeypatch.setattr(config, "AA_BASE_URL", "https://annas-archive.gl")
    monkeypatch.setattr(cover_service, "_capture_page_cover", Mock(side_effect=TimeoutError("challenge")))
    remember = Mock()
    monkeypatch.setattr(cover_service, "remember_browser_session", remember)

    with pytest.raises(TimeoutError):
        cover_service.capture_cover(MD5)

    remember.assert_not_called()
    assert isolated_session.cookies.get("verified") == "keep"
