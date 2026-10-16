import base64
import struct
import time

import pytest
from flask import Flask
from playwright.sync_api import sync_playwright

from services import cover_service

MD5 = "29f0d48d3aab1648e7aa3fb69324ad1d"
PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="


@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        yield browser
        browser.close()


@pytest.mark.parametrize("kind", ["image", "placeholder", "broken"])
def test_capture_exact_cover(browser, kind):
    page = browser.new_page()
    try:
        image = ""
        if kind == "image":
            image = f'<img width="220" height="330" src="data:image/png;base64,{PNG}">'
        elif kind == "broken":
            image = '<img src="data:image/png;base64,broken">'
        page.set_content(f'''<h1>Unrelated page content</h1>
            <div id="list_cover_aarecord_id__md5:{MD5}" style="width:220px;height:330px;overflow:hidden;background:lavender">
            {image}<div class="js-aarecord-list-fallback-cover"><div data-content="Rich Dad Poor Dad">Rich Dad Poor Dad</div></div></div>''')
        data, source_kind = cover_service._capture_page_cover(page, MD5, time.monotonic() + 3)
        assert source_kind == ("image" if kind == "image" else "placeholder")
        assert data.startswith(b"\x89PNG\r\n\x1a\n")
        width, height = struct.unpack(">II", data[16:24])
        assert width == 220 and height in (330, 331)
    finally:
        page.close()


@pytest.mark.parametrize("html", ["<h1>DDoS-Guard</h1>", '<div id="list_cover_aarecord_id__md5:wrong">Other book</div>', f'<div id="list_cover_aarecord_id__md5:{MD5}" style="width:220px;height:330px"></div>'])
def test_refuse_challenge_wrong_book_and_empty_cover(browser, html):
    page = browser.new_page()
    try:
        page.set_content(html)
        with pytest.raises(Exception):
            cover_service._capture_page_cover(page, MD5, time.monotonic() + 0.2)
    finally:
        page.close()


def test_invalid_md5_and_busy_slot(monkeypatch):
    monkeypatch.setattr(cover_service.config, "AA_BROWSER_ENABLED", True)
    with pytest.raises(ValueError):
        cover_service.capture_cover("../../etc/passwd")
    assert cover_service._browser_slot.acquire(blocking=False)
    try:
        with pytest.raises(cover_service.CoverCaptureBusy):
            cover_service.capture_cover(MD5)
    finally:
        cover_service._browser_slot.release()


def test_endpoint_envelope_and_errors(monkeypatch):
    from api import cover
    app = Flask(__name__)
    app.register_blueprint(cover.cover_bp)
    client = app.test_client()
    monkeypatch.setattr(cover, "capture_cover", lambda md5: (base64.b64decode(PNG), "placeholder"))
    response = client.post(f"/api/covers/{MD5}/screenshot")
    assert response.status_code == 200
    assert response.json["data"]["source_kind"] == "placeholder"
    assert response.json["data"]["mime_type"] == "image/png"

    def fail(_md5):
        raise cover_service.CoverCaptureBusy("busy")
    monkeypatch.setattr(cover, "capture_cover", fail)
    assert client.post(f"/api/covers/{MD5}/screenshot").status_code == 429
    def invalid(_md5):
        raise ValueError("invalid MD5")
    monkeypatch.setattr(cover, "capture_cover", invalid)
    assert client.post("/api/covers/invalid/screenshot").status_code == 400

    def unavailable(_md5):
        raise RuntimeError("challenge")
    monkeypatch.setattr(cover, "capture_cover", unavailable)
    response = client.post(f"/api/covers/{MD5}/screenshot")
    assert response.status_code == 502 and response.json["data"] is None


def test_browser_closed_and_slot_released_on_error(monkeypatch):
    from unittest.mock import Mock
    import playwright.sync_api
    browser = Mock()
    browser.new_context.side_effect = RuntimeError("failed context")
    runtime = Mock()
    runtime.chromium.launch.return_value = browser
    manager = Mock()
    manager.__enter__ = Mock(return_value=runtime)
    manager.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(playwright.sync_api, "sync_playwright", lambda: manager)
    monkeypatch.setattr(cover_service.config, "AA_BROWSER_ENABLED", True)
    with pytest.raises(RuntimeError, match="failed context"):
        cover_service.capture_cover(MD5)
    browser.close.assert_called_once()
    assert cover_service._browser_slot.acquire(blocking=False)
    cover_service._browser_slot.release()
