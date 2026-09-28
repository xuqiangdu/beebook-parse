from __future__ import annotations

import importlib
from unittest.mock import Mock, call

import pytest
import requests

import config


@pytest.fixture(scope="module")
def search_service():
    monkeypatch = pytest.MonkeyPatch()

    def offline(*_args, **_kwargs):
        raise requests.ConnectionError("offline test")

    monkeypatch.setattr(
        config,
        "AA_CANDIDATE_URLS",
        ["https://anna-search.invalid"],
    )
    monkeypatch.setattr(
        config,
        "AA_BASE_URL",
        "https://anna-search.invalid",
    )
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", False)
    monkeypatch.setattr(
        requests.Session,
        "get",
        offline,
    )
    module = importlib.import_module("services.search_service")
    monkeypatch.undo()
    return module


def _search_html(*cards: str) -> str:
    return (
        '<div class="js-aarecord-list-outer">'
        + "".join(cards)
        + "</div>"
    )


def _card(md5: str, meta: str, css_class: str = "") -> str:
    return f"""
    <div>
      <a href="/md5/{md5}"><img src="/cover.jpg"></a>
      <a class="line-clamp-1 {css_class}">Book {md5[:4]}</a>
      <a class="line-clamp-1">Author</a>
      <div class="font-semibold text-gray-500 {css_class}">{meta}</div>
    </div>
    """


def test_css_bracket_values_are_not_languages(search_service):
    md5 = "1" * 32
    html = _search_html(
        _card(md5, "PDF · 1.2MB", "text-[7pt] leading-[9px]"),
    )

    result = search_service._parse_with_regex(html)

    assert result["results"][0]["language"] == ""
    assert result["results"][0]["languages"] == []
    assert result["results"][0]["extension"] == "pdf"


def test_single_requested_language_fills_missing_page_language(search_service):
    md5 = "2" * 32
    html = _search_html(
        _card(md5, "EPUB · 2.5MB", "text-[7pt]"),
    )

    result = search_service._parse_with_bs4(html)
    search_service._apply_requested_fallbacks(
        result["results"],
        "ja",
        "",
    )

    assert result["results"][0]["language"] == "ja"
    assert result["results"][0]["languages"] == ["ja"]
    assert result["results"][0]["extension"] == "epub"


def test_search_books_applies_single_request_fallback(
    monkeypatch, search_service
):
    md5 = "5" * 32
    html = _search_html(_card(md5, "1.5MB · 📘 Book"))

    class Response:
        status_code = 200
        text = html

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setattr(
        search_service,
        "_alive_mirrors",
        ["https://anna-search.invalid"],
    )
    monkeypatch.setattr(
        search_service._session,
        "get",
        lambda *_args, **_kwargs: Response(),
    )

    result = search_service.search_books(
        query="book",
        lang=["de"],
        ext=["epub"],
    )

    assert result["results"][0]["language"] == "de"
    assert result["results"][0]["extension"] == "epub"


def test_search_books_does_not_return_partial_matches(
    monkeypatch, search_service
):
    partial_md5 = "6" * 32
    html = (
        _search_html()
        + '<div class="js-partial-matches-remove">'
        + "<button>Show 1 partial matches</button>"
        + _card(partial_md5, "English [en] · EPUB · 1.5MB")
        + "</div>"
    )

    class Response:
        status_code = 200
        text = html

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setattr(
        search_service,
        "_alive_mirrors",
        ["https://anna-search.invalid"],
    )
    monkeypatch.setattr(
        search_service._session,
        "get",
        lambda *_args, **_kwargs: Response(),
    )

    result = search_service.search_books(
        query="missing exact title",
        lang=["en"],
        ext=["epub"],
    )

    assert result["total"] == 0
    assert result["results"] == []


def test_page_language_and_extension_win_over_request_fallback(search_service):
    info = search_service._parse_meta("English [en] · PDF · 3MB")

    search_service._apply_requested_fallbacks([info], "de", "epub")

    assert info["language"] == "en"
    assert info["languages"] == ["en"]
    assert info["extension"] == "pdf"


def test_multiple_or_invalid_requested_languages_are_not_guessed(
    search_service,
):
    assert search_service._single_requested_language(["de", "ja"]) == ""
    assert search_service._single_requested_language(["7pt"]) == ""
    assert search_service._single_requested_language(["9px"]) == ""
    assert search_service._single_requested_language(["zh-hant"]) == "zh-Hant"


def test_regex_fallback_preserves_result_order(search_service):
    first = "3" * 32
    second = "4" * 32
    html = _search_html(
        _card(first, "PDF · 1MB"),
        _card(second, "EPUB · 2MB"),
    )

    result = search_service._parse_with_regex(html)
    search_service._apply_requested_fallbacks(
        result["results"],
        "de",
        "",
    )

    assert [item["md5"] for item in result["results"]] == [first, second]
    assert [item["language"] for item in result["results"]] == ["de", "de"]
    assert [item["extension"] for item in result["results"]] == [
        "pdf",
        "epub",
    ]


@pytest.mark.parametrize("html", [
    '<html><title>DDoS-Guard</title><body>Checking your browser</body></html>',
    '<html><title>Maintenance</title><body>Please try later</body></html>',
])
def test_invalid_search_page_is_an_error(monkeypatch, search_service, html):
    response = requests.Response()
    response.status_code = 200
    response._content = html.encode()
    monkeypatch.setattr(search_service, "_alive_mirrors", ["https://anna-search.invalid"])
    monkeypatch.setattr(search_service._session, "get", lambda *a, **kw: response)
    monkeypatch.setattr(search_service, "_trigger_refresh_async", lambda: None)

    result = search_service.search_books(query="Python")

    assert result["results"] == []
    assert "error" in result


def test_invalid_page_tries_next_mirror(monkeypatch, search_service):
    md5 = "7" * 32
    pages = iter([
        '<html><title>DDoS-Guard</title></html>',
        _search_html(_card(md5, "English [en] · EPUB · 1MB")),
    ])
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        response = requests.Response()
        response.status_code = 200
        response._content = next(pages).encode()
        response.encoding = "utf-8"
        return response

    monkeypatch.setattr(search_service, "_alive_mirrors", [
        "https://first.invalid", "https://second.invalid",
    ])
    monkeypatch.setattr(search_service._session, "get", get)
    monkeypatch.setattr(search_service, "_trigger_refresh_async", lambda: None)

    result = search_service.search_books(query="Python")

    assert [item["md5"] for item in result["results"]] == [md5]
    assert calls == ["https://first.invalid/search", "https://second.invalid/search"]


@pytest.mark.parametrize("html, expected_status", [
    ('<html><title>DDoS-Guard</title></html>', 502),
    (_search_html(), 200),
])
def test_search_api_distinguishes_blocked_and_empty(
    monkeypatch, search_service, html, expected_status,
):
    from flask import Flask
    from api.search import search_bp

    response = requests.Response()
    response.status_code = 200
    response._content = html.encode()
    monkeypatch.setattr(search_service, "_alive_mirrors", ["https://anna-search.invalid"])
    monkeypatch.setattr(search_service._session, "get", lambda *a, **kw: response)
    monkeypatch.setattr(search_service, "_trigger_refresh_async", lambda: None)
    app = Flask(__name__)
    app.register_blueprint(search_bp)

    result = app.test_client().get("/api/search?q=Python")

    assert result.status_code == expected_status
    assert result.json["code"] == (502 if expected_status == 502 else 0)
    assert result.json["data"] == {"total": 0, "results": []}


def test_all_blocked_mirrors_recover_browser_session(monkeypatch, search_service):
    candidate = "https://candidate.invalid"
    base = "https://configured.invalid"
    monkeypatch.setattr(config, "AA_CANDIDATE_URLS", [candidate])
    monkeypatch.setattr(config, "AA_BASE_URL", base)
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", True)
    probe = Mock(side_effect=[(candidate, None), (base, 100)])
    refresh = Mock()
    monkeypatch.setattr(search_service, "_probe_mirror", probe)
    monkeypatch.setattr(search_service, "_refresh_browser_session", refresh)

    assert search_service._probe_all_mirrors() == [base]

    refresh.assert_called_once_with(base)
    assert probe.call_args_list == [
        call(candidate, config.AA_PROBE_TIMEOUT),
        call(base, config.AA_REQUEST_TIMEOUT),
    ]


def test_working_http_mirror_does_not_launch_browser(monkeypatch, search_service):
    candidate = "https://candidate.invalid"
    monkeypatch.setattr(config, "AA_CANDIDATE_URLS", [candidate])
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", True)
    monkeypatch.setattr(search_service, "_probe_mirror", lambda *a: (candidate, 100))
    refresh = Mock()
    monkeypatch.setattr(search_service, "_refresh_browser_session", refresh)

    assert search_service._probe_all_mirrors() == [candidate]

    refresh.assert_not_called()


def test_browser_failure_keeps_service_available(monkeypatch, search_service):
    base = "https://configured.invalid"
    monkeypatch.setattr(config, "AA_CANDIDATE_URLS", [base])
    monkeypatch.setattr(config, "AA_BASE_URL", base)
    monkeypatch.setattr(config, "AA_BROWSER_ENABLED", True)
    monkeypatch.setattr(search_service, "_probe_mirror", lambda *a: (base, None))
    refresh = Mock(side_effect=TimeoutError("verification unavailable"))
    monkeypatch.setattr(search_service, "_refresh_browser_session", refresh)

    assert search_service._probe_all_mirrors() == [base]

    refresh.assert_called_once_with(base)
