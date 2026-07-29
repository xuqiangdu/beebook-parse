from __future__ import annotations

import importlib

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
