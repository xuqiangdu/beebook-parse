"""Capture only the cover displayed on an exact Anna book detail page."""

import re
import threading
import time
from urllib.parse import urlsplit

import config
from services.aa_session import new_browser_context, remember_browser_session

MAX_COVER_BYTES = 2 * 1024 * 1024
_browser_slot = threading.BoundedSemaphore(1)


class CoverCaptureBusy(RuntimeError):
    pass


def _remaining_ms(deadline):
    remaining = int((deadline - time.monotonic()) * 1000)
    if remaining <= 0:
        raise TimeoutError("cover capture deadline exceeded")
    return remaining


def _capture_page_cover(page, md5, deadline):
    selector = f'[id="list_cover_aarecord_id__md5:{md5}"]'
    cover = page.locator(selector)
    cover.wait_for(state="visible", timeout=_remaining_ms(deadline))
    # Wait for pending images before accepting the site's fallback card.
    source = page.wait_for_function("""selector => {
        const cover = document.querySelector(selector);
        if (!cover) return false;
        const visible = el => {
            const style = getComputedStyle(el), rect = el.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden'
                && Number(style.opacity) !== 0 && rect.width > 0 && rect.height > 0;
        };
        const images = [...cover.querySelectorAll('img')].filter(visible);
        if (images.some(img => !img.complete)) return false;
        if (images.some(img => img.naturalWidth > 0 && img.naturalHeight > 0)) return 'image';
        const fallback = cover.querySelector('.js-aarecord-list-fallback-cover');
        if (fallback && visible(fallback) && [...fallback.querySelectorAll('[data-content]')]
            .some(el => visible(el) && el.getAttribute('data-content').trim())) return 'placeholder';
        return false;
    }""", arg=selector, timeout=_remaining_ms(deadline))
    try:
        source_kind = source.json_value()
    finally:
        source.dispose()
    box = cover.bounding_box(timeout=_remaining_ms(deadline))
    if not box or not (40 <= box["width"] <= 1200 and 60 <= box["height"] <= 1800):
        raise RuntimeError("invalid cover region dimensions")
    data = cover.screenshot(type="png", timeout=_remaining_ms(deadline))
    if not data or len(data) > MAX_COVER_BYTES:
        raise RuntimeError("cover screenshot exceeds size limit")
    return data, source_kind


def capture_cover(md5):
    if not re.fullmatch(r"[0-9a-fA-F]{32}", md5):
        raise ValueError("invalid book MD5")
    if not config.AA_BROWSER_ENABLED:
        raise RuntimeError("cover browser is disabled")
    if not _browser_slot.acquire(blocking=False):
        raise CoverCaptureBusy("cover browser is busy")
    try:
        from playwright.sync_api import sync_playwright

        md5 = md5.lower()
        base = config.AA_BASE_URL.rstrip("/")
        deadline = time.monotonic() + min(config.AA_BROWSER_TIMEOUT, 45)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
                timeout=_remaining_ms(deadline),
            )
            try:
                context = new_browser_context(browser, base, viewport={"width": 1280, "height": 900})
                page = context.new_page()
                page.goto(f"{base}/md5/{md5}", wait_until="domcontentloaded",
                          timeout=_remaining_ms(deadline))
                data, source_kind = _capture_page_cover(page, md5, deadline)
                if (urlsplit(page.url).path != f"/md5/{md5}"
                        or urlsplit(page.url).netloc != urlsplit(base).netloc):
                    raise RuntimeError("book detail page redirected elsewhere")
                remember_browser_session(context, base, page.evaluate("navigator.userAgent"))
                return data, source_kind
            finally:
                browser.close()
    finally:
        _browser_slot.release()
