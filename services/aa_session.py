"""Share Anna HTTP and verified browser cookies only within this process."""

import threading
import time
from urllib.parse import urlsplit

import requests

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
})
_lock = threading.Lock()
_browser_user_agents = {}


def _matches_host(domain, host):
    normalized = domain.lstrip(".").lower()
    return bool(normalized) and (host == normalized or (
        domain.startswith(".") and host.endswith("." + normalized)))


def new_browser_context(browser, base, http_session=None, **options):
    http_session = http_session if http_session is not None else session
    host = urlsplit(base).hostname or ""
    cookies = []
    with _lock:
        user_agent = _browser_user_agents.get(host)
        for cookie in http_session.cookies:
            if not _matches_host(cookie.domain, host) or cookie.is_expired():
                continue
            restored = {
                "name": cookie.name, "value": cookie.value,
                "domain": cookie.domain, "path": cookie.path or "/",
                "secure": cookie.secure,
                "httpOnly": cookie.has_nonstandard_attr("HttpOnly")
                    and cookie.get_nonstandard_attr("HttpOnly") is not False,
            }
            if cookie.expires is not None:
                restored["expires"] = cookie.expires
            same_site = cookie.get_nonstandard_attr("SameSite")
            if isinstance(same_site, str):
                same_site = {"lax": "Lax", "strict": "Strict", "none": "None"}.get(same_site.lower())
            if same_site in ("Lax", "Strict", "None"):
                restored["sameSite"] = same_site
            cookies.append(restored)
    if user_agent:
        options["user_agent"] = user_agent
    context = browser.new_context(**options)
    if cookies:
        context.add_cookies(cookies)
    return context


def remember_browser_session(context, base, user_agent, http_session=None):
    """Call only after the expected page has been verified, never on challenge pages."""
    http_session = http_session if http_session is not None else session
    host = urlsplit(base).hostname or ""
    cookies = context.cookies()
    with _lock:
        _browser_user_agents[host] = user_agent
        http_session.headers["User-Agent"] = user_agent
        for cookie in cookies:
            if not _matches_host(cookie["domain"], host):
                continue
            expires = cookie.get("expires", -1)
            if expires >= 0 and expires <= time.time():
                try:
                    http_session.cookies.clear(cookie["domain"], cookie["path"], cookie["name"])
                except KeyError:
                    pass
                continue
            rest = {}
            if cookie.get("httpOnly"):
                rest["HttpOnly"] = True
            if cookie.get("sameSite") in ("Lax", "Strict", "None"):
                rest["SameSite"] = cookie["sameSite"]
            http_session.cookies.set(
                cookie["name"], cookie["value"], domain=cookie["domain"],
                path=cookie.get("path", "/"), secure=cookie.get("secure", False),
                expires=int(expires) if expires > 0 else None, rest=rest,
            )
