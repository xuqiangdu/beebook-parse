from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date, datetime

import requests

from services.redis_store import get_redis
import config

logger = logging.getLogger(__name__)

KEY_SET = "aa:keys"
KEY_ADDED_AT = "aa:key:added_at"
KEY_EXPIRY = "aa:key:expiry"          # hash: kid -> 会员到期时间戳(unix 秒，到期日 00:00)
COOLDOWN_PREFIX = "aa:key:cooldown:"
DISABLED_PREFIX = "aa:key:disabled:"

# AA 账户页会员到期行：会员：<strong>xxx</strong>，2026年5月15日 到期
_EXPIRY_RE = re.compile(
    r"会员[：:].*?(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*到期"
)


def key_id(secret_key: str) -> str:
    return hashlib.sha256(secret_key.encode("utf-8")).hexdigest()[:12]


def _cooldown_key(kid: str) -> str:
    return f"{COOLDOWN_PREFIX}{kid}"


def _disabled_key(kid: str) -> str:
    return f"{DISABLED_PREFIX}{kid}"


def _env_keys() -> list[str]:
    keys: list[str] = []
    if config.AA_SECRET_KEY.strip():
        keys.append(config.AA_SECRET_KEY.strip())
    for item in config.AA_SECRET_KEYS.split(","):
        item = item.strip()
        if item:
            keys.append(item)
    return list(dict.fromkeys(keys))


def seed_keys_from_env() -> int:
    """Import Docker/env configured keys into Redis on startup."""
    added = 0
    for secret_key in _env_keys():
        if add_key(secret_key, source="env"):
            added += 1
    return added


def add_key(secret_key: str, source: str = "api") -> bool:
    secret_key = secret_key.strip()
    if not secret_key:
        return False

    r = get_redis()
    inserted = bool(r.sadd(KEY_SET, secret_key))
    kid = key_id(secret_key)
    if inserted:
        r.hset(KEY_ADDED_AT, kid, str(int(time.time())))
        logger.info("AA key added: id=%s source=%s", kid, source)
    if inserted or source == "api":
        r.delete(_cooldown_key(kid), _disabled_key(kid))
    return inserted


def list_keys() -> list[dict]:
    r = get_redis()
    keys = sorted(r.smembers(KEY_SET), key=lambda item: key_id(item))
    added_at = r.hgetall(KEY_ADDED_AT)
    expiry_map = r.hgetall(KEY_EXPIRY)
    items: list[dict] = []
    for secret_key in keys:
        kid = key_id(secret_key)
        disabled_reason = r.get(_disabled_key(kid))
        cooldown_reason = r.get(_cooldown_key(kid))
        cooldown_ttl = r.ttl(_cooldown_key(kid))
        status = "active"
        reason = None
        if disabled_reason:
            status = "disabled"
            reason = disabled_reason
        elif cooldown_reason:
            status = "cooldown"
            reason = cooldown_reason
        expiry = _to_ts(expiry_map.get(kid))
        items.append({
            "id": kid,
            "status": status,
            "reason": reason,
            "cooldown_ttl": cooldown_ttl if cooldown_ttl and cooldown_ttl > 0 else 0,
            "added_at": int(added_at.get(kid, "0") or 0),
            # 会员到期时间戳(unix 秒;缓存值,不主动登录三方,实时刷新走 check_expiry)
            "expiry": expiry,
            "days_left": _days_until(expiry),
        })
    return items


def _to_ts(value: str | None) -> int | None:
    """Redis 里的到期时间(字符串)转 int 时间戳，无效返回 None。"""
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _days_until(ts: int | None) -> int | None:
    """到期时间戳距今天的天数，无法解析返回 None。"""
    if not ts:
        return None
    try:
        return (datetime.fromtimestamp(ts).date() - date.today()).days
    except (ValueError, TypeError, OSError):
        return None


def _fetch_expiry_from_aa(secret_key: str) -> int | None:
    """登录 AA 账户页，解析会员到期时间。

    账户可能有多条会员记录，取最早到期的一条作为账号实际过期时间。
    返回到期日 00:00 的 unix 时间戳(秒)，失败返回 None。
    """
    try:
        session = requests.Session()
        resp = session.post(
            f"{config.AA_BASE_URL}/account/",
            data={"key": secret_key},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=config.AA_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning("拉取 AA 会员到期时间失败: id=%s err=%s", key_id(secret_key), e)
        return None

    dates: list[date] = []
    for y, m, d in _EXPIRY_RE.findall(html):
        try:
            dates.append(date(int(y), int(m), int(d)))
        except ValueError:
            continue
    if not dates:
        logger.warning(
            "AA 账户页未解析到会员到期时间: id=%s (key 可能无效或未开通会员)",
            key_id(secret_key),
        )
        return None
    return int(datetime.combine(min(dates), datetime.min.time()).timestamp())


def get_key_expiry(secret_key: str, force: bool = False) -> dict:
    """获取单个 key 的会员到期时间。

    刷新逻辑（特色：人工续费后无感刷新）：
      - 无缓存 / force=True             → 登录三方拉取
      - 有缓存且距今 < 刷新阈值天数      → 登录三方二次拉取（续费后能拿到新到期时间）
      - 有缓存且距今 >= 刷新阈值天数     → 直接用缓存，不打三方
    """
    r = get_redis()
    kid = key_id(secret_key)
    cached = _to_ts(r.hget(KEY_EXPIRY, kid))
    cached_days = _days_until(cached)

    need_fetch = (
        force
        or cached is None
        or cached_days is None                       # 缓存值损坏
        or cached_days < config.AA_KEY_EXPIRY_REFRESH_DAYS
    )

    expiry = cached
    source = "cache"
    if need_fetch:
        fetched = _fetch_expiry_from_aa(secret_key)
        if fetched:
            r.hset(KEY_EXPIRY, kid, fetched)
            expiry = fetched
            source = "remote"
        else:
            # 三方拉取失败：有旧缓存就继续用，没有则置空
            source = "cache_stale" if cached else "unknown"

    return {
        "id": kid,
        "expiry": expiry,
        "days_left": _days_until(expiry),
        "source": source,
    }


def check_expiry(force: bool = False) -> list[dict]:
    """检查池内所有 key 的会员到期时间。"""
    r = get_redis()
    keys = sorted(r.smembers(KEY_SET), key=lambda item: key_id(item))
    return [get_key_expiry(secret_key, force=force) for secret_key in keys]


def available_keys() -> list[tuple[str, str]]:
    r = get_redis()
    keys = sorted(r.smembers(KEY_SET), key=lambda item: key_id(item))
    result: list[tuple[str, str]] = []
    for secret_key in keys:
        kid = key_id(secret_key)
        if r.exists(_disabled_key(kid)) or r.exists(_cooldown_key(kid)):
            continue
        result.append((kid, secret_key))
    return result


def unavailable_status() -> str | None:
    """Return the business reason when the pool exists but no key is active."""
    items = list_keys()
    if not items:
        return None
    if any(item["status"] == "active" for item in items):
        return None
    if any(item["status"] == "cooldown" for item in items):
        return "quota"
    return "disabled"


def mark_quota_exhausted(secret_key: str, reason: str) -> None:
    kid = key_id(secret_key)
    ttl = max(config.AA_KEY_COOLDOWN_SECONDS, 60)
    get_redis().setex(_cooldown_key(kid), ttl, reason)
    logger.warning("AA key quota exhausted: id=%s ttl=%ss reason=%s", kid, ttl, reason)


def mark_disabled(secret_key: str, reason: str) -> None:
    kid = key_id(secret_key)
    get_redis().set(_disabled_key(kid), reason)
    logger.error("AA key disabled: id=%s reason=%s", kid, reason)
