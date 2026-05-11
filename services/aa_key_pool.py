from __future__ import annotations

import hashlib
import logging
import time

from services.redis_store import get_redis
import config

logger = logging.getLogger(__name__)

KEY_SET = "aa:keys"
KEY_ADDED_AT = "aa:key:added_at"
COOLDOWN_PREFIX = "aa:key:cooldown:"
DISABLED_PREFIX = "aa:key:disabled:"


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
        items.append({
            "id": kid,
            "status": status,
            "reason": reason,
            "cooldown_ttl": cooldown_ttl if cooldown_ttl and cooldown_ttl > 0 else 0,
            "added_at": int(added_at.get(kid, "0") or 0),
        })
    return items


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
