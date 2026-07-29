from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime

import requests

import config
from services.redis_store import get_redis

logger = logging.getLogger(__name__)

# Only redacted identifiers and control metadata are stored in Redis.
KEY_IDS = "aa:control:key_ids"
KEY_STATE_PREFIX = "aa:control:key:"
KEY_EXPIRY = "aa:control:key_expiry"
ROTATION_KEY = "aa:control:rotation"
PROBE_LEASE_PREFIX = "aa:control:probe:"
CONTENT_LEASE_PREFIX = "aa:control:content:"

# Removed on startup because the legacy implementation stored raw keys here.
LEGACY_RAW_KEY_SET = "aa:keys"

_secrets_lock = threading.Lock()
_secret_by_id: dict[str, str] = {}

_EXPIRY_RE = re.compile(
    r"会员[：:].*?(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*到期"
)

_RENEW_STRING_LEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
return 0
"""

_RELEASE_STRING_LEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class AnnaContentBusyError(Exception):
    """Another worker held the same-content lock past the wait deadline."""


def key_id(secret_key: str) -> str:
    return hashlib.sha256(secret_key.encode("utf-8")).hexdigest()[:12]


def _state_key(kid: str) -> str:
    return f"{KEY_STATE_PREFIX}{kid}"


def _probe_lease_key(kid: str) -> str:
    return f"{PROBE_LEASE_PREFIX}{kid}"


def _env_keys() -> list[str]:
    keys: list[str] = []
    if config.AA_SECRET_KEY.strip():
        keys.append(config.AA_SECRET_KEY.strip())
    for item in config.AA_SECRET_KEYS.split(","):
        item = item.strip()
        if item:
            keys.append(item)
    return list(dict.fromkeys(keys))


def _configured_secrets() -> dict[str, str]:
    with _secrets_lock:
        return dict(_secret_by_id)


def _safe_reason(reason: str) -> str:
    value = str(reason or "")
    for secret_key in _configured_secrets().values():
        if secret_key:
            value = value.replace(secret_key, "[redacted]")
    return value[:500]


def seed_keys_from_env() -> int:
    """Load Anna keys into process memory and persist only redacted IDs."""
    secrets = {key_id(secret_key): secret_key for secret_key in _env_keys()}
    with _secrets_lock:
        _secret_by_id.clear()
        _secret_by_id.update(secrets)

    r = get_redis()
    previous = set(r.smembers(KEY_IDS))
    pipe = r.pipeline()
    pipe.delete(LEGACY_RAW_KEY_SET)
    pipe.delete(KEY_IDS)
    if secrets:
        pipe.sadd(KEY_IDS, *sorted(secrets))
    now = int(time.time())
    for kid in secrets:
        state_key = _state_key(kid)
        pipe.hsetnx(state_key, "status", "active")
        pipe.hsetnx(state_key, "added_at", now)
        pipe.hsetnx(state_key, "last_success_at", 0)
        pipe.hsetnx(state_key, "last_error_at", 0)
        pipe.hsetnx(state_key, "last_error", "")
        pipe.hsetnx(state_key, "next_probe_at", 0)
    pipe.execute()
    added = len(set(secrets) - previous)
    logger.info("AA account pool loaded from environment: accounts=%s new=%s",
                len(secrets), added)
    return added


def _ensure_loaded() -> dict[str, str]:
    secrets = _configured_secrets()
    if secrets or not _env_keys():
        return secrets
    seed_keys_from_env()
    return _configured_secrets()


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return default


def _state_for(r, kid: str) -> dict:
    state = r.hgetall(_state_key(kid))
    if not state:
        state = {"status": "active"}
    return state


def list_keys() -> list[dict]:
    """Return redacted account state. Raw keys never leave process memory."""
    secrets = _ensure_loaded()
    r = get_redis()
    now = int(time.time())
    expiry_map = r.hgetall(KEY_EXPIRY)
    items: list[dict] = []
    for kid in sorted(secrets):
        state = _state_for(r, kid)
        status = state.get("status") or "active"
        next_probe_at = _to_int(state.get("next_probe_at"))
        expiry = _to_ts(expiry_map.get(kid))
        items.append({
            "id": kid,
            "status": status,
            "last_success_at": _to_int(state.get("last_success_at")),
            "last_error_at": _to_int(state.get("last_error_at")),
            "last_error": state.get("last_error") or None,
            "next_probe_at": next_probe_at,
            "retry_after_seconds": max(next_probe_at - now, 0),
            "probe_due": status != "active" and next_probe_at > 0
                         and next_probe_at <= now,
            "added_at": _to_int(state.get("added_at")),
            "expiry": expiry,
            "days_left": _days_until(expiry),
        })
    return items


def pool_health(include_accounts: bool = True) -> dict:
    items = list_keys()
    summary = {
        "configured": len(items),
        "active": sum(item["status"] == "active" for item in items),
        "cooldown": sum(item["status"] == "cooldown" for item in items),
        "disabled": sum(item["status"] == "disabled" for item in items),
    }
    probes = [
        item["next_probe_at"] for item in items
        if item["next_probe_at"] > 0
    ]
    summary["next_probe_at"] = min(probes) if probes else 0
    if include_accounts:
        summary["accounts"] = items
    return summary


def _rotated_ids(r, ids: list[str]) -> list[str]:
    if not ids:
        return []
    cursor = int(r.incr(ROTATION_KEY)) - 1
    start = cursor % len(ids)
    return ids[start:] + ids[:start]


def available_keys() -> list[tuple[str, str]]:
    """Return usable accounts in an atomically rotated order.

    Cooldown/disabled accounts are selected only when their next probe is due,
    and a Redis probe lease prevents concurrent probe storms.
    """
    secrets = _ensure_loaded()
    if not secrets:
        return []

    r = get_redis()
    now = int(time.time())
    result: list[tuple[str, str]] = []
    for kid in _rotated_ids(r, sorted(secrets)):
        state = _state_for(r, kid)
        status = state.get("status") or "active"
        if status == "active":
            result.append((kid, secrets[kid]))
            continue

        next_probe_at = _to_int(state.get("next_probe_at"))
        if next_probe_at <= 0 or next_probe_at > now:
            continue
        probe_token = uuid.uuid4().hex
        if r.set(
            _probe_lease_key(kid),
            probe_token,
            nx=True,
            ex=max(config.AA_KEY_PROBE_LEASE_SECONDS, 1),
        ):
            result.append((kid, secrets[kid]))
    return result


def key_is_available(kid: str) -> bool:
    """Recheck one selected account immediately before an upstream call."""
    secrets = _ensure_loaded()
    if kid not in secrets:
        return False

    r = get_redis()
    state = _state_for(r, kid)
    status = state.get("status") or "active"
    if status == "active":
        return True

    next_probe_at = _to_int(state.get("next_probe_at"))
    if next_probe_at <= 0 or next_probe_at > int(time.time()):
        return False

    # available_keys() creates this lease for a due probe. A selected probe
    # remains valid only while that lease is still alive.
    return bool(r.exists(_probe_lease_key(kid)))


def unavailable_info() -> dict:
    """Describe the current pool-level business state without secrets."""
    items = list_keys()
    if not items:
        return {
            "reason": "unconfigured",
            "next_probe_at": 0,
            "retry_after_seconds": 0,
        }
    if any(item["status"] == "active" for item in items):
        return {
            "reason": "available",
            "next_probe_at": 0,
            "retry_after_seconds": 0,
        }

    cooldown = [item for item in items if item["status"] == "cooldown"]
    reason = "quota" if cooldown else "disabled"
    candidates = [
        item["next_probe_at"] for item in items
        if item["next_probe_at"] > 0
    ]
    next_probe_at = min(candidates) if candidates else 0
    return {
        "reason": reason,
        "next_probe_at": next_probe_at,
        "retry_after_seconds": max(next_probe_at - int(time.time()), 0),
    }


def unavailable_status() -> str | None:
    info = unavailable_info()
    if info["reason"] in {"available", "unconfigured"}:
        return None
    return info["reason"]


def mark_success(secret_key: str) -> None:
    kid = key_id(secret_key)
    now = int(time.time())
    r = get_redis()
    r.hset(_state_key(kid), mapping={
        "status": "active",
        "last_success_at": now,
        "next_probe_at": 0,
    })
    r.delete(_probe_lease_key(kid))


def mark_transient_error(secret_key: str, reason: str) -> None:
    kid = key_id(secret_key)
    get_redis().hset(_state_key(kid), mapping={
        "last_error_at": int(time.time()),
        "last_error": _safe_reason(reason),
    })


def mark_quota_exhausted(secret_key: str, reason: str) -> None:
    kid = key_id(secret_key)
    now = int(time.time())
    next_probe_at = now + max(config.AA_KEY_COOLDOWN_SECONDS, 60)
    r = get_redis()
    r.hset(_state_key(kid), mapping={
        "status": "cooldown",
        "quota_exhausted_at": now,
        "last_error_at": now,
        "last_error": _safe_reason(reason),
        "next_probe_at": next_probe_at,
    })
    r.delete(_probe_lease_key(kid))
    logger.warning("AA account quota exhausted: id=%s next_probe_at=%s",
                   kid, next_probe_at)


def mark_disabled(secret_key: str, reason: str) -> None:
    kid = key_id(secret_key)
    now = int(time.time())
    next_probe_at = now + max(config.AA_KEY_DISABLED_PROBE_SECONDS, 60)
    r = get_redis()
    r.hset(_state_key(kid), mapping={
        "status": "disabled",
        "disabled_at": now,
        "last_error_at": now,
        "last_error": _safe_reason(reason),
        "next_probe_at": next_probe_at,
    })
    r.delete(_probe_lease_key(kid))
    logger.error("AA account disabled: id=%s next_probe_at=%s",
                 kid, next_probe_at)


@contextmanager
def anna_content_lock(md5: str):
    """Serialize publication of one cached source file across all workers."""
    token = uuid.uuid4().hex
    lease_key = f"{CONTENT_LEASE_PREFIX}{md5.lower().strip()}"
    deadline = time.monotonic() + config.CONTENT_LOCK_WAIT_SECONDS
    client = None
    acquired = False
    heartbeat_stop = threading.Event()
    heartbeat_thread = None
    try:
        while time.monotonic() < deadline:
            try:
                client = get_redis()
                acquired = bool(client.set(
                    lease_key,
                    token,
                    nx=True,
                    ex=config.CONTENT_LOCK_LEASE_SECONDS,
                ))
            except Exception as exc:
                raise AnnaContentBusyError(
                    "Anna content lock Redis is unavailable"
                ) from exc
            if acquired:
                break
            time.sleep(0.05)
        if not acquired:
            raise AnnaContentBusyError("Anna content is already downloading")
        heartbeat_thread = threading.Thread(
            target=_renew_string_lease,
            args=(client, lease_key, token, heartbeat_stop),
            daemon=True,
        )
        heartbeat_thread.start()
        yield
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1)
        if acquired and client is not None:
            try:
                client.eval(
                    _RELEASE_STRING_LEASE,
                    1,
                    lease_key,
                    token,
                )
            except Exception:
                logger.warning("Failed to release Anna content lease")


def _renew_string_lease(client, lease_key: str, token: str,
                        stop: threading.Event) -> None:
    interval = max(min(config.CONTENT_LOCK_LEASE_SECONDS // 3, 30), 1)
    while not stop.wait(interval):
        try:
            renewed = client.eval(
                _RENEW_STRING_LEASE,
                1,
                lease_key,
                token,
                config.CONTENT_LOCK_LEASE_SECONDS,
            )
            if not renewed:
                logger.error("AA content lease disappeared before release")
                return
        except Exception as exc:
            logger.error(
                "AA content lease renewal failed: %s",
                type(exc).__name__,
            )


def _to_ts(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _days_until(ts: int | None) -> int | None:
    if not ts:
        return None
    try:
        return (datetime.fromtimestamp(ts).date() - date.today()).days
    except (ValueError, TypeError, OSError):
        return None


def _fetch_expiry_from_aa(secret_key: str) -> int | None:
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
    except Exception as exc:
        logger.warning("Failed to fetch AA membership expiry: id=%s error=%s",
                       key_id(secret_key), type(exc).__name__)
        return None

    dates: list[date] = []
    for y, m, d in _EXPIRY_RE.findall(html):
        try:
            dates.append(date(int(y), int(m), int(d)))
        except ValueError:
            continue
    if not dates:
        logger.warning("AA membership expiry not found: id=%s",
                       key_id(secret_key))
        return None
    return int(datetime.combine(min(dates), datetime.min.time()).timestamp())


def get_key_expiry(secret_key: str, force: bool = False) -> dict:
    r = get_redis()
    kid = key_id(secret_key)
    cached = _to_ts(r.hget(KEY_EXPIRY, kid))
    cached_days = _days_until(cached)
    need_fetch = (
        force
        or cached is None
        or cached_days is None
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
            source = "cache_stale" if cached else "unknown"
    return {
        "id": kid,
        "expiry": expiry,
        "days_left": _days_until(expiry),
        "source": source,
    }


def check_expiry(force: bool = False) -> list[dict]:
    secrets = _ensure_loaded()
    return [
        get_key_expiry(secrets[kid], force=force)
        for kid in sorted(secrets)
    ]
