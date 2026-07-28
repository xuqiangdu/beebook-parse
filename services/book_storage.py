from __future__ import annotations
"""
书籍文件获取服务

查找优先级：
  1. 本地缓存（books/ 目录）
  2. Anna's Archive Fast Download API（需要 VIP key）
  3. OSS 远程下载
"""

import os
import json
import logging
import urllib.request
import urllib.error
import uuid
from collections.abc import Callable

import config
from services.aa_key_pool import (
    AnnaDownloadBusyError,
    anna_content_lock,
    anna_download_slot,
    available_keys,
    mark_disabled,
    mark_quota_exhausted,
    mark_success,
    mark_transient_error,
    seed_keys_from_env,
    unavailable_info,
)

logger = logging.getLogger(__name__)

# Anna's Archive 下载 API
AA_BASE_URL = os.getenv("AA_BASE_URL", "https://zh.annas-archive.gl")

# OSS（备用）
OSS_BASE_URL = os.getenv("OSS_BASE_URL", "")

# Reject obvious placeholder/error bodies before they become durable book cache.
MIN_BOOK_FILE_BYTES = max(int(os.getenv("MIN_BOOK_FILE_BYTES", "32")), 1)


class AAVipExpiredError(Exception):
    """AA 账号 VIP 过期或未开通，需上游报警并提示用户续费"""

    def __init__(self, message: str, next_probe_at: int = 0,
                 retry_after_seconds: int = 0):
        super().__init__(message)
        self.next_probe_at = next_probe_at
        self.retry_after_seconds = retry_after_seconds


class AADownloadQuotaExceededError(Exception):
    """AA 账号当日下载额度用尽，需上游报警并提示用户次日重试"""

    def __init__(self, message: str, next_probe_at: int = 0,
                 retry_after_seconds: int = 0):
        super().__init__(message)
        self.next_probe_at = next_probe_at
        self.retry_after_seconds = retry_after_seconds


class AAUpstreamRateLimitedError(Exception):
    """AA returned a generic rate limit unrelated to account download quota."""

    def __init__(self, message: str, retry_after_seconds: int = 5):
        super().__init__(message)
        self.retry_after_seconds = max(int(retry_after_seconds or 5), 1)


def _redact_secret(value: str, secret_key: str) -> str:
    return str(value or "").replace(secret_key, "[redacted]")


def find_book_file(
    md5: str,
    extension: str = "",
    publish_guard: Callable[[], bool] | None = None,
) -> tuple[str | None, str | None]:
    """根据 md5 查找/下载书籍文件

    Returns:
        (filepath, error_reason)
          成功 → (filepath, None)
          失败 → (None, 失败原因)  失败原因按来源汇总,供上层 errorMsg 透出
    """
    md5 = md5.lower().strip()
    books_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "books")
    os.makedirs(books_dir, exist_ok=True)

    # 1. 本地缓存
    path = _find_local(books_dir, md5)
    if path:
        return path, None

    reasons: list[str] = []
    try:
        with anna_content_lock(md5):
            # Another worker may have published the same content while this
            # request waited for the per-MD5 lock.
            path = _find_local(books_dir, md5)
            if path:
                return path, None

            with anna_download_slot():
                # Select accounts only after a global download slot is held;
                # otherwise a queued worker can retain a stale account state.
                aa_keys = available_keys()
                if not aa_keys:
                    seed_keys_from_env()
                    aa_keys = available_keys()
                if not aa_keys:
                    info = unavailable_info()
                    if info["reason"] == "quota":
                        raise AADownloadQuotaExceededError(
                            "AA 账号当日下载额度用尽",
                            next_probe_at=info["next_probe_at"],
                            retry_after_seconds=info[
                                "retry_after_seconds"
                            ],
                        )
                    raise AAVipExpiredError(
                        "AA 账号未配置、VIP 过期或无可用 key",
                        next_probe_at=info["next_probe_at"],
                        retry_after_seconds=info[
                            "retry_after_seconds"
                        ],
                    )
                path, err = _download_from_aa_pool(
                    books_dir,
                    md5,
                    extension,
                    aa_keys,
                    publish_guard=publish_guard,
                )
            if path:
                return path, None
            if err:
                reasons.append(f"AA: {err}")

            # 3. OSS
            if OSS_BASE_URL:
                path, err = _download_from_oss(
                    books_dir,
                    md5,
                    extension,
                    publish_guard=publish_guard,
                )
                if path:
                    return path, None
                if err:
                    reasons.append(f"OSS: {err}")
            else:
                reasons.append("OSS: 未配置 OSS_BASE_URL")
    except AnnaDownloadBusyError as exc:
        raise AAUpstreamRateLimitedError(
            str(exc),
            retry_after_seconds=5,
        ) from exc

    reason = "; ".join(reasons) if reasons else "未知原因"
    logger.warning(f"找不到书籍文件: md5={md5} ext={extension} | {reason}")
    return None, reason


def _find_local(directory: str, md5: str) -> str | None:
    if not os.path.isdir(directory):
        return None
    for f in os.listdir(directory):
        fname = f.lower()
        if ".part." in fname:
            continue
        if fname == md5 or fname.startswith(md5 + "."):
            path = os.path.join(directory, f)
            if (
                os.path.isfile(path)
                and os.path.getsize(path) >= MIN_BOOK_FILE_BYTES
            ):
                return path
    return None


def _download_from_aa_pool(books_dir: str, md5: str, extension: str,
                           aa_keys: list[tuple[str, str]],
                           publish_guard: Callable[[], bool] | None = None,
                           ) -> tuple[str | None, str | None]:
    quota_errors = 0
    disabled_errors = 0
    transient_errors = 0
    rate_limit_errors: list[AAUpstreamRateLimitedError] = []
    reasons: list[str] = []

    for kid, secret_key in aa_keys:
        try:
            if publish_guard is None:
                path, err = _download_from_aa(
                    books_dir,
                    md5,
                    extension,
                    secret_key,
                    kid,
                )
            else:
                path, err = _download_from_aa(
                    books_dir,
                    md5,
                    extension,
                    secret_key,
                    kid,
                    publish_guard=publish_guard,
                )
        except AADownloadQuotaExceededError as e:
            quota_errors += 1
            reason = str(e)
            mark_quota_exhausted(secret_key, reason)
            reasons.append(f"key={kid} quota")
            continue
        except AAVipExpiredError as e:
            disabled_errors += 1
            reason = str(e)
            mark_disabled(secret_key, reason)
            reasons.append(f"key={kid} disabled")
            continue
        except AAUpstreamRateLimitedError as e:
            rate_limit_errors.append(e)
            mark_transient_error(secret_key, str(e))
            reasons.append(f"key={kid} rate-limited")
            continue

        if path:
            mark_success(secret_key)
            return path, None
        if err:
            transient_errors += 1
            mark_transient_error(secret_key, err)
            reasons.append(f"key={kid} {err}")

    # A transient account remains recoverable independently of quota reset.
    # Preserve it as a generic upstream error instead of misreporting 10001/10002.
    if rate_limit_errors:
        raise AAUpstreamRateLimitedError(
            "AA upstream rate limited",
            retry_after_seconds=max(
                error.retry_after_seconds for error in rate_limit_errors
            ),
        )
    if transient_errors:
        return None, "; ".join(reasons)

    info = unavailable_info()
    # quota + disabled is recoverable through the quota account, so it is 10002.
    if quota_errors or info["reason"] == "quota":
        raise AADownloadQuotaExceededError(
            "AA 账号当日下载额度用尽",
            next_probe_at=info["next_probe_at"],
            retry_after_seconds=info["retry_after_seconds"],
        )
    if disabled_errors or info["reason"] == "disabled":
        raise AAVipExpiredError(
            "AA 账号 VIP 过期、未开通或 key 异常",
            next_probe_at=info["next_probe_at"],
            retry_after_seconds=info["retry_after_seconds"],
        )

    return None, "; ".join(reasons) if reasons else "无可用下载链接"


def _download_from_aa(books_dir: str, md5: str, extension: str,
                      secret_key: str, key_label: str = "",
                      publish_guard: Callable[[], bool] | None = None,
                      ) -> tuple[str | None, str | None]:
    """通过 Anna's Archive Fast Download API 下载

    Returns:
        (filepath, error_reason)
          成功 → (filepath, None)
          失败 → (None, 原因字符串)  —— 用于 errorMsg 透传到上游

    Raises:
        AAVipExpiredError:           AA 返回 403 "Not a member"
        AADownloadQuotaExceededError: AA 返回 429 "No downloads left"
    """
    api_url = f"{AA_BASE_URL}/dyn/api/fast_download.json?md5={md5}&key={secret_key}"
    req = urllib.request.Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # 只识别 AA 明确约定的两种业务错误，其余落通用分支
        try:
            err_body = json.loads(e.read() or b"{}")
        except Exception:
            err_body = {}
        err_text = err_body.get("error", "")
        if e.code in (401, 403):
            if err_text == "Not a member":
                raise AAVipExpiredError("AA 账号 VIP 过期或未开通") from e
            reason = _redact_secret(
                f"API HTTP {e.code} {err_text or e.reason}",
                secret_key,
            )
            logger.warning(
                "AA API non-membership authorization error: "
                "status=%s key=%s",
                e.code,
                key_label,
            )
            return None, reason
        if e.code == 429 and err_text == "No downloads left":
            raise AADownloadQuotaExceededError("AA 账号当日下载额度用尽") from e
        if e.code == 429:
            retry_after = 5
            try:
                retry_after = int(e.headers.get("Retry-After") or 5)
            except (TypeError, ValueError):
                pass
            raise AAUpstreamRateLimitedError(
                "AA upstream rate limited",
                retry_after_seconds=retry_after,
            ) from e
        reason = _redact_secret(
            f"API HTTP {e.code} {err_text or e.reason}",
            secret_key,
        )
        logger.warning("AA API HTTP error: status=%s key=%s",
                       e.code, key_label)
        return None, reason
    except urllib.error.URLError as e:
        reason = _redact_secret(f"API 网络错误: {e.reason}", secret_key)
        logger.warning("AA API network failure: md5=%s key=%s",
                       md5, key_label)
        return None, reason
    except Exception as e:
        reason = _redact_secret(
            f"API 异常: {type(e).__name__}: {e}",
            secret_key,
        )
        logger.warning("AA API failure: md5=%s key=%s error=%s",
                       md5, key_label, type(e).__name__)
        return None, reason

    download_url = data.get("download_url")
    if not download_url:
        reason = _redact_secret(
            f"API 未返回 download_url(上游: {data.get('error', '未知')})",
            secret_key,
        )
        logger.warning("AA API returned no download URL: key=%s", key_label)
        return None, reason

    # 下载文件
    ext = extension or "pdf"
    local_path = os.path.join(books_dir, f"{md5}.{ext}")
    part_path = f"{local_path}.part.{uuid.uuid4().hex}"
    try:
        req2 = urllib.request.Request(download_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req2, timeout=300) as resp2:
            expected_length = _content_length(resp2)
            written = 0
            with open(part_path, "xb") as f:
                while True:
                    chunk = resp2.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    written += len(chunk)
                f.flush()
                os.fsync(f.fileno())
        if written < MIN_BOOK_FILE_BYTES:
            return None, (
                f"下载内容过小: minimum={MIN_BOOK_FILE_BYTES} "
                f"actual={written}"
            )
        if expected_length is not None and written != expected_length:
            return None, (
                f"下载不完整: expected={expected_length} actual={written}"
            )
        if publish_guard is not None and not publish_guard():
            return None, "下载任务已过期,放弃发布缓存"
        os.replace(part_path, local_path)
        if (
            os.path.exists(local_path)
            and os.path.getsize(local_path) >= MIN_BOOK_FILE_BYTES
        ):
            logger.info(f"AA 下载成功: {md5}.{ext} ({os.path.getsize(local_path)} bytes)")
            return local_path, None
        return None, "原子发布后文件为空"
    except urllib.error.HTTPError as e:
        reason = _redact_secret(
            f"下载 HTTP {e.code} {e.reason}",
            secret_key,
        )
        logger.warning("AA file download HTTP error: md5=%s status=%s",
                       md5, e.code)
        return None, reason
    except urllib.error.URLError as e:
        reason = _redact_secret(f"下载网络错误: {e.reason}", secret_key)
        logger.warning("AA file download network error: md5=%s", md5)
        return None, reason
    except Exception as e:
        reason = _redact_secret(
            f"下载异常: {type(e).__name__}: {e}",
            secret_key,
        )
        logger.warning("AA file download failure: md5=%s error=%s",
                       md5, type(e).__name__)
        return None, reason
    finally:
        try:
            if os.path.exists(part_path):
                os.remove(part_path)
        except OSError:
            logger.warning("Failed to remove partial AA download: md5=%s",
                           md5)


def _download_from_oss(books_dir: str, md5: str,
                       extension: str,
                       publish_guard: Callable[[], bool] | None = None,
                       ) -> tuple[str | None, str | None]:
    """从 OSS 下载

    Returns:
        (filepath, error_reason)
    """
    exts = [extension] if extension else ["pdf", "epub", "fb2", "djvu", "mobi", "txt"]
    last_err = None
    for ext in exts:
        filename = f"{md5}.{ext}"
        url = f"{OSS_BASE_URL.rstrip('/')}/{filename}"
        local_path = os.path.join(books_dir, filename)
        part_path = f"{local_path}.part.{uuid.uuid4().hex}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    data = resp.read()
                    if len(data) >= MIN_BOOK_FILE_BYTES:
                        with open(part_path, "xb") as f:
                            f.write(data)
                            f.flush()
                            os.fsync(f.fileno())
                        expected_length = _content_length(resp)
                        if (
                            expected_length is not None
                            and len(data) != expected_length
                        ):
                            last_err = (
                                f"{filename} 下载不完整: "
                                f"expected={expected_length} "
                                f"actual={len(data)}"
                            )
                            continue
                        if (
                            publish_guard is not None
                            and not publish_guard()
                        ):
                            last_err = f"{filename} 任务已过期"
                            continue
                        os.replace(part_path, local_path)
                        logger.info(f"OSS 下载成功: {filename}")
                        return local_path, None
                    last_err = (
                        f"{filename} 内容过小: "
                        f"minimum={MIN_BOOK_FILE_BYTES} actual={len(data)}"
                    )
                else:
                    last_err = f"{filename} HTTP {resp.status}"
        except urllib.error.HTTPError as e:
            last_err = f"{filename} HTTP {e.code}"
        except urllib.error.URLError as e:
            last_err = f"{filename} 网络错误: {e.reason}"
        except Exception as e:
            last_err = f"{filename} 异常: {type(e).__name__}: {e}"
        finally:
            try:
                if os.path.exists(part_path):
                    os.remove(part_path)
            except OSError:
                logger.warning(
                    "Failed to remove partial OSS download: md5=%s",
                    md5,
                )
    return None, last_err or "无候选扩展名"


def _content_length(response) -> int | None:
    try:
        raw = response.headers.get("Content-Length")
        if raw is None:
            return None
        value = int(raw)
        return value if value >= 0 else None
    except (AttributeError, TypeError, ValueError):
        return None


def get_file_extension(filepath: str) -> str:
    name = os.path.basename(filepath).lower()
    if name.endswith(".fb2.zip"):
        return "fb2"
    return name.rsplit(".", 1)[-1] if "." in name else ""
