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

import config
from services.aa_key_pool import (
    available_keys,
    mark_disabled,
    mark_quota_exhausted,
    seed_keys_from_env,
    unavailable_status,
)

logger = logging.getLogger(__name__)

# Anna's Archive 下载 API
AA_BASE_URL = os.getenv("AA_BASE_URL", "https://zh.annas-archive.gl")

# OSS（备用）
OSS_BASE_URL = os.getenv("OSS_BASE_URL", "")


class AAVipExpiredError(Exception):
    """AA 账号 VIP 过期或未开通，需上游报警并提示用户续费"""


class AADownloadQuotaExceededError(Exception):
    """AA 账号当日下载额度用尽，需上游报警并提示用户次日重试"""


def find_book_file(md5: str, extension: str = "") -> tuple[str | None, str | None]:
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

    # 2. 官方 Fast Download API
    aa_keys = available_keys()
    if not aa_keys:
        seed_keys_from_env()
        aa_keys = available_keys()

    if aa_keys:
        path, err = _download_from_aa_pool(books_dir, md5, extension, aa_keys)
        if path:
            return path, None
        if err:
            reasons.append(f"AA: {err}")
    else:
        status = unavailable_status()
        if status == "quota":
            raise AADownloadQuotaExceededError("AA 账号当日下载额度用尽")
        if status == "disabled":
            raise AAVipExpiredError("AA 账号 VIP 过期、未开通或 key 异常")
        reasons.append("AA: 未配置可用 AA key")

    # 3. OSS
    if OSS_BASE_URL:
        path, err = _download_from_oss(books_dir, md5, extension)
        if path:
            return path, None
        if err:
            reasons.append(f"OSS: {err}")
    else:
        reasons.append("OSS: 未配置 OSS_BASE_URL")

    reason = "; ".join(reasons) if reasons else "未知原因"
    logger.warning(f"找不到书籍文件: md5={md5} ext={extension} | {reason}")
    return None, reason


def _find_local(directory: str, md5: str) -> str | None:
    if not os.path.isdir(directory):
        return None
    for f in os.listdir(directory):
        fname = f.lower()
        if fname == md5 or fname.startswith(md5 + "."):
            return os.path.join(directory, f)
    return None


def _download_from_aa_pool(books_dir: str, md5: str, extension: str,
                           aa_keys: list[tuple[str, str]]) -> tuple[str | None, str | None]:
    quota_errors = 0
    disabled_errors = 0
    reasons: list[str] = []

    for kid, secret_key in aa_keys:
        try:
            path, err = _download_from_aa(books_dir, md5, extension, secret_key, kid)
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

        if path:
            return path, None
        if err:
            reasons.append(f"key={kid} {err}")

    if quota_errors and quota_errors == len(aa_keys):
        raise AADownloadQuotaExceededError("AA 账号当日下载额度用尽")
    if disabled_errors and disabled_errors == len(aa_keys):
        raise AAVipExpiredError("AA 账号 VIP 过期、未开通或 key 异常")
    if quota_errors or disabled_errors:
        reason = "; ".join(reasons) if reasons else "所有 AA key 均不可用"
        raise AAVipExpiredError(f"AA 无可用 key: {reason}")

    return None, "; ".join(reasons) if reasons else "无可用下载链接"


def _download_from_aa(books_dir: str, md5: str, extension: str,
                      secret_key: str, key_label: str = "") -> tuple[str | None, str | None]:
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
            raise AAVipExpiredError(f"AA key 异常或无权限: {err_text or e.reason}") from e
        if e.code == 429 and err_text == "No downloads left":
            raise AADownloadQuotaExceededError("AA 账号当日下载额度用尽") from e
        reason = f"API HTTP {e.code} {err_text or e.reason}"
        logger.warning(f"AA API HTTPError {e.code}: key={key_label} {err_text or e.reason}")
        return None, reason
    except urllib.error.URLError as e:
        reason = f"API 网络错误: {e.reason}"
        logger.warning(f"AA API 请求失败: {md5} - {e}")
        return None, reason
    except Exception as e:
        reason = f"API 异常: {type(e).__name__}: {e}"
        logger.warning(f"AA API 请求失败: {md5} - {e}")
        return None, reason

    download_url = data.get("download_url")
    if not download_url:
        reason = f"API 未返回 download_url(上游: {data.get('error', '未知')})"
        logger.warning(f"AA API 无下载链接: {data.get('error', '?')}")
        return None, reason

    # 下载文件
    ext = extension or "pdf"
    local_path = os.path.join(books_dir, f"{md5}.{ext}")
    try:
        req2 = urllib.request.Request(download_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req2, timeout=300) as resp2:
            with open(local_path, "wb") as f:
                while True:
                    chunk = resp2.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            logger.info(f"AA 下载成功: {md5}.{ext} ({os.path.getsize(local_path)} bytes)")
            return local_path, None
        return None, "下载完成但文件为空"
    except urllib.error.HTTPError as e:
        reason = f"下载 HTTP {e.code} {e.reason}"
        logger.warning(f"AA 文件下载失败: {md5} - {e}")
        return None, reason
    except urllib.error.URLError as e:
        reason = f"下载网络错误: {e.reason}"
        logger.warning(f"AA 文件下载失败: {md5} - {e}")
        return None, reason
    except Exception as e:
        reason = f"下载异常: {type(e).__name__}: {e}"
        logger.warning(f"AA 文件下载失败: {md5} - {e}")
        return None, reason


def _download_from_oss(books_dir: str, md5: str,
                       extension: str) -> tuple[str | None, str | None]:
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
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    data = resp.read()
                    if len(data) > 0:
                        with open(local_path, "wb") as f:
                            f.write(data)
                        logger.info(f"OSS 下载成功: {filename}")
                        return local_path, None
                    last_err = f"{filename} 返回空内容"
                else:
                    last_err = f"{filename} HTTP {resp.status}"
        except urllib.error.HTTPError as e:
            last_err = f"{filename} HTTP {e.code}"
        except urllib.error.URLError as e:
            last_err = f"{filename} 网络错误: {e.reason}"
        except Exception as e:
            last_err = f"{filename} 异常: {type(e).__name__}: {e}"
    return None, last_err or "无候选扩展名"


def get_file_extension(filepath: str) -> str:
    name = os.path.basename(filepath).lower()
    if name.endswith(".fb2.zip"):
        return "fb2"
    return name.rsplit(".", 1)[-1] if "." in name else ""
