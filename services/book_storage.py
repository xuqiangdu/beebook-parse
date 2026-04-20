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

logger = logging.getLogger(__name__)

# Anna's Archive 下载 API
AA_BASE_URL = os.getenv("AA_BASE_URL", "https://zh.annas-archive.gl")
AA_SECRET_KEY = os.getenv("AA_SECRET_KEY", "")

# OSS（备用）
OSS_BASE_URL = os.getenv("OSS_BASE_URL", "")


class AAVipExpiredError(Exception):
    """AA 账号 VIP 过期或未开通，需上游报警并提示用户续费"""


class AADownloadQuotaExceededError(Exception):
    """AA 账号当日下载额度用尽，需上游报警并提示用户次日重试"""


def find_book_file(md5: str, extension: str = "") -> str | None:
    """根据 md5 查找/下载书籍文件"""
    md5 = md5.lower().strip()
    books_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "books")
    os.makedirs(books_dir, exist_ok=True)

    # 1. 本地缓存
    path = _find_local(books_dir, md5)
    if path:
        return path

    # 2. 官方 Fast Download API
    if AA_SECRET_KEY:
        path = _download_from_aa(books_dir, md5, extension)
        if path:
            return path

    # 3. OSS
    if OSS_BASE_URL:
        path = _download_from_oss(books_dir, md5, extension)
        if path:
            return path

    logger.warning(f"找不到书籍文件: md5={md5} ext={extension}")
    return None


def _find_local(directory: str, md5: str) -> str | None:
    if not os.path.isdir(directory):
        return None
    for f in os.listdir(directory):
        fname = f.lower()
        if fname == md5 or fname.startswith(md5 + "."):
            return os.path.join(directory, f)
    return None


def _download_from_aa(books_dir: str, md5: str, extension: str) -> str | None:
    """通过 Anna's Archive Fast Download API 下载

    Raises:
        AAVipExpiredError:           AA 返回 403 "Not a member"
        AADownloadQuotaExceededError: AA 返回 429 "No downloads left"
    其它所有失败统一返回 None（调用方走通用"下载失败"分支）。
    """
    api_url = f"{AA_BASE_URL}/dyn/api/fast_download.json?md5={md5}&key={AA_SECRET_KEY}"
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
        if e.code == 403 and err_text == "Not a member":
            raise AAVipExpiredError("AA 账号 VIP 过期或未开通") from e
        if e.code == 429 and err_text == "No downloads left":
            raise AADownloadQuotaExceededError("AA 账号当日下载额度用尽") from e
        logger.warning(f"AA API HTTPError {e.code}: {err_text or e.reason}")
        return None
    except Exception as e:
        logger.warning(f"AA API 请求失败: {md5} - {e}")
        return None

    download_url = data.get("download_url")
    if not download_url:
        logger.warning(f"AA API 无下载链接: {data.get('error', '?')}")
        return None

    # 下载文件
    try:
        ext = extension or "pdf"
        local_path = os.path.join(books_dir, f"{md5}.{ext}")
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
            return local_path
    except Exception as e:
        logger.warning(f"AA 文件下载失败: {md5} - {e}")
    return None


def _download_from_oss(books_dir: str, md5: str, extension: str) -> str | None:
    """从 OSS 下载"""
    exts = [extension] if extension else ["pdf", "epub", "fb2", "djvu", "mobi", "txt"]
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
                        return local_path
        except Exception:
            pass
    return None


def get_file_extension(filepath: str) -> str:
    name = os.path.basename(filepath).lower()
    if name.endswith(".fb2.zip"):
        return "fb2"
    return name.rsplit(".", 1)[-1] if "." in name else ""
