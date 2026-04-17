from __future__ import annotations
"""
异步解析任务管理器(v2)

【设计目标】
  1. 不同书并行解析,互不阻塞
  2. 同一本书(md5+engine)并发提交时只跑一次,其他合流到同一 task
  3. 本服务是"无状态中间解析能力":解析结果只存 Redis(默认 3h TTL),不落盘
     原书(books/)保留 BOOK_CACHE_TTL_SEC(默认 3h),过期由看门狗清理
     上传文件(uploads/)解析完立即删除
     → 3h 窗口用于上游失败后的快速重试;窗口外进程无残留,服务完全无状态
  4. ⭐v2 真异步:POST 只登记,下载和解析都进 worker,接口立刻返回 task_id
  5. ⭐v2 大小文件分池:大文件不挤占小文件槽位
  6. ⭐v2 背压保护:队列过深返 429,内存吃紧拒大文件
  7. ⭐v2 看门狗超时:单任务超 5 分钟强制 failed (504);同时清理过期 books/ 原书缓存
  8. ⭐v2 启动自愈:进程重启时清僵尸(中间态一律 drop,客户端重提即可)

【状态机】
  None → pending → downloading → parsing → completed / failed

【线程池】
  - _download_executor      下载池 (DOWNLOAD_CONCURRENCY,默认 6)
  - _parse_small_executor   小文件解析池 (PARSE_SMALL_CONCURRENCY,默认 16,≤20MB)
  - _parse_large_executor   大文件解析池 (PARSE_LARGE_CONCURRENCY,默认 6,>20MB)

【背压计数器】
  Redis 全局 counter(跨进程/容器统一):
    parse:counter:queued      已入队还没开始
    parse:counter:downloading 下载中
    parse:counter:parsing     解析中
"""

import os
import time
import json
import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from parsers.factory import ParserFactory
from parsers.text_normalize import normalize as _normalize_text
from services.redis_store import (
    get_redis,
    store_parse_result,
    store_parse_error,
    store_parse_pending,
    update_parse_status,
    get_parse_meta,
    get_parse_text,
)
from services.book_storage import find_book_file, get_file_extension
import config

logger = logging.getLogger(__name__)

# psutil 是可选依赖,没装就跳过内存保护
try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False
    logger.warning("psutil 未安装,内存动态保护跳过(只靠队列阈值兜底)")

_factory = ParserFactory()

# ─────────────── 三个独立线程池 ───────────────
_download_executor = ThreadPoolExecutor(
    max_workers=config.DOWNLOAD_CONCURRENCY,
    thread_name_prefix="dl",
)
_parse_small_executor = ThreadPoolExecutor(
    max_workers=config.PARSE_SMALL_CONCURRENCY,
    thread_name_prefix="parse-s",
)
_parse_large_executor = ThreadPoolExecutor(
    max_workers=config.PARSE_LARGE_CONCURRENCY,
    thread_name_prefix="parse-l",
)

# ─────────────── 背压计数器(Redis,跨进程一致)───────────────
_COUNTER_KEYS = {
    "queued":      "parse:counter:queued",
    "downloading": "parse:counter:downloading",
    "parsing":     "parse:counter:parsing",
}


def _incr_counter(r, name: str) -> int:
    return int(r.incr(_COUNTER_KEYS[name]))


def _decr_counter(r, name: str) -> int:
    val = int(r.decr(_COUNTER_KEYS[name]))
    if val < 0:
        # 防御:不应该出现负数,出现就归零
        r.set(_COUNTER_KEYS[name], 0)
        return 0
    return val


def get_pressure() -> dict:
    """获取当前压力状态(供 /api/pressure 接口使用)"""
    r = get_redis()
    try:
        return {
            "queued":      int(r.get(_COUNTER_KEYS["queued"]) or 0),
            "downloading": int(r.get(_COUNTER_KEYS["downloading"]) or 0),
            "parsing":     int(r.get(_COUNTER_KEYS["parsing"]) or 0),
            "small_capacity":   config.PARSE_SMALL_CONCURRENCY,
            "large_capacity":   config.PARSE_LARGE_CONCURRENCY,
            "download_capacity": config.DOWNLOAD_CONCURRENCY,
            "max_queue_depth":  config.MAX_QUEUE_DEPTH,
            "mem_percent":      _system_mem_pressure(),
        }
    except Exception as e:
        logger.warning(f"获取压力状态失败: {e}")
        return {"error": str(e)}


# ─────────────── 内存动态保护 ───────────────
def _system_mem_pressure() -> float:
    """返回 0.0 ~ 1.0 的内存使用率"""
    if not _HAS_PSUTIL:
        return 0.0
    try:
        return psutil.virtual_memory().percent / 100.0
    except Exception:
        return 0.0


def is_overloaded(file_size: int = 0) -> tuple[bool, str]:
    """
    检查是否过载,返回 (是否过载, 原因)。
    file_size > 0 时按文件大小做差异化判断:大文件门槛更严。
    """
    r = get_redis()
    try:
        queued = int(r.get(_COUNTER_KEYS["queued"]) or 0)
    except Exception:
        # Redis 挂了视为不过载,让请求往后走由 submit 阶段处理
        return False, ""

    # 1) 队列深度兜底
    if queued >= config.MAX_QUEUE_DEPTH:
        return True, f"队列已满({queued}/{config.MAX_QUEUE_DEPTH})"

    # 2) 内存压力(动态)
    mem = _system_mem_pressure()
    if mem >= config.MEM_CRITICAL_WATERMARK:
        return True, f"内存严重不足({mem:.0%})"

    is_large = file_size > config.LARGE_FILE_THRESHOLD
    if is_large and mem >= config.MEM_HIGH_WATERMARK:
        return True, f"内存吃紧({mem:.0%}),暂停接收大文件"

    return False, ""


# ─────────────── 任务 ID 与去重锁 ───────────────
def _task_id(md5: str, engine: str | None) -> str:
    return f"{md5}_{engine or 'default'}"


def _lock_key(task_id: str) -> str:
    return f"parse:lock:{task_id}"


def _try_acquire_task(r, task_id: str) -> bool:
    """SETNX 抢锁。拿到锁的线程才入队,其余合流。"""
    return bool(r.set(_lock_key(task_id), "1", nx=True, ex=config.PARSE_LOCK_TTL))


def _release_task_lock(r, task_id: str):
    try:
        r.delete(_lock_key(task_id))
    except Exception:
        pass


# ─────────────── 提交入口(v2 真异步)───────────────
def submit_parse_by_md5(md5: str, extension: str = "",
                        engine: str | None = None) -> dict:
    """
    通过 md5 提交解析任务(v2 真异步,立刻返回 task_id)
    """
    md5 = md5.lower().strip()
    task_id = _task_id(md5, engine)
    r = get_redis()

    # 1) 已 completed:命中 Redis 缓存直接返回
    existing = get_parse_meta(r, task_id)
    if existing and existing.get("status") == "completed":
        return {
            "task_id": task_id,
            "status": "completed",
            "cached": True,
            "total_length": existing.get("total_length", 0),
        }
    # 已在跑(任何中间态)合流
    if existing and existing.get("status") in ("pending", "downloading",
                                                "parsing", "processing"):
        return {
            "task_id": task_id,
            "status": existing["status"],
            "cached": False,
            "total_length": 0,
        }

    # 2) 抢锁
    if not _try_acquire_task(r, task_id):
        # 别的线程抢到了,合流
        return {"task_id": task_id, "status": "pending",
                "cached": False, "total_length": 0}

    # 3) 登记 pending,立刻入下载池(下载和解析全部异步)
    deadline_ts = int((time.time() + config.TASK_TIMEOUT_SEC) * 1000)
    filename = f"{md5}.{extension}" if extension else md5
    store_parse_pending(r, task_id, filename, extension or "",
                        status="pending", deadline_ts=deadline_ts)
    _incr_counter(r, "queued")

    _download_executor.submit(
        _do_download_and_parse,
        task_id, md5, extension, engine,
    )

    return {"task_id": task_id, "status": "pending",
            "cached": False, "total_length": 0}


def submit_parse_by_file(file_data: bytes, filename: str,
                         engine: str | None = None) -> dict:
    """通过上传文件提交解析任务(测试用)"""
    file_md5 = hashlib.md5(file_data).hexdigest()

    ext = ""
    if filename.lower().endswith(".fb2.zip"):
        ext = "fb2"
    elif "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()

    task_id = _task_id(file_md5, engine)
    r = get_redis()

    existing = get_parse_meta(r, task_id)
    if existing and existing.get("status") == "completed":
        return {
            "task_id": task_id,
            "status": "completed",
            "cached": True,
            "total_length": existing.get("total_length", 0),
        }
    if existing and existing.get("status") in ("pending", "downloading",
                                                "parsing", "processing"):
        return {
            "task_id": task_id,
            "status": existing["status"],
            "cached": False,
            "total_length": 0,
        }

    if not _try_acquire_task(r, task_id):
        return {"task_id": task_id, "status": "pending",
                "cached": False, "total_length": 0}

    # 上传文件暂存到 uploads/(仅供解析器读取,解析完立即删)
    os.makedirs(config.UPLOAD_DIR, exist_ok=True)
    upload_path = os.path.join(config.UPLOAD_DIR, f"{file_md5}.{ext}")
    with open(upload_path, "wb") as f:
        f.write(file_data)

    deadline_ts = int((time.time() + config.TASK_TIMEOUT_SEC) * 1000)
    store_parse_pending(r, task_id, filename, ext,
                        status="pending", deadline_ts=deadline_ts,
                        file_size=len(file_data))
    _incr_counter(r, "queued")

    # 上传场景跳过下载阶段,直接路由到对应解析池
    # delete_after=True → 解析完(无论成败)立即删临时文件
    _route_to_parse_pool(
        len(file_data),
        _do_parse,
        task_id, upload_path, filename, ext, file_md5, len(file_data), engine, True,
    )
    return {"task_id": task_id, "status": "pending",
            "cached": False, "total_length": 0}


# ─────────────── worker:下载阶段 ───────────────
def _do_download_and_parse(task_id: str, md5: str, extension: str,
                           engine: str | None):
    """
    下载阶段 worker:
      1. 更新 status=downloading + queued-- + downloading++
      2. find_book_file (本地有就跳过下载)
      3. 路由到对应解析池
    """
    r = get_redis()
    try:
        # 状态切换:queued → downloading
        _decr_counter(r, "queued")
        _incr_counter(r, "downloading")
        update_parse_status(r, task_id, "downloading")

        filepath = find_book_file(md5, extension)
        if filepath is None:
            _decr_counter(r, "downloading")
            store_parse_error(
                r, task_id,
                f"找不到文件: md5={md5},请确认文件已导入存储或 AA_SECRET_KEY 已配置",
                f"{md5}.{extension or '?'}",
                extension or "",
                code=502,  # 下载/上游问题
            )
            _release_task_lock(r, task_id)
            return

        if not extension:
            extension = get_file_extension(filepath)
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)

        # 状态切换:downloading → 路由到解析池
        _decr_counter(r, "downloading")
        update_parse_status(r, task_id, "parsing", file_size=file_size)

        # delete_after=False:原书由 books/ TTL 机制统一管理,不在解析后删除
        _route_to_parse_pool(
            file_size,
            _do_parse,
            task_id, filepath, filename, extension, md5, file_size, engine, False,
        )
    except Exception as e:
        logger.exception(f"下载阶段异常: task_id={task_id}")
        try:
            _decr_counter(r, "downloading")
        except Exception:
            pass
        store_parse_error(
            r, task_id, f"下载异常: {e}",
            f"{md5}.{extension or '?'}", extension or "", code=502,
        )
        _release_task_lock(r, task_id)


def _route_to_parse_pool(file_size: int, fn, *args):
    """按文件大小路由到小池或大池"""
    r = get_redis()
    _incr_counter(r, "parsing")
    if file_size > config.LARGE_FILE_THRESHOLD:
        _parse_large_executor.submit(_track_future, fn, *args)
    else:
        _parse_small_executor.submit(_track_future, fn, *args)


def _track_future(fn, *args):
    """包一层捕获未处理异常,避免 Future 静默吞错"""
    try:
        fn(*args)
    except Exception:
        logger.exception(f"线程池任务未捕获异常: fn={fn.__name__} args[0]={args[0] if args else '?'}")


# ─────────────── worker:解析阶段 ───────────────
def _do_parse(task_id: str, filepath: str, filename: str, ext: str,
              file_md5: str, file_size: int, engine: str | None = None,
              delete_after: bool = False):
    """
    线程池内执行的解析逻辑(v2:计数器 + 状态切换)

    delete_after: True 时 finally 阶段删除 filepath(用于 /api/parse/upload
      的临时文件;md5 下载场景传 False,原书由 books/ TTL 统一清理)
    """
    r = get_redis()
    try:
        handler, detected_ext = _factory.detect_and_get_handler(filepath, filename)
        if engine:
            handler = _factory.get_handler(detected_ext or ext, engine)

        if handler is None:
            store_parse_error(
                r, task_id,
                f"不支持的格式: {ext},支持: {', '.join(_factory.supported_formats().keys())}",
                filename, ext, code=501,
            )
            return

        start = time.time()
        result = handler.parse(filepath)
        elapsed_ms = round((time.time() - start) * 1000, 2)

        if not result.success:
            store_parse_error(r, task_id, result.error, filename, ext, code=501)
            return

        # 归一化:压掉多余空行 + 合并极端短行碎片(可通过 TEXT_NORMALIZE=0 关闭)
        orig_len = len(result.text)
        text = _normalize_text(result.text)
        saved = orig_len - len(text)

        # 结果只进 Redis,不落盘(本服务无状态,TTL 到期自动失效)
        store_parse_result(
            r, task_id, text, result.engine,
            filename, file_md5, file_size, elapsed_ms, ext,
        )

        logger.info(
            f"解析完成: {filename} [{ext}] {file_size}B → {len(text)}字符 "
            f"{elapsed_ms}ms (归一化省 {saved}字符)"
        )

    except Exception as e:
        logger.exception(f"解析异常: {filename}")
        store_parse_error(r, task_id, f"内部错误: {str(e)}",
                          filename, ext, code=500)
    finally:
        _decr_counter(r, "parsing")
        _release_task_lock(r, task_id)
        # 上传场景:不管成败都清理临时文件
        if delete_after:
            try:
                os.remove(filepath)
            except FileNotFoundError:
                pass
            except Exception as e:
                logger.warning(f"清理上传临时文件失败 {filepath}: {e}")


# ─────────────── 状态查询 ───────────────
def get_task_status(task_id: str) -> dict | None:
    r = get_redis()
    return get_parse_meta(r, task_id)


def get_task_text(task_id: str) -> str | None:
    r = get_redis()
    return get_parse_text(r, task_id)


# ─────────────── ⭐ 启动自愈(方案 A)───────────────
def reconcile_on_startup() -> dict:
    """
    进程启动时清理僵尸状态。多进程场景下用 SETNX 保证只一个实例做。

    清理:
      1. 所有遗留 lock(老进程的死锁)
      2. 所有 counter 重置(老进程的内存计数器丢了)
      3. 中间态 meta 一律删除(本服务无状态,解析结果不落盘,客户端重提即可)
    """
    stats = {"locks_cleared": 0, "counters_reset": 0,
             "dropped": 0, "skipped": False}

    try:
        r = get_redis()
        r.ping()
    except Exception as e:
        logger.warning(f"启动自愈跳过:Redis 不可用 ({e})")
        stats["skipped"] = True
        return stats

    # 抢分布式自愈锁,多进程下只一个跑
    if not r.set("parse:reconcile:lock", "1", nx=True, ex=60):
        logger.info("启动自愈:别的进程已在做,跳过")
        stats["skipped"] = True
        return stats

    # 1) 清旧 lock
    for key in r.scan_iter(match="parse:lock:*", count=200):
        r.delete(key)
        stats["locks_cleared"] += 1

    # 2) 重置 counter(进程级状态都已经丢失,从 0 开始算)
    for k in _COUNTER_KEYS.values():
        r.set(k, 0)
        stats["counters_reset"] += 1

    # 3) 清理中间态 meta:无法分辨是否真的跑完,直接丢弃,客户端重提
    intermediate_states = {"pending", "downloading", "parsing", "processing"}
    for meta_key in r.scan_iter(match="parse:*:meta", count=200):
        raw = r.get(meta_key)
        if not raw:
            continue
        try:
            meta = json.loads(raw)
        except Exception:
            continue
        if meta.get("status") not in intermediate_states:
            continue
        r.delete(meta_key)
        stats["dropped"] += 1

    logger.info(
        f"启动自愈完成: 清锁={stats['locks_cleared']} "
        f"重置 counter={stats['counters_reset']} "
        f"丢弃中间态={stats['dropped']}"
    )
    return stats


# ─────────────── ⭐ 看门狗(方案 C1)───────────────
_watchdog_thread = None
_watchdog_stop = threading.Event()


def _cleanup_expired_books():
    """
    清理 books/ 目录下超过 BOOK_CACHE_TTL_SEC 的原书缓存。
    按 mtime 判断过期(每次命中会被 book_storage 重新 touch 也可以,但目前不 touch,
    以入盘时间为准,足够支撑 3h 重试窗口)。
    """
    books_dir = config.BOOK_STORAGE_DIR
    if not os.path.isdir(books_dir):
        return 0
    ttl = config.BOOK_CACHE_TTL_SEC
    if ttl <= 0:
        return 0
    now = time.time()
    removed = 0
    for name in os.listdir(books_dir):
        full = os.path.join(books_dir, name)
        try:
            if not os.path.isfile(full):
                continue
            if now - os.path.getmtime(full) < ttl:
                continue
            os.remove(full)
            removed += 1
        except FileNotFoundError:
            continue
        except Exception as e:
            logger.warning(f"清理过期原书失败 {full}: {e}")
    if removed:
        logger.info(f"清理 books/ 过期原书: {removed} 个(TTL={ttl}s)")
    return removed


def _watchdog_loop():
    """
    后台 daemon 线程,每 WATCHDOG_INTERVAL_SEC 扫一次:
      1. 超时中间态任务 → 标 failed (504)
      2. books/ 过期原书 → 删除(保持服务无状态)
    """
    intermediate_states = {"pending", "downloading", "parsing", "processing"}
    logger.info(
        f"看门狗启动: 间隔 {config.WATCHDOG_INTERVAL_SEC}s, "
        f"任务超时 {config.TASK_TIMEOUT_SEC}s, "
        f"books 缓存 TTL {config.BOOK_CACHE_TTL_SEC}s"
    )

    while not _watchdog_stop.is_set():
        try:
            r = get_redis()
            now_ms = int(time.time() * 1000)
            timed_out = 0

            for meta_key in r.scan_iter(match="parse:*:meta", count=200):
                raw = r.get(meta_key)
                if not raw:
                    continue
                try:
                    meta = json.loads(raw)
                except Exception:
                    continue
                if meta.get("status") not in intermediate_states:
                    continue
                deadline = meta.get("deadline_ts")
                if not deadline or now_ms < deadline:
                    continue

                # 超时!标 failed
                parts = meta_key.split(":")
                if len(parts) < 3:
                    continue
                task_id = ":".join(parts[1:-1])

                # 根据原状态判断是 download 阶段还是 parse 阶段超时
                old_status = meta.get("status", "")
                store_parse_error(
                    r, task_id,
                    f"任务超时(>{config.TASK_TIMEOUT_SEC}s,卡在 {old_status} 阶段),"
                    "被看门狗终止",
                    meta.get("filename", ""),
                    meta.get("format", ""),
                    code=504,
                )
                # counter 回收(对应阶段)
                if old_status == "downloading":
                    _decr_counter(r, "downloading")
                elif old_status in ("parsing", "processing"):
                    _decr_counter(r, "parsing")
                elif old_status == "pending":
                    _decr_counter(r, "queued")
                # 释放锁,允许重提
                _release_task_lock(r, task_id)
                timed_out += 1
                logger.warning(
                    f"看门狗超时终止: task_id={task_id} 阶段={old_status}"
                )

            if timed_out > 0:
                logger.info(f"看门狗本轮终止 {timed_out} 个超时任务")
        except Exception:
            logger.exception("看门狗循环异常")

        # 顺手清理过期原书(失败不影响主流程)
        try:
            _cleanup_expired_books()
        except Exception:
            logger.exception("清理过期原书异常")

        _watchdog_stop.wait(config.WATCHDOG_INTERVAL_SEC)


def start_watchdog():
    """启动看门狗后台线程(只启动一次)"""
    global _watchdog_thread
    if _watchdog_thread and _watchdog_thread.is_alive():
        return
    _watchdog_thread = threading.Thread(
        target=_watchdog_loop, name="watchdog", daemon=True,
    )
    _watchdog_thread.start()


def stop_watchdog():
    """停看门狗(测试用)"""
    _watchdog_stop.set()
