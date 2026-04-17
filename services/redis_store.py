from __future__ import annotations
"""
Redis 存储服务 - 大 Key 自动拆分

策略：
  - 文本超过 CHUNK_SIZE(512KB) 时，自动拆分为多个 chunk
  - 存储结构：
      parse:{task_id}:meta   → JSON { chunks, total_length, engine, ... }
      parse:{task_id}:chunk:0 → 文本第 0 段
      parse:{task_id}:chunk:1 → 文本第 1 段
      ...
  - 读取时按 chunk 顺序拼接还原完整文本
"""

import json
import time
import redis

import config


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        password=config.REDIS_PASSWORD,
        decode_responses=True,
    )


def _chunk_key(task_id: str, index: int) -> str:
    return f"parse:{task_id}:chunk:{index}"


def _meta_key(task_id: str) -> str:
    return f"parse:{task_id}:meta"


def store_parse_result(r: redis.Redis, task_id: str, text: str,
                       engine: str, filename: str, file_md5: str,
                       file_size: int, parse_time_ms: float,
                       fmt: str):
    """
    将解析结果存入 Redis，大文本自动拆分。
    """
    chunk_size = config.REDIS_CHUNK_SIZE
    ttl = config.REDIS_PARSE_TTL

    # 拆分文本
    chunks = []
    for i in range(0, max(len(text), 1), chunk_size):
        chunks.append(text[i:i + chunk_size])
    if not chunks:
        chunks = [""]

    # 用 pipeline 批量写入
    pipe = r.pipeline()
    for i, chunk in enumerate(chunks):
        pipe.setex(_chunk_key(task_id, i), ttl, chunk)

    # 写入 meta
    meta = {
        "status": "completed",
        "chunks": len(chunks),
        "total_length": len(text),
        "engine": engine,
        "filename": filename,
        "file_md5": file_md5,
        "file_size": file_size,
        "parse_time_ms": parse_time_ms,
        "format": fmt,
    }
    pipe.setex(_meta_key(task_id), ttl, json.dumps(meta, ensure_ascii=False))
    pipe.execute()


def store_parse_error(r: redis.Redis, task_id: str, error: str,
                      filename: str, fmt: str, code: int = 501):
    """
    存储解析失败信息
    code: 失败类型,用于客户端区分
      501 解析器失败(默认) / 502 下载失败 / 504 看门狗超时
    """
    meta = {
        "status": "failed",
        "error": error,
        "error_code": code,
        "filename": filename,
        "format": fmt,
        "chunks": 0,
        "total_length": 0,
    }
    r.setex(_meta_key(task_id), config.REDIS_PARSE_TTL,
            json.dumps(meta, ensure_ascii=False))


def store_parse_pending(r: redis.Redis, task_id: str, filename: str, fmt: str,
                        status: str = "pending",
                        deadline_ts: int | None = None,
                        file_size: int = 0):
    """
    标记任务为某个中间状态(v2 状态机)
    status: pending / downloading / parsing / processing(兼容)
    deadline_ts: 看门狗截止时间(毫秒),超过被强制 failed
    """
    meta = {
        "status": status,
        "filename": filename,
        "format": fmt,
        "chunks": 0,
        "total_length": 0,
        "started_at": int(time.time() * 1000),
    }
    if deadline_ts:
        meta["deadline_ts"] = deadline_ts
    if file_size:
        meta["file_size"] = file_size
    r.setex(_meta_key(task_id), config.REDIS_PARSE_TTL,
            json.dumps(meta, ensure_ascii=False))


def update_parse_status(r: redis.Redis, task_id: str, status: str,
                        **extra_fields):
    """
    更新任务状态(状态机流转用),保留已有字段,只覆盖 status 和传入的字段。
    比如 pending → downloading → parsing 各阶段调一次。
    """
    raw = r.get(_meta_key(task_id))
    if raw is None:
        return  # meta 已过期,放弃更新
    try:
        meta = json.loads(raw)
    except Exception:
        return
    meta["status"] = status
    meta.update(extra_fields)
    # 保留原 TTL(用 setex 重设也行,这里取剩余 TTL)
    ttl = r.ttl(_meta_key(task_id))
    if ttl is None or ttl < 0:
        ttl = config.REDIS_PARSE_TTL
    r.setex(_meta_key(task_id), ttl,
            json.dumps(meta, ensure_ascii=False))


def get_parse_meta(r: redis.Redis, task_id: str) -> dict | None:
    """获取任务元信息（用于轮询状态）"""
    raw = r.get(_meta_key(task_id))
    if raw is None:
        return None
    return json.loads(raw)


def get_parse_text(r: redis.Redis, task_id: str, meta: dict = None) -> str | None:
    """拼接所有 chunk 还原完整文本"""
    if meta is None:
        meta = get_parse_meta(r, task_id)
    if meta is None or meta.get("status") != "completed":
        return None

    chunk_count = meta["chunks"]
    if chunk_count == 0:
        return ""

    # 批量读取所有 chunk
    pipe = r.pipeline()
    for i in range(chunk_count):
        pipe.get(_chunk_key(task_id, i))
    parts = pipe.execute()

    return "".join(part or "" for part in parts)


def delete_parse_result(r: redis.Redis, task_id: str):
    """清理任务数据"""
    meta = get_parse_meta(r, task_id)
    keys = [_meta_key(task_id)]
    if meta:
        for i in range(meta.get("chunks", 0)):
            keys.append(_chunk_key(task_id, i))
    r.delete(*keys)
