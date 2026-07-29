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

_FENCED_SET_META = """
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local current = cjson.decode(raw)
if current['attempt_id'] ~= ARGV[1] then
  return 0
end
if current['status'] == 'failed' or current['status'] == 'completed' then
  return 0
end
redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), ARGV[3])
return 1
"""


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        password=config.REDIS_PASSWORD,
        decode_responses=True,
    )


def _chunk_key(task_id: str, index: int, attempt_id: str | None = None) -> str:
    if attempt_id:
        return f"parse:{task_id}:attempt:{attempt_id}:chunk:{index}"
    return f"parse:{task_id}:chunk:{index}"


def _meta_key(task_id: str) -> str:
    return f"parse:{task_id}:meta"


def _set_meta(r: redis.Redis, task_id: str, meta: dict, ttl: int,
              attempt_id: str | None = None) -> bool:
    raw = json.dumps(meta, ensure_ascii=False)
    if not attempt_id:
        r.setex(_meta_key(task_id), ttl, raw)
        return True
    return bool(r.eval(
        _FENCED_SET_META,
        1,
        _meta_key(task_id),
        attempt_id,
        ttl,
        raw,
    ))


def store_parse_result(r: redis.Redis, task_id: str, text: str,
                       engine: str, filename: str, file_md5: str,
                       file_size: int, parse_time_ms: float,
                       fmt: str, attempt_id: str | None = None) -> bool:
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
        pipe.setex(_chunk_key(task_id, i, attempt_id), ttl, chunk)
    pipe.execute()

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
    if attempt_id:
        meta["attempt_id"] = attempt_id
        meta["chunk_attempt_id"] = attempt_id
    stored = _set_meta(r, task_id, meta, ttl, attempt_id=attempt_id)
    if not stored:
        stale_keys = [
            _chunk_key(task_id, i, attempt_id)
            for i in range(len(chunks))
        ]
        if stale_keys:
            r.delete(*stale_keys)
    return stored


def store_parse_error(r: redis.Redis, task_id: str, error: str,
                      filename: str, fmt: str, code: int = 501,
                      attempt_id: str | None = None,
                      **extra_fields) -> bool:
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
    if attempt_id:
        meta["attempt_id"] = attempt_id
    meta.update(extra_fields)
    return _set_meta(
        r,
        task_id,
        meta,
        config.REDIS_PARSE_TTL,
        attempt_id=attempt_id,
    )


def store_parse_pending(r: redis.Redis, task_id: str, filename: str, fmt: str,
                        status: str = "pending",
                        deadline_ts: int | None = None,
                        file_size: int = 0,
                        attempt_id: str | None = None):
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
    if attempt_id:
        meta["attempt_id"] = attempt_id
    r.setex(_meta_key(task_id), config.REDIS_PARSE_TTL,
            json.dumps(meta, ensure_ascii=False))


def update_parse_status(r: redis.Redis, task_id: str, status: str,
                        attempt_id: str | None = None,
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
        return False
    if attempt_id and meta.get("attempt_id") != attempt_id:
        return False
    meta["status"] = status
    meta.update(extra_fields)
    # 保留原 TTL(用 setex 重设也行,这里取剩余 TTL)
    ttl = r.ttl(_meta_key(task_id))
    if ttl is None or ttl < 0:
        ttl = config.REDIS_PARSE_TTL
    return _set_meta(r, task_id, meta, ttl, attempt_id=attempt_id)


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
    chunk_attempt_id = meta.get("chunk_attempt_id")
    for i in range(chunk_count):
        pipe.get(_chunk_key(task_id, i, chunk_attempt_id))
    parts = pipe.execute()

    return "".join(part or "" for part in parts)


def delete_parse_result(r: redis.Redis, task_id: str):
    """清理任务数据"""
    meta = get_parse_meta(r, task_id)
    keys = [_meta_key(task_id)]
    if meta:
        chunk_attempt_id = meta.get("chunk_attempt_id")
        for i in range(meta.get("chunks", 0)):
            keys.append(_chunk_key(task_id, i, chunk_attempt_id))
    r.delete(*keys)
