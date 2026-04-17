"""
beebook-parse v2 高级场景单测

测试 curl 不好覆盖的内部行为:
  1. 启动自愈(reconcile_on_startup):僵尸 meta 一律清理(服务无状态,不再磁盘回盘)
  2. 看门狗(watchdog):中间态 meta deadline 过期被强制 failed + books/ 过期清理
  3. 内存动态保护(is_overloaded with file_size)
  4. counter 跨场景的累加/递减一致性
  5. update_parse_status 状态流转

注意:
  这些测试**不启动 HTTP 服务**,直接 import 服务层函数测。
  REDIS_DB 默认 16(避开 sh 脚本的 15),避免污染。

跑法:
  pytest tests/test_advanced.py -v -s
"""
from __future__ import annotations
import os
import sys
import json
import time
import threading
import pytest

# 把项目根加到 sys.path,允许 import config / services
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─────────────── fixtures ──────────────
@pytest.fixture(scope="module")
def redis_client():
    """专门用于测试的 redis 连接,每个测试前清空 db"""
    import redis
    import config
    r = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True,
    )
    r.ping()
    return r


@pytest.fixture(autouse=True)
def clean_db(redis_client):
    """每个测试前后清空 db"""
    redis_client.flushdb()
    yield
    redis_client.flushdb()


# ─────────────── 1. 启动自愈 ──────────────
class TestReconcileOnStartup:
    """方案 A:启动自愈"""

    def test_clears_old_locks(self, redis_client):
        """场景:旧进程留下了 lock,新进程启动应清理"""
        from services.task_manager import reconcile_on_startup

        # 模拟旧进程留下的 lock
        redis_client.set("parse:lock:abc_default", "1")
        redis_client.set("parse:lock:def_default", "1")
        assert redis_client.exists("parse:lock:abc_default")

        # 启动自愈
        stats = reconcile_on_startup()

        assert stats["locks_cleared"] >= 2
        assert not redis_client.exists("parse:lock:abc_default")
        assert not redis_client.exists("parse:lock:def_default")
        print(f"\n  ✓ 清理了 {stats['locks_cleared']} 个旧锁")

    def test_resets_counters(self, redis_client):
        """场景:旧进程的 counter 残留,新进程启动应重置为 0"""
        from services.task_manager import reconcile_on_startup

        # 模拟残留计数
        redis_client.set("parse:counter:queued", 5)
        redis_client.set("parse:counter:downloading", 3)
        redis_client.set("parse:counter:parsing", 7)

        stats = reconcile_on_startup()

        assert int(redis_client.get("parse:counter:queued")) == 0
        assert int(redis_client.get("parse:counter:downloading")) == 0
        assert int(redis_client.get("parse:counter:parsing")) == 0
        assert stats["counters_reset"] == 3
        print(f"\n  ✓ 重置了 3 个 counter")

    def test_drops_zombie_processing_meta(self, redis_client):
        """场景:旧进程留下中间态 meta → 服务无状态,一律丢弃(客户端重提)"""
        from services.task_manager import reconcile_on_startup

        # 模拟僵尸 meta
        zombie_meta = {
            "status": "processing",
            "filename": "ghost.pdf",
            "format": "pdf",
            "chunks": 0,
            "total_length": 0,
        }
        redis_client.set(
            "parse:abc_pymupdf:meta",
            json.dumps(zombie_meta),
        )
        # completed 状态不应被动
        good_meta = {**zombie_meta, "status": "completed", "chunks": 1, "total_length": 100}
        redis_client.set("parse:keep_default:meta", json.dumps(good_meta))

        stats = reconcile_on_startup()

        assert stats["dropped"] == 1
        assert not redis_client.exists("parse:abc_pymupdf:meta")
        assert redis_client.exists("parse:keep_default:meta")  # completed 不动
        print(f"\n  ✓ 丢弃中间态僵尸: {stats['dropped']}")

    def test_skipped_when_other_instance_holds_lock(self, redis_client):
        """场景:另一个进程已在做 reconcile,本进程应跳过(SETNX 防重入)"""
        from services.task_manager import reconcile_on_startup

        # 占住自愈锁
        redis_client.set("parse:reconcile:lock", "1", ex=60)

        stats = reconcile_on_startup()

        assert stats["skipped"] is True
        print(f"\n  ✓ 别的进程在做时正确跳过")


# ─────────────── 2. 看门狗 ──────────────
class TestWatchdog:
    """方案 C1:看门狗超时强制 failed"""

    def test_watchdog_kills_overdue_task(self, redis_client):
        """场景:任务 deadline 已过 → 应被看门狗标 failed (504)"""
        import config
        from services.task_manager import (
            start_watchdog, stop_watchdog, _watchdog_stop,
        )

        # 准备一个已经超时的 meta
        task_id = "overdue_task_default"
        past_deadline = int((time.time() - 10) * 1000)  # 10 秒前就过期了
        redis_client.set(
            f"parse:{task_id}:meta",
            json.dumps({
                "status": "parsing",
                "filename": "stuck.pdf",
                "format": "pdf",
                "chunks": 0,
                "total_length": 0,
                "deadline_ts": past_deadline,
            }),
        )
        redis_client.set("parse:counter:parsing", 1)
        redis_client.set(f"parse:lock:{task_id}", "1")

        # 启动看门狗(daemon thread,内部周期 = config.WATCHDOG_INTERVAL_SEC)
        _watchdog_stop.clear()
        start_watchdog()

        # 等够一轮 + 缓冲
        time.sleep(config.WATCHDOG_INTERVAL_SEC + 1)

        meta = json.loads(redis_client.get(f"parse:{task_id}:meta"))
        assert meta["status"] == "failed"
        assert meta["error_code"] == 504
        assert "看门狗" in meta["error"]
        # counter 应该被回收
        assert int(redis_client.get("parse:counter:parsing") or 0) == 0
        # lock 应该被释放
        assert not redis_client.exists(f"parse:lock:{task_id}")
        print(f"\n  ✓ 看门狗正确标 504,counter 回收,lock 释放")

        stop_watchdog()
        time.sleep(0.5)

    def test_watchdog_ignores_in_time_task(self, redis_client):
        """场景:任务 deadline 还没到 → 不应被动"""
        import config
        from services.task_manager import (
            start_watchdog, stop_watchdog, _watchdog_stop,
        )

        task_id = "in_time_default"
        future_deadline = int((time.time() + 60) * 1000)  # 60 秒后才过期
        meta_orig = {
            "status": "parsing",
            "filename": "alive.pdf",
            "format": "pdf",
            "chunks": 0,
            "total_length": 0,
            "deadline_ts": future_deadline,
        }
        redis_client.set(f"parse:{task_id}:meta", json.dumps(meta_orig))

        _watchdog_stop.clear()
        start_watchdog()
        time.sleep(config.WATCHDOG_INTERVAL_SEC + 1)

        meta = json.loads(redis_client.get(f"parse:{task_id}:meta"))
        assert meta["status"] == "parsing"  # 没动
        print(f"\n  ✓ 未到期任务不被误杀")

        stop_watchdog()
        time.sleep(0.5)

    def test_watchdog_handles_multiple_overdue(self, redis_client):
        """场景:多个超时任务同时存在 → 都应被处理"""
        import config
        from services.task_manager import (
            start_watchdog, stop_watchdog, _watchdog_stop,
        )

        past_ts = int((time.time() - 5) * 1000)
        for i in range(3):
            tid = f"overdue{i}_default"
            redis_client.set(
                f"parse:{tid}:meta",
                json.dumps({
                    "status": "downloading",
                    "filename": f"f{i}.pdf",
                    "format": "pdf",
                    "chunks": 0,
                    "total_length": 0,
                    "deadline_ts": past_ts,
                }),
            )

        _watchdog_stop.clear()
        start_watchdog()
        time.sleep(config.WATCHDOG_INTERVAL_SEC + 1)

        for i in range(3):
            tid = f"overdue{i}_default"
            meta = json.loads(redis_client.get(f"parse:{tid}:meta"))
            assert meta["status"] == "failed"
            assert meta["error_code"] == 504
        print(f"\n  ✓ 同时处理 3 个超时任务")

        stop_watchdog()
        time.sleep(0.5)


# ─────────────── 2b. books/ 过期清理 ──────────────
class TestBooksCacheCleanup:
    """看门狗顺手清理 books/ 过期原书,保证服务无状态"""

    def test_cleanup_removes_expired_files(self, tmp_path):
        """场景:books/ 下有过期文件 → 被删除;未过期文件 → 保留"""
        import config
        from services.task_manager import _cleanup_expired_books

        orig_dir = config.BOOK_STORAGE_DIR
        orig_ttl = config.BOOK_CACHE_TTL_SEC
        config.BOOK_STORAGE_DIR = str(tmp_path)
        config.BOOK_CACHE_TTL_SEC = 60  # 1 分钟 TTL 方便测试
        try:
            stale = tmp_path / "old.pdf"
            fresh = tmp_path / "new.pdf"
            stale.write_bytes(b"stale content")
            fresh.write_bytes(b"fresh content")
            # 把 stale 的 mtime 改成 2 小时前
            two_hours_ago = time.time() - 7200
            os.utime(stale, (two_hours_ago, two_hours_ago))

            removed = _cleanup_expired_books()

            assert removed == 1
            assert not stale.exists()
            assert fresh.exists()
            print(f"\n  ✓ 清理 {removed} 个过期文件,保留未过期")
        finally:
            config.BOOK_STORAGE_DIR = orig_dir
            config.BOOK_CACHE_TTL_SEC = orig_ttl

    def test_cleanup_handles_missing_dir(self, tmp_path):
        """场景:books/ 目录不存在 → 返回 0,不抛异常"""
        import config
        from services.task_manager import _cleanup_expired_books

        orig_dir = config.BOOK_STORAGE_DIR
        config.BOOK_STORAGE_DIR = str(tmp_path / "nonexistent")
        try:
            assert _cleanup_expired_books() == 0
            print(f"\n  ✓ 目录不存在时安全返回")
        finally:
            config.BOOK_STORAGE_DIR = orig_dir


# ─────────────── 3. 内存保护 ──────────────
class TestMemoryProtection:
    """is_overloaded 的差异化判断"""

    def test_queue_full_blocks_all(self, redis_client):
        """队列满,所有大小都拒"""
        import config
        from services.task_manager import is_overloaded

        redis_client.set("parse:counter:queued", config.MAX_QUEUE_DEPTH)

        overloaded, reason = is_overloaded(file_size=1024)
        assert overloaded
        assert "队列已满" in reason
        print(f"\n  ✓ 队列满拒绝小文件: {reason}")

        overloaded, reason = is_overloaded(file_size=100*1024*1024)
        assert overloaded
        print(f"  ✓ 队列满拒绝大文件: {reason}")

    def test_normal_state_passes(self, redis_client):
        """空闲状态,不该拒绝"""
        from services.task_manager import is_overloaded

        redis_client.set("parse:counter:queued", 0)
        overloaded, reason = is_overloaded(file_size=1024)
        assert not overloaded
        print(f"\n  ✓ 空闲状态正常放行")


# ─────────────── 4. 状态机流转 ──────────────
class TestStateTransition:
    """update_parse_status 的字段覆盖与保留"""

    def test_status_update_preserves_other_fields(self, redis_client):
        """更新 status 不应丢其他字段"""
        from services.redis_store import update_parse_status

        redis_client.setex(
            "parse:tx_default:meta", 60,
            json.dumps({
                "status": "pending",
                "filename": "x.pdf",
                "format": "pdf",
                "deadline_ts": 1234567890,
                "chunks": 0,
                "total_length": 0,
            })
        )

        update_parse_status(redis_client, "tx_default", "downloading")

        meta = json.loads(redis_client.get("parse:tx_default:meta"))
        assert meta["status"] == "downloading"
        assert meta["filename"] == "x.pdf"
        assert meta["deadline_ts"] == 1234567890  # 保留
        print(f"\n  ✓ 状态更新不丢字段")

    def test_status_update_with_extra_fields(self, redis_client):
        """更新 status 同时可加新字段(如 file_size)"""
        from services.redis_store import update_parse_status

        redis_client.setex(
            "parse:ty_default:meta", 60,
            json.dumps({
                "status": "downloading",
                "filename": "y.pdf",
                "format": "pdf",
                "chunks": 0,
                "total_length": 0,
            })
        )

        update_parse_status(redis_client, "ty_default", "parsing",
                            file_size=12345678)

        meta = json.loads(redis_client.get("parse:ty_default:meta"))
        assert meta["status"] == "parsing"
        assert meta["file_size"] == 12345678
        print(f"\n  ✓ 状态更新可附带新字段")


# ─────────────── 5. counter 一致性 ──────────────
class TestCounterConsistency:
    """背压计数器不应出现负数 / 不应泄漏"""

    def test_decr_below_zero_clamps_to_zero(self, redis_client):
        """递减到负数应钳制为 0(防御性)"""
        from services.task_manager import _decr_counter

        redis_client.set("parse:counter:queued", 0)
        val = _decr_counter(redis_client, "queued")
        assert val == 0
        # Redis 里也应该是 0,不是 -1
        assert int(redis_client.get("parse:counter:queued")) == 0
        print(f"\n  ✓ counter 不会出现负数")

    def test_get_pressure_returns_full_snapshot(self, redis_client):
        """get_pressure 字段齐全"""
        from services.task_manager import get_pressure

        redis_client.set("parse:counter:queued", 3)
        redis_client.set("parse:counter:parsing", 2)

        p = get_pressure()
        assert p["queued"] == 3
        assert p["parsing"] == 2
        assert "small_capacity" in p
        assert "large_capacity" in p
        assert "max_queue_depth" in p
        print(f"\n  ✓ pressure 字段: {list(p.keys())}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
