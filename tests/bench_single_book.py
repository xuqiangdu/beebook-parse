"""
单书解析耗时 benchmark
针对一本已经在 books/ 里的书,测端到端各阶段耗时:
  POST 返回 → pending → downloading → parsing → completed

用法:
  python3 tests/bench_single_book.py <md5> <extension>

需要:服务已经跑在 localhost:$PORT (默认 5556)
"""
from __future__ import annotations
import os
import sys
import time
import json
import requests

BASE = os.getenv("BENCH_BASE", "http://localhost:5556")


def bench(md5: str, extension: str, label: str = ""):
    url = f"{BASE}/api/parse"
    tid = f"{md5}_default"

    # 先清掉 Redis 里的缓存,保证冷启动
    try:
        import redis
        r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"),
                        port=int(os.getenv("REDIS_PORT", 6379)),
                        db=int(os.getenv("REDIS_DB", 15)),
                        decode_responses=True)
        for k in r.scan_iter(f"parse:{tid}*"):
            r.delete(k)
        r.delete(f"parse:lock:{tid}")
    except Exception:
        pass

    print(f"\n{'='*60}")
    print(f"  测试: {label or md5} ({extension})")
    print(f"{'='*60}")

    # 1) POST 提交,测 HTTP 接收延迟
    t_post = time.time()
    resp = requests.post(url, json={"md5": md5, "extension": extension})
    post_ms = (time.time() - t_post) * 1000
    data = resp.json()["data"]
    print(f"① POST 返回            {post_ms:7.1f} ms   status={data['status']} "
          f"cached={data.get('cached', '?')}")

    if data.get("cached"):
        # 命中缓存,拉文本看延迟
        t_get = time.time()
        get_body = requests.get(f"{BASE}/api/parse/{tid}").json()["data"]
        get_ms = (time.time() - t_get) * 1000
        total_len = get_body.get("total_length", 0)
        print(f"② GET 拿文本            {get_ms:7.1f} ms   字符={total_len}")
        print(f"\n总耗时: {post_ms + get_ms:7.1f} ms  (命中缓存)")
        return

    # 2) 轮询,记录每次状态变化的时间戳
    t_start = time.time()
    last_status = data["status"]
    state_transitions = {last_status: 0}
    final_body = None
    interval = 0.05  # 50ms 轮询,尽量精确捕捉状态变化

    while True:
        elapsed = time.time() - t_start
        if elapsed > 60:
            print(f"超时(60s)!最后状态 {last_status}")
            return
        body = requests.get(f"{BASE}/api/parse/{tid}").json()
        data = body.get("data") or {}
        status = data.get("status")

        if status != last_status:
            state_transitions[status] = elapsed
            last_status = status

        if status == "completed":
            final_body = data
            break
        if status == "failed":
            print(f"❌ 解析失败: {body.get('message')} code={body.get('code')}")
            return
        time.sleep(interval)

    # 3) 输出各阶段
    stages = [("pending", "①→pending"),
              ("downloading", "②→downloading"),
              ("parsing", "③→parsing"),
              ("completed", "④→completed")]
    prev_ms = 0.0
    for key, label in stages:
        if key in state_transitions:
            ts_ms = state_transitions[key] * 1000
            delta = ts_ms - prev_ms
            print(f"{label:<20} 累计 {ts_ms:7.1f} ms  (本阶段 {delta:6.1f} ms)")
            prev_ms = ts_ms

    total_ms = (time.time() - t_start) * 1000
    total_length = final_body.get("total_length", 0) if final_body else 0
    file_size = final_body.get("file_size", 0) if final_body else 0
    parse_time_ms = final_body.get("parse_time_ms", 0) if final_body else 0

    print(f"\n📊 总结:")
    print(f"   文件大小:      {file_size / 1024 / 1024:7.2f} MB")
    print(f"   解析出字符数:   {total_length:,}")
    print(f"   纯解析耗时:    {parse_time_ms:7.1f} ms  (服务端记录)")
    print(f"   端到端墙钟:    {total_ms:7.1f} ms  (客户端视角)")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: bench_single_book.py <md5> <extension> [label]")
        sys.exit(1)
    md5 = sys.argv[1]
    ext = sys.argv[2]
    label = sys.argv[3] if len(sys.argv) > 3 else ""
    bench(md5, ext, label)
