"""
beebook-parse 书籍搜索+解析服务

架构:
    ┌──────────┐     ┌───────────────┐     ┌───────┐
    │  Client  │────→│  Flask API    │────→│ Redis │
    │          │     │               │     │ (缓存) │
    └──────────┘     │  search ──────────→ Anna's Archive 官网
                     │  parse ───────────→ Fast Download API
                     │    ↓               │
                     │  Parser Handler    │
                     │  (pdf/epub/fb2/..) │
                     └───────────────────┘

API:
    GET  /api/search             搜索书籍（通过官网）
    POST /api/parse              提交解析任务（通过 md5）
    GET  /api/parse/<task_id>    轮询解析结果
    GET  /api/formats            支持的格式
    GET  /health                 健康检查
"""

import os
import logging

# basicConfig 必须在导入 api.* 之前生效：
# search_service 在模块加载时就做镜像探测并打 INFO 日志，导入顺序错了日志会丢
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

from flask import Flask

from api.parse import parse_bp
from api.search import search_bp
from api.common import api_ok
from services.task_manager import reconcile_on_startup, start_watchdog
import config

app = Flask(__name__)
# 让 jsonify 直接输出 UTF-8 中文字符,而非 \uXXXX 转义(方便终端 curl 查看)
app.json.ensure_ascii = False
app.register_blueprint(parse_bp)
app.register_blueprint(search_bp)


# ⭐v2 启动自愈 + 看门狗
# 模块级执行,gunicorn 多 worker / Flask reloader 都会触发(reconcile 内部用 SETNX 防重入)
# 看门狗只在主进程启动一次(线程级幂等)
def _bootstrap():
    """启动时的一次性动作:清僵尸 + 起后台看门狗"""
    try:
        stats = reconcile_on_startup()
        if not stats.get("skipped"):
            logging.getLogger(__name__).info(
                f"启动自愈结果: {stats}"
            )
    except Exception:
        logging.getLogger(__name__).exception("启动自愈失败,但服务仍可继续")

    try:
        start_watchdog()
    except Exception:
        logging.getLogger(__name__).exception("看门狗启动失败,任务超时保护不可用")


_bootstrap()


@app.route("/health", methods=["GET"])
def health():
    redis_ok = False
    try:
        from services.redis_store import get_redis
        r = get_redis()
        r.ping()
        redis_ok = True
    except Exception:
        pass

    from parsers.factory import ParserFactory
    formats = ParserFactory().supported_formats()

    return api_ok({
        "status": "ok" if redis_ok else "degraded",
        "redis": "connected" if redis_ok else "disconnected",
        "search_source": config.AA_BASE_URL,
        "download_api": "fast_download" if config.AA_SECRET_KEY else "未配置",
        "supported_formats": formats,
    })


if __name__ == "__main__":
    os.makedirs(config.UPLOAD_DIR, exist_ok=True)
    os.makedirs(config.BOOK_STORAGE_DIR, exist_ok=True)

    print("=" * 60)
    print("  beebook-parse 书籍搜索+解析服务 (v2 真异步)")
    print("=" * 60)
    print()
    print(f"并发配置:")
    print(f"  小文件池(≤{config.LARGE_FILE_THRESHOLD_MB}MB): {config.PARSE_SMALL_CONCURRENCY}")
    print(f"  大文件池(>{config.LARGE_FILE_THRESHOLD_MB}MB): {config.PARSE_LARGE_CONCURRENCY}")
    print(f"  下载池:    {config.DOWNLOAD_CONCURRENCY}")
    print(f"  队列阈值:  {config.MAX_QUEUE_DEPTH}  (超过返 429)")
    print(f"  任务超时:  {config.TASK_TIMEOUT_SEC}s")
    print()
    print("API:")
    print(f"  GET  /api/search?q=关键词    搜索书籍")
    print(f"  POST /api/parse              解析书籍(真异步,立刻返回 task_id)")
    print(f"  GET  /api/parse/<task_id>    轮询结果")
    print(f"  GET  /api/pressure           查看当前负载 ⭐v2")
    print(f"  GET  /api/formats            支持格式")
    print(f"  GET  /health                 健康检查")
    print()
    print(f"⚠️  注意: dev server 仅供本地调试,生产请用 gunicorn:")
    print(f"   gunicorn -w 1 -k gthread --threads 8 --timeout 0 -b 0.0.0.0:{config.PORT} app:app")
    print()

    app.run(host="0.0.0.0", port=config.PORT, debug=True)
