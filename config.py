import os

# Redis（解析结果缓存）
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Redis 大 Key 拆分阈值（512KB）
REDIS_CHUNK_SIZE = int(os.getenv("REDIS_CHUNK_SIZE", 512 * 1024))
# 解析结果缓存过期时间（秒）- 默认 3 小时
# 本服务定位为"中间解析能力"、无状态：结果只存 Redis，不落盘；到期自动清理
# 3 小时窗口用于上游调用失败时的快速重试
REDIS_PARSE_TTL = int(os.getenv("REDIS_PARSE_TTL", 3 * 3600))

# ─────────────── 解析并发(v2:大小文件分池)────────────────
# 默认值按 4C8G 机器调优:小池吃 ~1G 内存,大池吃 ~2G,留充足缓冲
# 大文件单本峰值内存 ~300MB,小文件 ~60MB
#
# 想调更激进:在 docker-compose env 里覆盖即可
#   PARSE_SMALL_CONCURRENCY=24
#   PARSE_LARGE_CONCURRENCY=8

# 小文件解析池 (≤ LARGE_FILE_THRESHOLD_MB)
PARSE_SMALL_CONCURRENCY = int(os.getenv("PARSE_SMALL_CONCURRENCY", 16))

# 大文件解析池 (> LARGE_FILE_THRESHOLD_MB)
PARSE_LARGE_CONCURRENCY = int(os.getenv("PARSE_LARGE_CONCURRENCY", 6))

# 下载并发池(独立于解析,避免下载慢拖累解析)
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", 6))

# 大小文件分界(MB)
LARGE_FILE_THRESHOLD_MB = int(os.getenv("LARGE_FILE_THRESHOLD_MB", 20))
LARGE_FILE_THRESHOLD = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

# (向后兼容)旧代码可能用到的总并发上限,等于小池 + 大池
PARSE_CONCURRENCY = int(os.getenv(
    "PARSE_CONCURRENCY",
    PARSE_SMALL_CONCURRENCY + PARSE_LARGE_CONCURRENCY,
))

# ─────────────── 背压(v2)────────────────
# 队列深度上限,超过返 429。默认 = 总并发 × 3
MAX_QUEUE_DEPTH = int(os.getenv("MAX_QUEUE_DEPTH", 0)) or (
    (PARSE_SMALL_CONCURRENCY + PARSE_LARGE_CONCURRENCY + DOWNLOAD_CONCURRENCY) * 3
)

# 内存压力阈值(0.0 ~ 1.0):
#   超过 MEM_HIGH_WATERMARK 拒接大文件
#   超过 MEM_CRITICAL_WATERMARK 拒接所有任务
# 注:依赖 psutil,没装则跳过(降级为只看 queued)
MEM_HIGH_WATERMARK = float(os.getenv("MEM_HIGH_WATERMARK", "0.85"))
MEM_CRITICAL_WATERMARK = float(os.getenv("MEM_CRITICAL_WATERMARK", "0.95"))

# ─────────────── 看门狗(v2)────────────────
# 单任务超时(秒)。超过这个时间被强制标 failed (code=504)
TASK_TIMEOUT_SEC = int(os.getenv("TASK_TIMEOUT_SEC", 300))

# 看门狗扫描频率(秒)
WATCHDOG_INTERVAL_SEC = int(os.getenv("WATCHDOG_INTERVAL_SEC", 30))

# ─────────────── 去重锁 ────────────────
# 单任务去重锁 TTL(秒):相同 md5+engine 在这个窗口内只跑一次
# 取值应略大于最慢任务的解析时间,避免锁过早释放导致同一任务被重复调度
PARSE_LOCK_TTL = int(os.getenv("PARSE_LOCK_TTL", 600))

# Anna's Archive
# AA_BASE_URL：启动时会从 AA_CANDIDATE_URLS 里自动选最快的一个覆盖这个值
# 若所有候选都不可用，则保留此默认值
AA_BASE_URL = os.getenv("AA_BASE_URL", "https://zh.annas-archive.gl")
AA_SECRET_KEY = os.getenv("AA_SECRET_KEY", "")
AA_SECRET_KEYS = os.getenv("AA_SECRET_KEYS", "")
AA_KEY_ADMIN_SECRET = os.getenv("AA_KEY_ADMIN_SECRET", "beebook")
AA_KEY_COOLDOWN_SECONDS = int(os.getenv("AA_KEY_COOLDOWN_SECONDS", 24 * 3600))

# 搜索镜像候选（逗号分隔）。启动时并行探测，用"能返回真实搜索结果页"作为活判据，
# 选 latency 最低的作为 search_service 的实际 base URL。
AA_CANDIDATE_URLS = [
    u.strip() for u in os.getenv(
        "AA_CANDIDATE_URLS",
        "https://annas-archive.gl,"
        "https://annas-archive.gd,"
        "https://zh.annas-archive.pk,"
        "https://zh.annas-archive.gl,"
        "https://zh.annas-archive.gd,"
        "https://annas-archive.pk"
    ).split(",") if u.strip()
]

# 镜像探测超时（秒）
AA_PROBE_TIMEOUT = int(os.getenv("AA_PROBE_TIMEOUT", 6))

# 搜索请求超时：主镜像（秒）
AA_REQUEST_TIMEOUT = int(os.getenv("AA_REQUEST_TIMEOUT", 15))
# 搜索请求超时：降级到备胎时（秒）。短一些，避免备胎也慢拖垮总时长
AA_FALLBACK_TIMEOUT = int(os.getenv("AA_FALLBACK_TIMEOUT", 6))
# 连续失败多少次触发后台重新探测（重排镜像优先级）
AA_REFRESH_AFTER_FAILS = int(os.getenv("AA_REFRESH_AFTER_FAILS", 3))

# 文件目录
# UPLOAD_DIR:  /api/parse/upload 的临时文件,解析完立即删除
# BOOK_STORAGE_DIR: 通过 md5 下载的原书缓存,按 BOOK_CACHE_TTL_SEC 过期清理
UPLOAD_DIR = os.path.join(os.path.dirname(__file__), "uploads")
BOOK_STORAGE_DIR = os.path.join(os.path.dirname(__file__), "books")

# 原书缓存过期时间(秒) - 默认 3 小时
# 和 REDIS_PARSE_TTL 对齐:上游失败重试窗口,过期后由看门狗自动清理,保证服务无状态
BOOK_CACHE_TTL_SEC = int(os.getenv("BOOK_CACHE_TTL_SEC", 3 * 3600))

# OSS（备用）
OSS_BASE_URL = os.getenv("OSS_BASE_URL", "")

# 服务端口
PORT = int(os.getenv("PORT", 5555))
