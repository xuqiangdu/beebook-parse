# beebook-parse

书籍搜索 + 下载 + 解析服务。

## 架构

```
客户端
  │
  ├── GET /api/search?q=xxx ──→ 爬取 Anna's Archive 官网搜索页 ──→ 返回书籍列表
  │
  ├── POST /api/parse {md5} ──→ 调用官方 Fast Download API 下载 ──→ 解析为文本
  │                                                                    ↓
  └── GET /api/parse/<id>   ──→ 从 Redis 获取解析结果 ←──── 文本缓存（1小时）
```

只需要 2 个容器：**app + redis**，不需要 ES、MariaDB、数据导入。

## 机器配置

| 配置项 | 最低 | 推荐 |
|--------|------|------|
| CPU | 1核 | 2核 |
| 内存 | 1GB | 2GB |
| 磁盘 | 5GB | 20GB（缓存下载的书） |

## 部署

### 1. 配置环境变量

```bash
cp .env.example .env
vi .env
```

Anna 账号只允许配置在未跟踪的 `.env` 或部署平台 Secret 中：

```env
AA_SECRET_KEYS=key1,key2,key3
```

`AA_SECRET_KEY` 仅保留旧部署兼容；新部署统一使用 `AA_SECRET_KEYS`。
脱敏账号池健康接口如需启用，使用部署 Secret 单独设置
`AA_KEY_ADMIN_SECRET`。新增或替换账号后必须重启服务。原始账号 Key 不写入 Redis、API
响应、日志、代码、测试或文档；Redis 只保存不可逆账号 ID 和状态。

账号池轮转、冷却、同 MD5 内容锁和 Parse 缓存复用 Compose 中原有的
Redis，并通过独立 key 前缀隔离；Redis 地址、TTL 和解析池参数使用
Compose 或代码默认值，不写入账号 `.env`。相同 MD5 使用分布式内容锁，下载先写唯一
`.part.*` 文件，长度校验通过后才原子发布，半文件不会进入缓存。
默认拒绝小于 32 字节的原文件，避免第三方占位文本进入书籍缓存。

### 2. 启动服务

```bash
docker compose up -d
```

首次启动会构建镜像（约 1-2 分钟）。

### 3. 验证

```bash
# 健康检查
curl http://localhost:5555/health

# 账号池脱敏健康状态（只读）
curl -H "X-Admin-Secret: <admin-secret>" \
  http://localhost:5555/api/admin/aa-keys/health

# 搜索
curl "http://localhost:5555/api/search?q=python&ext=pdf"

# 解析
curl -X POST http://localhost:5555/api/parse \
  -H "Content-Type: application/json" \
  -d '{"md5": "e1a448749dbc9402584da3c64a6eeac9", "extension": "pdf"}'

# 轮询结果
curl http://localhost:5555/api/parse/e1a448749dbc9402584da3c64a6eeac9_default
```

### 测试服务器本地直发

不依赖 GitHub Actions 或 Docker Hub，在本机完成 `linux/amd64` 构建后通过
SSH 传输镜像，并只重建远端 `beebook-app`：

```bash
./deploy-remote.sh
```

脚本固定发布到 `ubuntu@13.229.64.143:/home/ubuntu/beebook-parse`，不会执行
`docker compose down`、不会重启 `beebook-redis`、不会清理全局镜像。发布前后
会核对所有非目标容器，健康检查失败时自动恢复上一个 `beebook-app` 镜像。
Anna Key 仍只维护在远端未跟踪的 `.env`，不会进入构建上下文或镜像。

## API

### 搜索

```
GET /api/search?q=关键词&lang=zh&ext=pdf
```

| 参数 | 说明 |
|------|------|
| q | 搜索关键词（必填） |
| lang | 语言：en/zh/... |
| ext | 格式：pdf/epub/... |
| content | 类型：book_nonfiction/book_fiction |
| sort | 排序：newest/oldest/largest/smallest |
| page | 页码 |

返回：
```json
{
  "total": 443,
  "results": [
    {
      "md5": "0d585b8f3a19b248f5b4e3e283aa35ba",
      "title": "中国哲学简史",
      "author": "冯友兰著; 赵复三译",
      "extension": "pdf",
      "filesize_str": "76.6MB",
      "year": "2013",
      "language": "zh"
    }
  ]
}
```

### 解析

```
POST /api/parse
Content-Type: application/json
{"md5": "xxx", "extension": "pdf"}
```

返回：`{"task_id": "xxx_default", "status": "processing"}`

### 轮询

```
GET /api/parse/<task_id>
```

完成时返回：
```json
{
  "status": "completed",
  "engine": "pymupdf",
  "text": "全文内容...",
  "total_length": 213732,
  "parse_time_ms": 271
}
```

失败（扫描版 PDF）：
```json
{
  "status": "failed",
  "error": "扫描版 PDF（图片无文字层），无法提取文本，需要 OCR 处理"
}
```

### 支持的格式

```
GET /api/formats
```

| 格式 | 引擎 | 说明 |
|------|------|------|
| pdf | PyMuPDF | 有文字层的 PDF |
| epub | EbookLib | 电子书 |
| fb2 | lxml | FictionBook |
| mobi/azw3 | mobi | Kindle |
| djvu | djvutxt | 需安装系统依赖 |
| txt | 直接读取 | 纯文本 |
| docx | lxml | Word 文档 |

## 日常运维

```bash
# 启动
docker compose up -d

# 查看日志
docker compose logs -f app

# 停止
docker compose down

# 重建（代码更新后）
docker compose up -d --build app

# 清理下载缓存
rm -rf books/*
```

## 目录结构

```
beebook-parse/
├── docker-compose.yml       # 服务编排（app + redis）
├── Dockerfile               # 应用镜像
├── .env.example             # 环境变量模板
├── requirements.txt         # Python 依赖
├── app.py                   # 入口
├── config.py                # 配置
├── api/
│   ├── parse.py             # 解析 API
│   └── search.py            # 搜索 API
├── parsers/
│   ├── base.py              # 基类（策略模式）
│   ├── factory.py           # 工厂（自动选 Handler）
│   ├── pdf_handler.py       # PDF 解析
│   ├── epub_handler.py      # EPUB 解析
│   ├── fb2_handler.py       # FB2 解析
│   ├── djvu_handler.py      # DJVU 解析
│   ├── mobi_handler.py      # MOBI/AZW3 解析
│   └── simple_handlers.py   # TXT/DOCX/CBR/CBZ
├── services/
│   ├── book_storage.py      # 文件获取（本地/官方API/OSS）
│   ├── redis_store.py       # Redis 缓存
│   ├── search_service.py    # 搜索（爬官网）
│   └── task_manager.py      # 异步任务
├── books/                   # 下载的原书缓存（TTL 3h，看门狗自动清理）
└── uploads/                 # 上传接口的临时文件（解析完立即删除）
```

> **无状态设计**:本服务仅作为中间解析能力，不永久保存任何文件。
> - 解析结果只存 Redis,`REDIS_PARSE_TTL` 默认 3 小时
> - 原书下载缓存 `books/`,`BOOK_CACHE_TTL_SEC` 默认 3 小时,由看门狗定期扫描清理
> - 上传文件 `uploads/`,解析完(成功/失败/异常)立即删除
>
> 3 小时窗口用于支撑上游失败重试的快速响应;之外零残留。
