# P0 多语言分库 Parse 账号池与韧性测试报告

- 日期：2026-07-28
- 分支：`feature/p0-multilingual-library-import`
- 基线：`3b133cd`
- 范围：OpenSpec `8.1`–`8.11`
- 真实 Anna fast-download 调用：已在所有 mock 回归完成后执行
- 测试上游：mock 函数、`127.0.0.1` 临时 HTTP 服务、真实 Anna

## 实现结果

| OpenSpec | 结果 | 实现 |
|---|---|---|
| 8.1 | 通过 | 按最终职责移除 parse 的 Anna 专用并发 2；导书并发由 AIBookServer 控制，parse 保留独立下载/解析池 |
| 8.2 | 通过 | Redis `INCR` 原子轮转账号，冷却探测使用原子 probe lease |
| 8.3 | 通过 | 仅持久化账号 ID、active/cooldown/disabled、last success/error、next probe |
| 8.4 | 通过 | 单账号 quota 或 membership 失败后继续尝试下一账号 |
| 8.5 | 通过 | quota + disabled 返回 10002；全 disabled 返回 10001；普通 429 保持 429 |
| 8.6 | 通过 | 10001/10002 响应包含 `next_probe_at`、`retry_after_seconds` 和 `Retry-After` |
| 8.7 | 通过 | 背压统计 queued + downloading + parsing，并用 Lua 原子预留总在途槽位 |
| 8.8 | 通过 | task lock、meta、chunk 和 counter 使用 attempt generation fencing |
| 8.9 | 通过 | 账号状态、轮转、探测和同 MD5 锁使用原 Redis 独立前缀；启动时删除旧 Redis 原始 Key 集合 |
| 8.10 | 通过 | 覆盖多账号、混合错误、探测、同 MD5 锁、背压、fencing、半文件、过小占位内容、原子发布和本地假上游 |
| 8.11 | 通过 | 账号只从环境加载，删除运行时新增账号接口，增加只读脱敏健康接口 |

## 自动化测试

运行环境使用一次性本地 Redis，未配置任何真实 Anna 账号：

```bash
REDIS_HOST=127.0.0.1 \
REDIS_PORT=16380 \
REDIS_DB=0 \
WATCHDOG_INTERVAL_SEC=1 \
AA_SECRET_KEY= \
AA_SECRET_KEYS= \
.venv/bin/python -m pytest -q
```

结果：

```text
34 passed in 11.75s
```

覆盖：

- 原有启动自愈、看门狗、缓存清理、内存保护、状态流转和 counter 测试。
- 新增账号轮转、Redis 无原始 Key、quota/disabled 聚合、普通错误隔离、
  冷却单探测、同 MD5 串行、
  总在途背压、attempt fencing、业务状态码、动态 Retry-After、脱敏健康接口、
  半包清理、过小占位内容拒绝、过期任务禁止发布、本地 HTTP quota/普通
  429 和日志脱敏测试。

## 真实 Anna 最终测试

真实账号只写入被 Git 忽略的 `.env`，报告、代码、Redis、日志和提交均不
包含原始 Key。

1. `q=Jane Eyre&lang=en&ext=txt` 搜索成功；
2. MD5 `e1385081818aa05ecf039e436465dce9` 冷解析：
   `cached=false`、161,204B、156,677 字符、completed；
3. 同 MD5 再次提交 `cached=true`；
4. 清理同一内容缓存后重复 30 次仍未触发 10002，说明相同内容重复获取不
   作为新的独立配额消耗；
5. 改用不同 MD5 的小体积 TXT 串行测试，最多 30 条；
6. 第 28 个候选返回 HTTP 429、业务 code 10002、
   `Retry-After: 1780`；
7. 客户端收到 10002 后立即停止，没有提交下一条；
8. 健康状态变为 `configured=1, active=0, cooldown=1`，并保留
   `next_probe_at`。

真实搜索同时暴露一个上游数据质量场景：某条宣称 0.1MB 的 TXT 实际只
下载到 6B 占位内容。实现已新增 `MIN_BOOK_FILE_BYTES=32`，过小文件不
发布、不缓存、不残留 `.part`，并补充自动用例。

23 个真实成功下载文件已从仓库移到
`/tmp/codex-real-anna-distinct-20260728`，未直接删除，可恢复。

## 静态和部署校验

```text
python syntax ok
git diff --check: passed
docker compose --env-file /dev/null config --quiet: passed
```

## 安全检查

- 测试账号字符串运行时随机生成，不写入代码或报告。
- mock 自动测试显式清空账号环境变量；真实最终测试只从被忽略的 `.env`
  加载专用账号。
- Redis 只保存 SHA-256 截断账号 ID，不保存原始账号 Key。
- `/api/admin/aa-keys/health` 和旧只读列表只返回脱敏 ID 与状态。
- 已移除 `/api/admin/aa-keys/add`；新增账号必须修改忽略的 `.env` 或部署环境并重启。
- 异常消息写 Redis/API 前执行运行时 Key 脱敏，日志只记录账号 ID。
- 账号状态和内容锁使用原 Redis 的 `aa:control:*` 前缀，不再要求第二套 Redis。
- 原书使用唯一临时文件写入，校验后原子发布；半文件和过期 attempt 不进入缓存。
- 默认拒绝小于 32B 的占位/异常原文件，可通过 `MIN_BOOK_FILE_BYTES`
  调整。

## 运行方式

1. 复制 `.env.example` 为被 Git 忽略的 `.env`。
2. 在 `.env` 中只配置逗号分隔的 `AA_SECRET_KEYS`。
3. 设置独立管理凭证后启动：

```bash
docker compose up -d
```

4. 查看聚合健康状态：

```bash
curl http://localhost:5555/health
```

5. 使用管理凭证查看只读账号明细：

```bash
curl -H "X-Admin-Secret: <admin-secret>" \
  http://localhost:5555/api/admin/aa-keys/health
```

原始账号 Key 不应出现在命令参数、日志或 API 响应中。
