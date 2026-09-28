# Anna 搜索浏览器验证恢复验收

日期：2026-09-28。目标服务：13.229.64.143:5555。

## 故障证据

- 原 `/api/search?q=Python&lang=en&ext=epub` 返回 HTTP 502，上游为 HTTP 403。
- 在服务所在服务器检查 6 个已配置镜像，均返回 `Server: ddos-guard` 和浏览器验证页。
- 线上 `search_service.py`、`config.py` 与本地修复前版本 SHA-256 一致。
- 普通 requests 和 curl_cffi 均返回 403。无头和默认桌面 Chromium 要求人工检查。
- Chromium 桌面兼容模式完成自动检查；同服务器复用验证会话的 HTTP 请求能返回真实搜索页。

## 修改

- 所有 HTTP 镜像不可用时，在启动探测或后台重探中使用 Chromium 完成验证。
- 只在确认真实搜索结果容器存在后接收会话，再验证普通 HTTP 探测成功。
- 日常请求继续使用 requests；Cookie 只留在内存，验证结束关闭浏览器。
- 缺失搜索结果容器的验证页、维护页不再被解释为合法空结果；继续尝试下一镜像。
- Docker 增加 Playwright Chromium 和 Xvfb。下载、账号池和解析逻辑不变。

## 验证

- 搜索定向测试：15 passed。
- 新部署镜像在独立 Redis 上运行全部测试：56 passed in 99.93s。
- 独立服务冷启动约 22 秒，日志确认浏览器会话验证成功且 HTTP 搜索恢复。
- 下列真实调用均带 AIBookServer 的 `ext=epub&ext=txt`、
  `content=book_nonfiction&content=book_unknown`、`lang`、`page=1` 参数，客户端超时 6 秒。

| 搜索词 | 语言 | HTTP / code | 总数 | 本页条数 | 耗时 |
| --- | --- | --- | --- | --- | --- |
| Atomic Habits | en | 200 / 0 | 91 | 50 | 2.587s |
| 人类简史 | zh | 200 / 0 | 445 | 50 | 1.996s |
| The Wealth of Nations | en | 200 / 0 | 175 | 50 | 3.110s |
| beebook-nonexistent-title-20260928-xq9z81 | en | 200 / 0 | 0 | 0 | 1.719s |

- 清除隔离进程中的验证 Cookie 后，后台重新验证成功；Atomic Habits 再次返回 50 条，耗时 1.744s。
- 测试未使用实际下载账号，未提交书籍下载、导入或解读任务。

## 运行边界

- 这是当前服务器和当前上游检查的实测结果，不能保证上游未来更改规则后仍自动通过。
- 若出现人工验证码或验证超时，搜索明确报错，不把拦截页当成“没有书”。
- 使用现有直发方式部署时需要 `--pull never`，避免被镜像仓库中较旧的 `latest` 覆盖。
