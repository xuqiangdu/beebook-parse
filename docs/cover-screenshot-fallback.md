# 网页封面截图

`POST /api/covers/<md5>/screenshot` 用于原封面链接下载失败后的重绘兜底，只接受 32 位十六进制 MD5。服务打开 `AA_BASE_URL` 对应详情页，截取该 MD5 的封面区域。

成功沿用 JSON envelope，`data` 包含：

```json
{
  "image_base64": "<PNG base64>",
  "mime_type": "image/png",
  "source_kind": "image"
}
```

`source_kind` 为 `image`（网页图片）或 `placeholder`（网站书名作者占位卡）。占位卡也是网页截图，不代表恢复了原图。验证页、错误页、空区域不返回图片。

使用现有 `AA_BROWSER_ENABLED=true`、Playwright Chromium 和 Linux Xvfb 环境。浏览器流程最多等待 `min(AA_BROWSER_TIMEOUT, 45)` 秒；单进程只允许一个截图请求，忙时返回 HTTP 429 和 `Retry-After: 5`。MD5 非法返回 400；网页或浏览器不可用返回 502。PNG 最大 2 MiB。

搜索 HTTP、搜索验证浏览器和截图浏览器共享进程内的 Cookie。新浏览器只恢复域名匹配且未过期的 Cookie，并沿用该站点验证成功时的 User-Agent，保留 Cookie 的 path、Secure、HttpOnly、SameSite 属性。只有确认真实搜索页或对应书籍封面可用后才写回浏览器 Cookie，验证页和截图失败不会覆盖有效会话。

Cookie 不写入文件、Redis、数据库、日志或 API 响应；服务重启后重新验证。不同镜像域名遵循 Cookie 自身域范围，不能把 `.gl` Cookie 直接发送到 `.gd`。Cookie 过期、服务器出口改变或网站要求重新验证时，仍可能进入自动检查或超时失败，复用不保证永久免验证。

调用方应先完整尝试原有封面流程（本地下载、失败时 URL 直传及模型结果解析），整体失败后再请求截图；截图成功后用 `data:image/png;base64,...` 送入重绘模型。发布时先发布本接口，再发布 AIBookServer 调用方，无需数据库迁移。

验证：`.venv/bin/python -m pytest tests/test_aa_session.py tests/test_cover_service.py tests/test_search_service.py -q`。测试包含真实 Chromium 的本地 HTTP 场景：一次搜索验证、两个新浏览器连续截图以及 HTTP 会话复用；不导入应用启动逻辑，不连接或清理 Redis。

2026-10-16 发布前真实验证：上述回归 36 项通过。匿名会话连续搜索 Rich Dad Poor Dad、Atomic Habits、Crimes against logic 分别返回 50、50、6 条结果，三个对应 MD5 的截图接口均返回有效 220×331 PNG。第一本原图 HTTP 415，截图为网站占位卡；另外两本截图为网页真实封面。连续请求复用进程内 Cookie，未调用书籍下载或消耗下载配额。
