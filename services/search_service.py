from __future__ import annotations
"""
搜索服务 - 通过爬取 Anna's Archive 官网搜索页获取结果

策略：优先使用 BeautifulSoup（CSS 选择器），解析失败时降级到正则。
"""

import re
import time
import logging
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import config

logger = logging.getLogger(__name__)

# 模块级 Session：复用 TLS 连接和 HTTP keep-alive，省去每次握手（~500ms-1s）
# 同时声明 Accept-Encoding，让上游返回 gzip 压缩的 HTML（560KB → ~80KB）
_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
})

# ---- 活跃镜像状态（全模块共享）----
# _alive_mirrors: 按 latency 排序的活跃镜像列表，第一个即"主镜像"
# 读操作用 snapshot 拷贝，避免迭代中被刷新线程改动；写操作持锁
_mirrors_lock = threading.Lock()
_alive_mirrors: list[str] = []
_consecutive_fails = 0          # 连续失败计数
_refresh_in_progress = False    # 去抖：避免多个并发失败同时触发多次 refresh


def _probe_mirror(base: str, timeout: int) -> tuple[str, float | None]:
    """
    探测单个镜像。通过两个判据才算"真可用"：
      1. HTTP 200
      2. 返回 HTML 含 'js-aarecord-list-outer'（真实搜索结果容器）
    这样能过滤 .li 的反爬 JS 页、.gs 的空壳页等"伪 200"镜像。
    返回 (base, latency_ms)；失败返回 (base, None)。
    """
    try:
        t0 = time.time()
        r = _session.get(f"{base}/search",
                         params={"q": "中国"},  # 用常见中文词，保证能命中内容
                         timeout=timeout)
        dt = (time.time() - t0) * 1000
        if r.status_code == 200 and "js-aarecord-list-outer" in r.text:
            return base, dt
    except Exception as e:
        logger.debug(f"镜像探测失败 {base}: {e}")
    return base, None


def _probe_all_mirrors() -> list[str]:
    """
    并行探测所有候选镜像，返回按 latency 升序排列的"真可用"镜像列表。
    全部不可用时返回 [config.AA_BASE_URL]（保底，后续请求可能仍会挂）。
    """
    candidates = config.AA_CANDIDATE_URLS or [config.AA_BASE_URL]
    logger.info(f"开始探测镜像可用性：{len(candidates)} 个候选")

    alive: list[tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=len(candidates)) as ex:
        futures = [ex.submit(_probe_mirror, c, config.AA_PROBE_TIMEOUT)
                   for c in candidates]
        for f in as_completed(futures):
            base, latency = f.result()
            if latency is not None:
                alive.append((base, latency))
                logger.info(f"  ✓ {base:<40} {latency:.0f}ms")
            else:
                logger.info(f"  ✗ {base:<40} 不可用")

    if not alive:
        logger.warning(f"所有镜像都不可用，回退到 {config.AA_BASE_URL}")
        return [config.AA_BASE_URL]

    alive.sort(key=lambda x: x[1])
    chosen = [m for m, _ in alive]
    logger.info(f"选中主镜像：{chosen[0]}（{alive[0][1]:.0f}ms），候补 {len(chosen)-1} 个")
    return chosen


def _update_mirrors(new_list: list[str]):
    """原子替换活跃镜像列表（持锁），重置失败计数"""
    global _alive_mirrors, _consecutive_fails
    with _mirrors_lock:
        _alive_mirrors = new_list
        _consecutive_fails = 0


def _get_mirrors_snapshot() -> list[str]:
    """返回活跃镜像的只读快照，调用方迭代期间不受刷新影响"""
    with _mirrors_lock:
        return list(_alive_mirrors)


def _trigger_refresh_async():
    """
    后台异步重探镜像。用 _refresh_in_progress 去抖，避免多个并发
    失败请求同时触发多次重探（探测本身是 2-6s 的开销）。
    """
    global _refresh_in_progress
    with _mirrors_lock:
        if _refresh_in_progress:
            return
        _refresh_in_progress = True

    def _run():
        global _refresh_in_progress
        try:
            new_list = _probe_all_mirrors()
            _update_mirrors(new_list)
        except Exception as e:
            logger.exception(f"后台刷新镜像失败: {e}")
        finally:
            with _mirrors_lock:
                _refresh_in_progress = False

    threading.Thread(target=_run, daemon=True, name="mirror-refresh").start()


def _record_success():
    """请求成功 → 重置连续失败计数"""
    global _consecutive_fails
    with _mirrors_lock:
        _consecutive_fails = 0


def _record_failure() -> bool:
    """
    请求失败 → 失败计数 +1，达到阈值时返回 True 表示应该触发后台刷新
    """
    global _consecutive_fails
    with _mirrors_lock:
        _consecutive_fails += 1
        return _consecutive_fails >= config.AA_REFRESH_AFTER_FAILS


# ---- 启动时探测一次 ----
_alive_mirrors = _probe_all_mirrors()
AA_BASE_URL = _alive_mirrors[0] if _alive_mirrors else config.AA_BASE_URL


def refresh_mirror() -> str:
    """
    手动/同步刷新活跃镜像。返回新选中的主 base URL。
    """
    global AA_BASE_URL
    new_list = _probe_all_mirrors()
    _update_mirrors(new_list)
    AA_BASE_URL = new_list[0] if new_list else config.AA_BASE_URL
    return AA_BASE_URL


def search_books(
    query: str = "",
    lang: str | list[str] = "",
    ext: str | list[str] = "",
    content_type: str | list[str] = "",
    sort: str = "",
    page: int = 1,
) -> dict:
    """
    搜索书籍（通过官网搜索页）

    lang / ext / content_type 可以是字符串或列表：
      - 字符串 "pdf"                       → 单值过滤
      - 列表 ["pdf","epub","mobi"]         → 多值过滤（OR 关系），requests 会自动
        展开成 ?ext=pdf&ext=epub&ext=mobi，Anna's Archive 原生支持

    容灾逻辑：
      1. 主镜像（_alive_mirrors[0]）失败 → 立即尝试下一个（用更短超时）
      2. 全部镜像失败 → 返回 error 并触发后台重探
      3. 任一镜像成功 → 重置失败计数；若命中的是备胎而非主镜像，
         说明主镜像可能已经不行，也触发后台刷新重排优先级
    """
    if not query.strip():
        return {"total": 0, "results": [], "error": "请提供搜索关键词"}

    params: dict = {"q": query}
    if lang:
        params["lang"] = lang
    if ext:
        params["ext"] = ext
    if content_type:
        params["content"] = content_type
    if sort:
        params["sort"] = sort
    if page > 1:
        params["page"] = str(page)

    mirrors = _get_mirrors_snapshot()
    if not mirrors:
        mirrors = [config.AA_BASE_URL]

    html = None
    used_mirror = None
    last_err = None
    primary = mirrors[0]
    for i, mirror in enumerate(mirrors):
        # 主镜像用长超时，备胎用短超时（避免一个 timeout 拖长总响应）
        timeout = config.AA_REQUEST_TIMEOUT if i == 0 else config.AA_FALLBACK_TIMEOUT
        try:
            resp = _session.get(f"{mirror}/search", params=params, timeout=timeout)
            resp.raise_for_status()
            html = resp.text
            used_mirror = mirror
            break
        except Exception as e:
            last_err = e
            logger.warning(f"镜像请求失败 [{mirror}] timeout={timeout}s: {e}")
            continue

    if html is None:
        # 全挂了：记失败 + 触发后台重探
        if _record_failure():
            logger.error(f"连续 {config.AA_REFRESH_AFTER_FAILS} 次失败，触发镜像重探")
            _trigger_refresh_async()
        return {"total": 0, "results": [],
                "error": f"所有镜像暂不可用: {last_err}"}

    _record_success()
    # 命中的不是主镜像（主挂了走备胎成功）→ 异步重排，把更可用的顶上来
    if used_mirror != primary:
        logger.info(f"降级命中备胎 {used_mirror}（主 {primary} 失败），后台重新选主")
        _trigger_refresh_async()

    # 先用 BeautifulSoup 解析，失败时降级到正则
    try:
        result = _parse_with_bs4(html)
        if result["results"]:
            return result
        logger.warning("BS4 解析无结果，降级到正则")
    except Exception as e:
        logger.warning(f"BS4 解析异常，降级到正则: {e}")

    return _parse_with_regex(html)


def _parse_with_bs4(html: str) -> dict:
    """主方案：使用 BeautifulSoup CSS 选择器"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 总数
    total = _extract_total(html)

    # 搜索结果卡片（每个卡片是 js-aarecord-list-outer 的直接子 div）
    outer = soup.select_one(".js-aarecord-list-outer")
    cards = outer.find_all("div", recursive=False) if outer else []

    results = []
    for card in cards:
        # md5 从第一个 /md5/ 链接取
        md5_link = card.select_one('a[href*="/md5/"]')
        if not md5_link:
            continue
        href = md5_link.get("href", "")
        md5_match = re.search(r"/md5/([a-f0-9]{32})", href)
        if not md5_match:
            continue
        md5 = md5_match.group(1)

        # 文本链接：标题、作者、出版社（用 class 包含 line-clamp- 来过滤）
        text_links = []
        for a in card.select("a"):
            classes = a.get("class") or []
            if any("line-clamp-" in c for c in classes):
                txt = a.get_text(strip=True)
                if txt and txt != "Save":
                    text_links.append(a)

        title = text_links[0].get_text(strip=True) if len(text_links) > 0 else ""
        author = text_links[1].get_text(strip=True) if len(text_links) > 1 else ""
        publisher = text_links[2].get_text(strip=True) if len(text_links) > 2 else ""

        # 封面图（卡片内第一个 <img> 的 src）
        img_tag = card.find("img")
        cover_url = img_tag.get("src", "") if img_tag else ""

        # 描述 / 目录摘要：class 中同时含 line-clamp、text-sm、text-gray-600
        description = ""
        for d in card.find_all("div"):
            cls = d.get("class") or []
            if (
                any("line-clamp" in c for c in cls)
                and "text-sm" in cls
                and "text-gray-600" in cls
            ):
                description = d.get_text(strip=True)
                break

        # 元信息行：精确匹配 class，找最深层的叶子 div（只含自己的文本，不包含子 div）
        meta_text = ""
        candidates = []
        for d in card.find_all("div"):
            classes = d.get("class") or []
            # 元信息 div 的特征：有 font-semibold 且 text-gray 类
            if "font-semibold" in classes and any("gray" in c for c in classes):
                # 用 " ".join(d.stripped_strings) 避免子元素嵌套内容串在一起
                t = " ".join(d.find_all(text=True, recursive=False)).strip()
                # 如果只取直接文本不够，再取完整（但排除嵌套 div）
                if not t or "·" not in t:
                    # 复制一份，移除嵌套的 div 再取 text
                    import copy
                    d_copy = copy.copy(d)
                    for sub in d_copy.find_all("div"):
                        sub.decompose()
                    t = d_copy.get_text(strip=True)
                if "·" in t and re.search(r"\[\w{2,3}\]", t):
                    candidates.append(t)

        # 选最短的（最接近纯元信息）
        if candidates:
            meta_text = min(candidates, key=len)

        # 兜底：如果上面没找到，用宽松模式但排除过长的
        if not meta_text:
            for d in card.find_all("div"):
                t = d.get_text(strip=True)
                if "·" in t and re.search(r"\[\w{2,3}\]", t) and len(t) < 250:
                    meta_text = t
                    break

        meta_info = _parse_meta(meta_text)

        results.append({
            "md5": md5,
            "title": title,
            "author": author,
            "publisher": publisher,
            "cover_url": cover_url,
            "description": description,
            **meta_info,
            "detail_url": f"{AA_BASE_URL}/md5/{md5}",
        })

    return {"total": total or len(results), "results": results}


def _parse_meta(text: str) -> dict:
    """
    解析元信息行，如:
      "英语 [en] · 繁体中文 [zh-Hant] · 中文 [zh] · EPUB · 0.5MB · 📘 非小说类图书 · 🚀/duxiu/lgli/zlib"

    同一 md5 在 AA 上可能挂多个语言标签(来自不同数据源对这本书的语言标注),
    全部保留到 languages / language_names;language / language_name 保留首项做向后兼容。
    """
    info = {
        "language": "",
        "language_name": "",
        "languages": [],
        "language_names": [],
        "extension": "",
        "filesize_str": "",
        "content_type_name": "",
        "sources": [],
    }

    # 语言 "中文 [zh]" / "English [en]" / "繁体中文 [zh-Hant]"
    # 放宽到 [\w-]{2,8} 以兼容 BCP-47 子标签 (zh-Hant / pt-BR / sr-Latn 等)
    lang_matches = re.findall(r"(\S+)\s*\[([\w-]{2,8})\]", text)
    if lang_matches:
        info["language_names"] = [name for name, _ in lang_matches]
        info["languages"] = [code for _, code in lang_matches]
        info["language_name"] = lang_matches[0][0]
        info["language"] = lang_matches[0][1]

    # 格式（大写或小写 .ext）
    for part in text.split("·"):
        p = part.strip()
        # PDF / EPUB 这种纯大小写格式
        if re.fullmatch(r"[A-Z]{2,5}", p) or re.fullmatch(r"[a-z]{2,5}", p):
            info["extension"] = p.lower()
            break

    # 文件大小
    size_match = re.search(r"([\d.]+\s*[KMGT]B)", text)
    if size_match:
        info["filesize_str"] = size_match.group(1).replace(" ", "")

    # 内容类型（📘 后面的文字）
    content_match = re.search(r"📘\s*(\S+)", text)
    if content_match:
        info["content_type_name"] = content_match.group(1)

    # 来源（🚀 后面 /xxx/yyy/zzz）
    source_match = re.search(r"🚀\s*/([\w/]+)", text)
    if source_match:
        info["sources"] = [s for s in source_match.group(1).split("/") if s]

    return info


def _extract_total(html: str) -> int:
    """提取搜索结果总数"""
    patterns = [
        r"总计\s*(\d[\d,]*)",
        r"(\d[\d,]*)\s*TOTAL",
        r"TOTAL[)\s]*(\d[\d,]*)",
    ]
    for p in patterns:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return int(m.group(1).replace(",", ""))
    return 0


def _parse_with_regex(html: str) -> dict:
    """降级方案：正则解析（DOM 结构变化时兜底）"""
    total = _extract_total(html)
    results = []

    blocks = re.split(r'<a[^>]*href="/md5/', html)
    seen = set()

    for block in blocks[1:]:
        md5_match = re.match(r"([a-f0-9]{32})", block)
        if not md5_match:
            continue
        md5 = md5_match.group(1)
        if md5 in seen:
            continue
        seen.add(md5)

        chunk = block[:3000]
        contents = re.findall(r'data-content="([^"]*)"', chunk)

        ext_match = re.search(r"·\s*([A-Z]{2,5})\s*·", chunk)
        size_match = re.search(r"([\d.]+\s*[KMGT]B)", chunk)
        # 和 _parse_meta 保持一致:全部语言都抓,放宽到 BCP-47 子标签
        lang_codes = re.findall(r"\[([\w-]{2,8})\]", chunk)
        cover_match = re.search(r'<img[^>]*\bsrc="([^"]+)"', chunk)

        results.append({
            "md5": md5,
            "title": contents[0] if len(contents) > 0 else "",
            "author": contents[1] if len(contents) > 1 else "",
            "publisher": contents[2] if len(contents) > 2 else "",
            "cover_url": cover_match.group(1) if cover_match else "",
            "extension": ext_match.group(1).lower() if ext_match else "",
            "filesize_str": size_match.group(1).replace(" ", "") if size_match else "",
            "language": lang_codes[0] if lang_codes else "",
            "languages": lang_codes,
            "detail_url": f"{AA_BASE_URL}/md5/{md5}",
        })

    return {"total": total or len(results), "results": results, "parser": "regex-fallback"}
