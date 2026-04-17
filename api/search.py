"""
搜索 API - 通过官网搜索

GET /api/search  搜索书籍
"""

from flask import Blueprint, request
from services.search_service import search_books
from api.common import (
    api_ok, api_err,
    CODE_PARAM_INVALID, CODE_UPSTREAM_FAIL,
)

search_bp = Blueprint("search", __name__)


def _fix_mojibake(s: str) -> str:
    """
    修正 curl 未 URL 编码时 WSGI 把 UTF-8 字节当 Latin-1 解出的 mojibake。

    例: curl "...?q=中国哲学简史"（不编码）→ Werkzeug 拿到 'ä¸­å\\x9b½å\\x93²...'。
    能成功 latin-1 回编 + UTF-8 重解即为 mojibake，还原为 '中国哲学简史'；
    正常中文字符超出 latin-1 范围，encode 会抛异常，保持原样。
    """
    if not s:
        return s
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


@search_bp.route("/api/search", methods=["GET"])
def search():
    """
    搜索书籍

    参数:
        q        - 搜索关键词（必填）
        lang     - 语言（en/zh/...），可重复传多值
        ext      - 格式（pdf/epub/...），可重复传多值
        content  - 内容类型（book_nonfiction/book_fiction），可重复传多值
        sort     - 排序（newest/oldest/largest/smallest）
        page     - 页码（默认 1）

    示例:
        curl "http://localhost:5555/api/search?q=python&ext=pdf&lang=en"
        curl "http://localhost:5555/api/search?q=python&ext=pdf&ext=epub&ext=mobi"  # 多格式
        curl "http://localhost:5555/api/search?q=中国哲学简史&lang=zh"
    """
    query = _fix_mojibake(request.args.get("q", "").strip())
    # getlist 同时兼容单值（?ext=pdf → ['pdf']）和多值（?ext=pdf&ext=epub → ['pdf','epub']）
    lang = [s.strip() for s in request.args.getlist("lang") if s.strip()]
    ext = [s.strip() for s in request.args.getlist("ext") if s.strip()]
    content_type = [s.strip() for s in request.args.getlist("content") if s.strip()]
    sort = request.args.get("sort", "").strip()
    page = max(1, int(request.args.get("page", 1)))

    if not query:
        return api_err(CODE_PARAM_INVALID, "请提供搜索关键词 q", http_status=400)

    result = search_books(
        query=query,
        lang=lang,
        ext=ext,
        content_type=content_type,
        sort=sort,
        page=page,
    )

    # search_service 返回格式：{total, results, [error], [parser]}
    # 有 error → 上游问题，返 502
    if "error" in result:
        return api_err(CODE_UPSTREAM_FAIL, result["error"], http_status=502,
                       data={"total": 0, "results": []})

    data = {"total": result.get("total", 0), "results": result.get("results", [])}
    # parser 字段（仅降级到正则时出现）也透传，便于调试
    if "parser" in result:
        data["parser"] = result["parser"]
    return api_ok(data)
