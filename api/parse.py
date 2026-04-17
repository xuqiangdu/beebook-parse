from __future__ import annotations
"""
解析 API (v2)

POST /api/parse              通过 md5 提交解析任务(v2 真异步)
POST /api/parse/upload       上传文件解析(仅测试用)
GET  /api/parse/<task_id>    轮询任务状态 + 获取结果
GET  /api/formats            查看支持的格式
GET  /api/pressure           查看当前服务负载(v2)

响应统一用 api/common.py 的包装:{code, data, message, timestamp}
"""

from flask import Blueprint, request

from services.task_manager import (
    submit_parse_by_md5,
    submit_parse_by_file,
    get_task_status,
    get_task_text,
    is_overloaded,
    get_pressure,
)
from parsers.factory import ParserFactory
from api.common import (
    api_ok, api_err,
    CODE_PARAM_INVALID, CODE_NOT_FOUND,
    CODE_PARSE_FAILED, CODE_EMPTY_CONTENT,
    CODE_OVERLOADED, CODE_TIMEOUT, CODE_UPSTREAM_FAIL,
)

parse_bp = Blueprint("parse", __name__)
_factory = ParserFactory()

# v2 中间状态全集(客户端轮询时这些都返回 code=0)
INTERMEDIATE_STATES = {"pending", "downloading", "parsing", "processing"}


@parse_bp.route("/api/parse", methods=["POST"])
def create_parse_task():
    """
    通过 md5 提交解析任务(v2 真异步,立刻返回 task_id)。

    请求 JSON: { "md5": "...", "extension": "pdf" }
    查询参数: ?engine=pymupdf  可选,指定 PDF 引擎
    """
    data = request.get_json(silent=True)
    if not data or not data.get("md5"):
        return api_err(CODE_PARAM_INVALID,
                       '请提供 md5,如: {"md5": "abc...", "extension": "pdf"}',
                       http_status=400)

    md5 = data["md5"].strip().lower()
    extension = data.get("extension", "").strip().lower()
    engine = request.args.get("engine")

    # ⭐v2 背压检查(过载返 429)
    overloaded, reason = is_overloaded()
    if overloaded:
        return api_err(
            CODE_OVERLOADED,
            f"服务繁忙: {reason},请稍后重试",
            http_status=429,
            data=get_pressure(),
            headers={"Retry-After": "5"},
        )

    result = submit_parse_by_md5(md5, extension, engine)
    if "error" in result:
        return api_err(CODE_NOT_FOUND, result["error"], http_status=404)

    return api_ok(result)


@parse_bp.route("/api/parse/upload", methods=["POST"])
def upload_and_parse():
    """
    上传文件解析(仅测试用)。
    示例:curl -F "file=@book.pdf" http://localhost:5555/api/parse/upload
    """
    if "file" not in request.files:
        return api_err(CODE_PARAM_INVALID,
                       "请上传文件: -F 'file=@yourfile.pdf'",
                       http_status=400)

    file = request.files["file"]
    if file.filename == "":
        return api_err(CODE_PARAM_INVALID, "文件名为空", http_status=400)

    # ⭐v2 上传同样要走背压(按上传体积估算大小)
    file_data = file.read()
    overloaded, reason = is_overloaded(file_size=len(file_data))
    if overloaded:
        return api_err(
            CODE_OVERLOADED,
            f"服务繁忙: {reason},请稍后重试",
            http_status=429,
            data=get_pressure(),
            headers={"Retry-After": "5"},
        )

    engine = request.args.get("engine")
    result = submit_parse_by_file(file_data, file.filename, engine)
    return api_ok(result)


@parse_bp.route("/api/parse/<task_id>", methods=["GET"])
def poll_parse_task(task_id):
    """
    轮询解析任务状态(v2 状态枚举):

      - pending / downloading / parsing / processing: code=0,客户端继续轮询
      - completed + text 非空: code=0 data.text 含全文
      - completed + text 空:    code=503 内容为空
      - failed (501): 解析器失败
      - failed (502): 下载失败
      - failed (504): 看门狗超时(v2)
      - 任务不存在:   code=404
    """
    meta = get_task_status(task_id)
    if meta is None:
        return api_err(CODE_NOT_FOUND, "任务不存在或已过期", http_status=404)

    payload = {"task_id": task_id, **meta}

    status = meta.get("status")
    if status == "completed":
        text = get_task_text(task_id)
        payload["text"] = text or ""
        # 解析完成但文本为空 → 503
        if not text:
            return api_err(
                CODE_EMPTY_CONTENT,
                "解析完成但文本内容为空(可能是纯扫描件且未 OCR)",
                data=payload,
            )
        return api_ok(payload)

    if status == "failed":
        # ⭐v2 根据 error_code 返回不同业务码
        error_code = meta.get("error_code", CODE_PARSE_FAILED)
        if error_code == CODE_TIMEOUT:
            return api_err(CODE_TIMEOUT, meta.get("error", "任务超时"),
                           data=payload)
        if error_code == CODE_UPSTREAM_FAIL:
            return api_err(CODE_UPSTREAM_FAIL,
                           meta.get("error", "上游/下载失败"),
                           http_status=502, data=payload)
        return api_err(CODE_PARSE_FAILED, meta.get("error", "解析失败"),
                       data=payload)

    # ⭐v2 中间状态(pending/downloading/parsing/processing)都是 code=0
    if status in INTERMEDIATE_STATES:
        return api_ok(payload)

    # 未知状态:防御性兜底
    return api_ok(payload)


@parse_bp.route("/api/formats", methods=["GET"])
def list_formats():
    """列出支持的格式和引擎"""
    return api_ok(_factory.supported_formats())


@parse_bp.route("/api/pressure", methods=["GET"])
def pressure():
    """⭐v2 查看当前负载(供上游主动检查、做智能批量提交决策)"""
    return api_ok(get_pressure())
