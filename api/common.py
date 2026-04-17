from __future__ import annotations
"""
统一响应工具。所有 HTTP 响应 body 均为：
  {
    "code":    int,    # 0 成功；非 0 业务错误码
    "data":    any,    # 业务载荷，错误时为 null
    "message": str,    # 提示信息（成功时为 "success"）
    "timestamp": int   # 毫秒级 unix 时间戳
  }

业务码约定(详见接入文档):
  0   成功
  400 参数错误
  404 资源不存在
  429 过载限流(v2)
  500 内部错误
  501 解析失败
  502 所有上游镜像不可用 / 下载失败
  503 解析结果文本为空
  504 任务超时(v2,看门狗触发)
"""

import time
from flask import jsonify

# ---- 业务码 ----
CODE_OK = 0
CODE_PARAM_INVALID = 400
CODE_NOT_FOUND = 404
CODE_OVERLOADED = 429        # v2: 队列过载/内存吃紧
CODE_INTERNAL = 500
CODE_PARSE_FAILED = 501
CODE_UPSTREAM_FAIL = 502
CODE_EMPTY_CONTENT = 503
CODE_TIMEOUT = 504            # v2: 看门狗超时


def _envelope(code: int, data, message: str) -> dict:
    return {
        "code": code,
        "data": data,
        "message": message,
        "timestamp": int(time.time() * 1000),
    }


def api_ok(data=None, message: str = "success"):
    """成功响应,HTTP 200"""
    return jsonify(_envelope(CODE_OK, data, message))


def api_err(code: int, message: str, http_status: int = 200, data=None,
            headers: dict | None = None):
    """
    错误响应。HTTP 状态由调用方决定:
      - 参数/找不到/内部错误 → 对应 4xx/5xx(遵循 REST 语义)
      - 业务语义错误(解析失败、内容为空等)→ HTTP 200,靠 body.code 区分
      - v2: 429 时 headers 传 {"Retry-After": "5"}
    """
    resp = jsonify(_envelope(code, data, message))
    if headers:
        return resp, http_status, headers
    return resp, http_status
