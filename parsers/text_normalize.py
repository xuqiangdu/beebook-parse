"""
解析后文本轻量归一化(兜底用)

现在 EPUB 的碎片问题已经在 epub_handler 里按 HTML 语义从源头解决,
本模块只做一些普遍的清理:
  1. 统一换行符 (\\r\\n / \\r → \\n)
  2. 行末空白去掉
  3. 多个连续空行 → 1 个空行
  4. 行内 3+ 空白(制表/连续空格)→ 2 个
  5. 合并极端短行(≤4 字符)到上一行 —— 兜底 PDF 偶发碎片

通过 env TEXT_NORMALIZE=0 可关闭
"""
from __future__ import annotations
import os
import re

ENABLED = os.getenv("TEXT_NORMALIZE", "1") != "0"

SHORT_LINE_CHARS = int(os.getenv("TEXT_NORMALIZE_SHORT_LINE", "4"))

_END_MARKS = set("。.！!？?；;:：")


def _is_cjk(ch: str) -> bool:
    code = ord(ch)
    return (0x4E00 <= code <= 0x9FFF
            or 0x3400 <= code <= 0x4DBF
            or 0x3040 <= code <= 0x30FF
            or 0xAC00 <= code <= 0xD7AF)


def _smart_join(last: str, line: str) -> str:
    """相邻两行拼接:CJK/符号不加空格,其他加"""
    if _is_cjk(last[-1]) or _is_cjk(line[0]):
        return last + line
    if last[-1].isalnum() and line[0].isalnum():
        return last + " " + line
    return last + line


def normalize(text: str) -> str:
    """轻量归一化,避免破坏合法换行"""
    if not text or not ENABLED:
        return text

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = text.split("\n")
    merged: list[str] = []
    for raw in lines:
        line = raw.rstrip()
        if not line:
            merged.append("")
            continue
        # 极端短行(≤4)且上一行未以段落结束符结尾 → 合并
        if (len(line) <= SHORT_LINE_CHARS
                and merged and merged[-1]
                and merged[-1][-1] not in _END_MARKS):
            merged[-1] = _smart_join(merged[-1], line)
        else:
            merged.append(line)

    text = "\n".join(merged)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{3,}", "  ", text)
    return text
