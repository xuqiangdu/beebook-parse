from __future__ import annotations
"""
解析器工厂 - 根据文件扩展名自动选择 Handler

使用方式:
    factory = ParserFactory()
    handler = factory.get_handler("pdf")
    result = handler.parse("/path/to/file.pdf")
"""

import os

from parsers.base import BaseParser, ParseResult
from parsers.pdf_handler import PdfTextHandler, PyMuPdfHandler
from parsers.epub_handler import EpubHandler
from parsers.fb2_handler import Fb2Handler
from parsers.djvu_handler import DjvuHandler
from parsers.mobi_handler import MobiHandler
from parsers.simple_handlers import TxtHandler, DocxHandler, CbzHandler, CbrHandler


# 扩展名 → MIME 类型映射（用于文件类型检测的 fallback）
MIME_TO_EXT = {
    "application/pdf": "pdf",
    "application/epub+zip": "epub",
    "application/x-mobipocket-ebook": "mobi",
    "image/vnd.djvu": "djvu",
    "application/x-fictionbook+xml": "fb2",
    "text/plain": "txt",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "application/x-cbz": "cbz",
    "application/x-cbr": "cbr",
    "application/zip": "zip",
    "application/x-rar-compressed": "cbr",
}


class ParserFactory:
    """解析器工厂"""

    def __init__(self):
        # 注册所有 Handler，顺序即为同扩展名时的优先级
        self._handlers: list[BaseParser] = [
            PyMuPdfHandler(),
            PdfTextHandler(),
            EpubHandler(),
            Fb2Handler(),
            DjvuHandler(),
            MobiHandler(),
            TxtHandler(),
            DocxHandler(),
            CbzHandler(),
            CbrHandler(),
        ]
        # 扩展名 → 默认 Handler 映射
        self._ext_map: dict[str, BaseParser] = {}
        for handler in self._handlers:
            for ext in handler.extensions:
                if ext not in self._ext_map:
                    self._ext_map[ext] = handler

    def get_handler(self, ext: str, engine: str = None) -> BaseParser | None:
        """
        根据扩展名获取 Handler。
        可选指定 engine 名称（如 "pymupdf"）来选择特定引擎。
        """
        ext = ext.lower().lstrip(".")
        if engine:
            for handler in self._handlers:
                if handler.engine_name == engine and handler.can_handle(ext):
                    return handler
        return self._ext_map.get(ext)

    def detect_and_get_handler(self, filepath: str, filename: str = None) -> tuple[BaseParser | None, str]:
        """
        自动检测文件类型并返回 Handler 和检测到的扩展名。

        优先用文件名扩展名，fallback 到 MIME 类型检测。
        """
        # 1. 从文件名提取扩展名
        name = filename or os.path.basename(filepath)
        if name.lower().endswith(".fb2.zip"):
            return self._ext_map.get("fb2"), "fb2"
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""

        if ext and ext in self._ext_map:
            return self._ext_map[ext], ext

        # 2. MIME 类型检测
        detected_ext = self._detect_by_mime(filepath)
        if detected_ext and detected_ext in self._ext_map:
            return self._ext_map[detected_ext], detected_ext

        # 3. Magic bytes 检测
        detected_ext = self._detect_by_magic_bytes(filepath)
        if detected_ext and detected_ext in self._ext_map:
            return self._ext_map[detected_ext], detected_ext

        return None, ext

    def _detect_by_mime(self, filepath: str) -> str | None:
        try:
            import magic as _magic
            mime = _magic.from_file(filepath, mime=True)
            return MIME_TO_EXT.get(mime)
        except Exception:
            return None

    def _detect_by_magic_bytes(self, filepath: str) -> str | None:
        """通过文件头字节检测格式"""
        try:
            with open(filepath, "rb") as f:
                header = f.read(16)
        except Exception:
            return None

        if header[:5] == b"%PDF-":
            return "pdf"
        if header[:4] == b"PK\x03\x04":
            # ZIP 系列：可能是 EPUB、DOCX、CBZ
            try:
                import zipfile
                zf = zipfile.ZipFile(filepath)
                names = zf.namelist()
                if "mimetype" in names:
                    mt = zf.read("mimetype").decode("utf-8", errors="replace").strip()
                    if "epub" in mt:
                        return "epub"
                if "word/document.xml" in names:
                    return "docx"
                # 检查是否全是图片 → cbz
                image_exts = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
                if all(os.path.splitext(n)[1].lower() in image_exts or n.endswith("/") for n in names):
                    return "cbz"
                return "zip"
            except Exception:
                return "zip"
        if header[:8] == b"AT&TFORM":
            return "djvu"
        if header[60:68] == b"BOOKMOBI":
            return "mobi"

        return None

    def supported_formats(self) -> dict:
        """返回所有支持的格式及引擎信息"""
        formats = {}
        for handler in self._handlers:
            for ext in handler.extensions:
                if ext not in formats:
                    formats[ext] = []
                formats[ext].append(handler.engine_name)
        return formats
