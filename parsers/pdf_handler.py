import os
import subprocess
import tempfile

from parsers.base import BaseParser, ParseResult


def _find_bin(name, extra_paths=None):
    import shutil
    path = shutil.which(name)
    if path:
        return path
    for candidate in (extra_paths or []):
        if os.path.isfile(candidate):
            return candidate
    return None


class PdfTextHandler(BaseParser):
    """pdftotext (poppler) - 最快的 PDF 解析"""
    extensions = ["pdf"]
    engine_name = "pdftotext"

    def parse(self, filepath: str) -> ParseResult:
        pdftotext_bin = _find_bin("pdftotext", [
            "/opt/homebrew/bin/pdftotext",
            "/usr/local/bin/pdftotext",
        ])
        if not pdftotext_bin:
            return ParseResult(text="", engine=self.engine_name,
                               error="pdftotext 未安装: brew install poppler")
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            result = subprocess.run(
                [pdftotext_bin, "-layout", filepath, tmp_path],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                return ParseResult(text="", engine=self.engine_name,
                                   error=f"pdftotext 失败: {result.stderr}")
            with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
                return ParseResult(text=f.read(), engine=self.engine_name)
        except subprocess.TimeoutExpired:
            return ParseResult(text="", engine=self.engine_name, error="超时(>300秒)")
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class PyMuPdfHandler(BaseParser):
    """PyMuPDF (fitz) - 默认 PDF 解析"""
    extensions = ["pdf"]
    engine_name = "pymupdf"

    def parse(self, filepath: str) -> ParseResult:
        try:
            import fitz
        except ImportError:
            return ParseResult(text="", engine=self.engine_name,
                               error="PyMuPDF 未安装: pip install PyMuPDF")
        try:
            doc = fitz.open(filepath)
            page_count = len(doc)
            pages = [page.get_text() for page in doc]
            text = "\n".join(pages)
            doc.close()

            # 扫描版 PDF（图片无文字层）直接返回错误
            if len(text.strip()) < 100 and page_count > 3:
                return ParseResult(text="", engine=self.engine_name,
                                   error="扫描版 PDF（图片无文字层），无法提取文本，需要 OCR 处理")

            return ParseResult(text=text, engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))
