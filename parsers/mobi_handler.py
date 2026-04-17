from __future__ import annotations
import os
import subprocess
import tempfile
import shutil

from parsers.base import BaseParser, ParseResult


class MobiHandler(BaseParser):
    """处理 MOBI 和 AZW3 格式"""
    extensions = ["mobi", "azw3"]
    engine_name = "mobi"

    def parse(self, filepath: str) -> ParseResult:
        # 方案1: mobi 库解压后解析
        result = self._try_mobi_lib(filepath)
        if result and result.success:
            return result

        # 方案2: PyMuPDF
        result = self._try_pymupdf(filepath)
        if result and result.success:
            return result

        # 方案3: Calibre
        result = self._try_calibre(filepath)
        if result and result.success:
            return result

        return ParseResult(text="", engine=self.engine_name,
                           error="无法解析 MOBI/AZW3，请安装: pip install mobi")

    def _try_mobi_lib(self, filepath: str) -> ParseResult | None:
        try:
            import mobi
            from bs4 import BeautifulSoup
            from parsers.epub_handler import EpubHandler
        except ImportError:
            return None
        try:
            tempdir, extracted_path = mobi.extract(filepath)
            try:
                if extracted_path.endswith(".epub"):
                    handler = EpubHandler()
                    result = handler.parse(extracted_path)
                    result.engine = self.engine_name
                    return result
                else:
                    with open(extracted_path, "r", encoding="utf-8", errors="replace") as f:
                        content = f.read()
                    soup = BeautifulSoup(content, "html.parser")
                    text = soup.get_text(separator="\n", strip=True)
                    if text.strip():
                        return ParseResult(text=text, engine=self.engine_name)
            finally:
                shutil.rmtree(tempdir, ignore_errors=True)
        except Exception:
            return None

    def _try_pymupdf(self, filepath: str) -> ParseResult | None:
        try:
            import fitz
            doc = fitz.open(filepath)
            pages = [page.get_text() for page in doc]
            doc.close()
            text = "\n".join(pages)
            if text.strip():
                return ParseResult(text=text, engine=self.engine_name)
        except Exception:
            return None

    def _try_calibre(self, filepath: str) -> ParseResult | None:
        ebook_convert = shutil.which("ebook-convert")
        if not ebook_convert:
            for p in ["/opt/homebrew/bin/ebook-convert",
                      "/Applications/calibre.app/Contents/MacOS/ebook-convert"]:
                if os.path.isfile(p):
                    ebook_convert = p
                    break
        if not ebook_convert:
            return None
        try:
            with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
                tmp_path = tmp.name
            result = subprocess.run(
                [ebook_convert, filepath, tmp_path],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode == 0:
                with open(tmp_path, "r", encoding="utf-8", errors="replace") as f:
                    return ParseResult(text=f.read(), engine=self.engine_name)
        except Exception:
            pass
        finally:
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        return None
