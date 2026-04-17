"""txt / docx / cbr / cbz 等简单格式的 Handler"""

import os
import zipfile

from parsers.base import BaseParser, ParseResult


class TxtHandler(BaseParser):
    extensions = ["txt"]
    engine_name = "txt"

    def parse(self, filepath: str) -> ParseResult:
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                return ParseResult(text=f.read(), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))


class DocxHandler(BaseParser):
    extensions = ["docx"]
    engine_name = "docx"

    def parse(self, filepath: str) -> ParseResult:
        try:
            from lxml import etree
        except ImportError:
            return ParseResult(text="", engine=self.engine_name,
                               error="依赖未安装: pip install lxml")
        try:
            zf = zipfile.ZipFile(filepath)
            xml_content = zf.read("word/document.xml")
            tree = etree.fromstring(xml_content)
            ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            texts = []
            for para in tree.iter(f"{{{ns}}}p"):
                para_text = ""
                for run in para.iter(f"{{{ns}}}t"):
                    if run.text:
                        para_text += run.text
                if para_text:
                    texts.append(para_text)
            zf.close()
            return ParseResult(text="\n".join(texts), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))


class CbzHandler(BaseParser):
    extensions = ["cbz"]
    engine_name = "cbz"

    def parse(self, filepath: str) -> ParseResult:
        try:
            zf = zipfile.ZipFile(filepath)
            texts = []
            for name in sorted(zf.namelist()):
                if name.lower().endswith((".txt", ".html", ".htm", ".xml")):
                    data = zf.read(name)
                    texts.append(data.decode("utf-8", errors="replace"))
            zf.close()
            return ParseResult(text="\n\n".join(texts), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))


class CbrHandler(BaseParser):
    extensions = ["cbr"]
    engine_name = "cbr"

    def parse(self, filepath: str) -> ParseResult:
        try:
            import rarfile
        except ImportError:
            return ParseResult(text="", engine=self.engine_name,
                               error="依赖未安装: pip install rarfile")
        try:
            rf = rarfile.RarFile(filepath)
            texts = []
            for name in sorted(rf.namelist()):
                if name.lower().endswith((".txt", ".html", ".htm", ".xml")):
                    data = rf.read(name)
                    texts.append(data.decode("utf-8", errors="replace"))
            rf.close()
            return ParseResult(text="\n\n".join(texts), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))
