from parsers.base import BaseParser, ParseResult


class Fb2Handler(BaseParser):
    extensions = ["fb2"]
    engine_name = "fb2"

    def parse(self, filepath: str) -> ParseResult:
        try:
            from lxml import etree
        except ImportError:
            return ParseResult(text="", engine=self.engine_name,
                               error="依赖未安装: pip install lxml")
        try:
            tree = etree.parse(filepath)
            ns = {"fb": "http://www.gribuser.ru/xml/fictionbook/2.0"}
            body = tree.xpath("//fb:body", namespaces=ns)
            if not body:
                body = tree.xpath("//body")
            texts = []
            for b in body:
                for elem in b.iter():
                    if elem.text and elem.text.strip():
                        texts.append(elem.text.strip())
                    if elem.tail and elem.tail.strip():
                        texts.append(elem.tail.strip())
            return ParseResult(text="\n".join(texts), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))
