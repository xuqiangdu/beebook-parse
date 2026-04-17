from parsers.base import BaseParser, ParseResult


# 需要换行的块级元素(之后的文本另起一行)
_BLOCK_TAGS = {
    "p", "div", "li", "br", "hr", "tr", "td", "th",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "pre", "blockquote", "section", "article",
    "header", "footer", "nav", "aside",
    "dl", "dt", "dd", "figure", "figcaption",
}


def _extract_text(soup) -> str:
    """
    按 HTML 语义抽文本:
      - 块级元素(p/div/h1/li/...)之后加换行
      - 内联元素(em/span/a/code...)只在 text 之间加空格
    这样能避免 BeautifulSoup 原生 get_text(separator='\\n')
    把代码块的每个 token 都拆成单独一行的问题。
    """
    parts = []

    def walk(node):
        # 纯文本
        if isinstance(node, str):
            s = str(node).strip()
            if s:
                parts.append(s)
            return
        # 元素节点
        children = getattr(node, "children", None)
        if children is None:
            return
        name = getattr(node, "name", None)
        is_block = name in _BLOCK_TAGS
        # 块级元素前先换行(避免黏在上一段末尾)
        if is_block and parts and not parts[-1].endswith("\n"):
            parts.append("\n")
        for child in children:
            walk(child)
        # 块级元素后换行
        if is_block and parts and not parts[-1].endswith("\n"):
            parts.append("\n")

    walk(soup)
    # 内联 text 之间用空格连接(parts 中间除换行外都是文本片段)
    out = []
    for p in parts:
        if p == "\n":
            out.append("\n")
        else:
            # 如果上一段是纯文本,插一个空格分隔
            if out and out[-1] and out[-1][-1] not in (" ", "\n"):
                out.append(" ")
            out.append(p)
    text = "".join(out)
    # 多换行压缩
    import re
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 行内多余空白
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


class EpubHandler(BaseParser):
    extensions = ["epub"]
    engine_name = "epub"

    def parse(self, filepath: str) -> ParseResult:
        try:
            import ebooklib
            from ebooklib import epub
            from bs4 import BeautifulSoup
        except ImportError:
            return ParseResult(text="", engine=self.engine_name,
                               error="依赖未安装: pip install EbookLib beautifulsoup4")
        try:
            book = epub.read_epub(filepath, options={"ignore_ncx": True})
            texts = []
            # lxml 解析器是 C 实现,比 html.parser 快 3-5 倍
            for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
                soup = BeautifulSoup(item.get_content(), "lxml")
                text = _extract_text(soup)
                if text:
                    texts.append(text)
            return ParseResult(text="\n\n".join(texts), engine=self.engine_name)
        except Exception as e:
            return ParseResult(text="", engine=self.engine_name, error=str(e))
