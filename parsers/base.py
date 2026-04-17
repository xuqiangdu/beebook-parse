"""
解析器基类 - 策略模式

所有格式的 Handler 都继承 BaseParser，实现 parse() 方法。
ParserFactory 根据文件扩展名自动选择对应的 Handler。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ParseResult:
    """解析结果"""
    text: str
    engine: str
    error: str = ""

    @property
    def success(self) -> bool:
        return self.error == ""


class BaseParser(ABC):
    """解析器基类"""

    # 子类声明自己支持的扩展名列表
    extensions: list[str] = []
    # 引擎名称
    engine_name: str = ""

    @abstractmethod
    def parse(self, filepath: str) -> ParseResult:
        """解析文件，返回 ParseResult"""
        ...

    def can_handle(self, ext: str) -> bool:
        return ext.lower() in self.extensions
