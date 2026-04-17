import os
import subprocess

from parsers.base import BaseParser, ParseResult


class DjvuHandler(BaseParser):
    extensions = ["djvu"]
    engine_name = "djvu"

    def parse(self, filepath: str) -> ParseResult:
        import shutil
        djvutxt_bin = shutil.which("djvutxt")
        if not djvutxt_bin:
            for candidate in ["/opt/homebrew/bin/djvutxt", "/usr/local/bin/djvutxt"]:
                if os.path.isfile(candidate):
                    djvutxt_bin = candidate
                    break
        if not djvutxt_bin:
            return ParseResult(text="", engine=self.engine_name,
                               error="djvutxt 未安装: brew install djvulibre")
        try:
            result = subprocess.run(
                [djvutxt_bin, filepath],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                return ParseResult(text="", engine=self.engine_name,
                                   error=f"djvutxt 失败: {result.stderr}")
            return ParseResult(text=result.stdout, engine=self.engine_name)
        except subprocess.TimeoutExpired:
            return ParseResult(text="", engine=self.engine_name, error="超时(>300秒)")
