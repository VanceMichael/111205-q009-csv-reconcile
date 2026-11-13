"""测试引导：``python3 -I`` 下把仓库根注入 ``sys.path``。

隔离模式忽略 cwd 与 PYTHONPATH，但 unittest discover 会把本文件所在的
``tests/`` 目录置于导入路径中，因此这里可以可靠地按文件位置定位仓库根。
"""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
