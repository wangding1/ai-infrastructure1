"""pytest 根配置：保证 tests 包内模块可导入 support 等共享辅助。"""

import sys
from pathlib import Path

_TESTS_ROOT = Path(__file__).resolve().parent
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))
