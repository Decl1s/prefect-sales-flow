"""Команда проверяется отдельным процессом — так видны её код выхода и stdout.

Изоляция настроек тоже проверяется в новом процессе: Prefect должен впервые
импортироваться после configure_local, иначе проверяется совсем другой порядок.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class CLITests(unittest.TestCase):
    def test_demo_and_overwrite_exit_code(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "orders.csv"
            command = [sys.executable, "-m", "sales_flow", "demo", "--output", str(target)]
            first = subprocess.run(command, cwd=ROOT, capture_output=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            self.assertIn("8 строк", first.stdout.decode("utf-8"))
            before = target.read_bytes()
            again = subprocess.run(command, cwd=ROOT, capture_output=True)
            self.assertEqual(again.returncode, 1)
            self.assertEqual(target.read_bytes(), before)

    def test_local_settings_use_none_and_separate_home(self):
        with tempfile.TemporaryDirectory() as folder:
            # Отдельный процесс: настройка делается до самого первого импорта Prefect.
            code = '''
import sys
from pathlib import Path
from sales_flow.local import configure_local, local_settings
configure_local(sys.argv[1])
with local_settings():
    from prefect.settings import get_current_settings
    s = get_current_settings()
    assert s.api.url is None and s.api.key is None
    assert not s.server.analytics_enabled
    assert s.home == Path(sys.argv[1])
    assert s.profiles_path.parent == s.home
print("isolated")
'''
            result = subprocess.run([sys.executable, "-c", code, folder], cwd=ROOT,
                                    env={**os.environ, "PREFECT_API_URL": "https://example.invalid/api",
                                         "PREFECT_API_KEY": "synthetic-only"}, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8"))
            self.assertIn(b"isolated", result.stdout)


if __name__ == "__main__":
    unittest.main()
