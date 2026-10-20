"""Проверки правил без сервера: хороший пример, границы и отказ без порчи файлов.

setUp создаёт отдельную временную папку для каждого теста; addCleanup удаляет её
даже при ошибке утверждения. assertEqual сверяет конкретный ожидаемый результат,
assertRaises требует отказа там, где опасно молча продолжать обработку.
Для нового правила сначала выпиши маленький пример и его правильную сумму.
"""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sales_flow.core import (DataError, PublishError, TransientPublishError, aggregate,
                             generate_demo, publish, quality_gate, read_source)


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "orders.csv"
        generate_demo(self.source)

    def summary(self):
        return aggregate(quality_gate(read_source(self.source)))

    def test_demo_counts_and_exact_money(self):
        summary = self.summary()
        self.assertEqual([summary[k] for k in ("input_rows", "unique_orders", "duplicates",
                                               "paid_orders", "cancelled_orders", "revenue_cents")],
                         [8, 6, 2, 5, 1, 159000])
        self.assertEqual(summary["daily"], [
            {"order_date": "2026-10-01", "orders": 2, "units": 5, "revenue_cents": 36000},
            {"order_date": "2026-10-02", "orders": 3, "units": 7, "revenue_cents": 123000}])

    def test_run_key_stable_and_sensitive_to_input_bytes(self):
        a = read_source(self.source)
        self.assertEqual(a, read_source(self.source))
        self.source.write_bytes(self.source.read_bytes().replace(b"\n", b"\r\n"))
        b = read_source(self.source)
        self.assertNotEqual(a["run_key"], b["run_key"])
        self.assertEqual(quality_gate(a)["orders"], quality_gate(b)["orders"])

    def test_generator_refuses_overwrite(self):
        before = self.source.read_bytes()
        with self.assertRaises(FileExistsError):
            generate_demo(self.source)
        self.assertEqual(before, self.source.read_bytes())

    def test_invalid_fields_are_rejected(self):
        original = self.source.read_text()
        cases = [("O0001", "bad"), ("2026-10-01", "2026-02-30"),
                 ("2026-10-01", "20261001"), ("book", "unknown"), ("paid", "pending"),
                 (",2,15000,", ",0,15000,"), (",2,15000,", ",2,-1,"),
                 (",2,15000,", ",2,1.5,"), (",2,15000,", ",+2,15000,"),
                 (",2,15000,", ",2,10000000,"), ("order_id", "id")]
        for old, new in cases:
            with self.subTest(new=new):
                self.source.write_text(original.replace(old, new), encoding="utf-8")
                with self.assertRaises(DataError):
                    self.summary()

    def test_conflicting_order_is_rejected(self):
        with self.source.open("a", encoding="utf-8") as stream:
            stream.write("O0001,2026-10-01,book,3,15000,paid\n")
        with self.assertRaisesRegex(DataError, "конфликт"):
            self.summary()

    def test_empty_and_ragged_csv(self):
        header = self.source.read_text().splitlines()[0]
        for content in ("", header + "\n", header + "\nO0001\n", header + '\n"unclosed'):
            with self.subTest(content=content[:20]):
                self.source.write_text(content, encoding="utf-8")
                with self.assertRaises(DataError):
                    self.summary()

    def test_input_limits_and_encoding(self):
        with patch("sales_flow.core.MAX_BYTES", 10):
            with self.assertRaises(DataError):
                read_source(self.source)
        with patch("sales_flow.core.MAX_ROWS", 7):
            with self.assertRaises(DataError):
                self.summary()
        self.source.write_bytes(b"\xff")
        with self.assertRaises(DataError):
            read_source(self.source)
        self.source.unlink()
        with self.assertRaises(DataError):
            read_source(self.source)

    def test_utf8_bom_accepted(self):
        self.source.write_bytes(b"\xef\xbb\xbf" + self.source.read_bytes())
        self.assertEqual(self.summary()["revenue_cents"], 159000)

    def test_cancelled_only_produces_zero(self):
        self.source.write_text(self.source.read_text().replace(",paid", ",cancelled"), encoding="utf-8")
        result = self.summary()
        self.assertEqual(result["daily"], [])
        self.assertEqual(result["revenue_cents"], 0)

    def test_publish_is_idempotent_and_manifest_matches(self):
        summary = self.summary()
        root = self.root / "output"
        first = publish(summary, root)
        target = root / first["run_key"]
        before = {p.name: p.read_bytes() for p in target.iterdir()}
        manifest = json.loads(before["manifest.json"])
        for name, digest in manifest["files"].items():
            self.assertEqual(digest, hashlib.sha256(before[name]).hexdigest())
        second = publish(summary, root, fail=True)
        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(before, {p.name: p.read_bytes() for p in target.iterdir()})
        self.assertEqual(len(list(root.iterdir())), 1)

    def test_transient_failure_leaves_no_partial_result(self):
        root = self.root / "output"
        with self.assertRaises(TransientPublishError):
            publish(self.summary(), root, fail=True)
        self.assertEqual(list(root.iterdir()), [])
        self.assertFalse(publish(self.summary(), root)["reused"])

    def test_corrupt_and_foreign_outputs_refused(self):
        summary = self.summary()
        root = self.root / "output"
        publish(summary, root)
        target = root / summary["run_key"]
        original = (target / "summary.json").read_bytes()
        (target / "summary.json").write_bytes(b"broken")
        with self.assertRaises(PublishError):
            publish(summary, root)
        self.assertEqual((target / "summary.json").read_bytes(), b"broken")
        (target / "summary.json").write_bytes(original)
        (target / "extra.txt").write_text("foreign")
        with self.assertRaises(PublishError):
            publish(summary, root)

    def test_write_error_cleans_staging(self):
        root = self.root / "output"
        with patch.object(Path, "write_bytes", side_effect=OSError("simulated")):
            with self.assertRaises(OSError):
                publish(self.summary(), root)
        self.assertEqual(list(root.iterdir()), [])

    def test_input_symlink_refused_when_supported(self):
        link = self.root / "link.csv"
        try:
            link.symlink_to(self.source)
        except (OSError, NotImplementedError):
            self.skipTest("Создание символических ссылок недоступно.")
        with self.assertRaises(PublishError):
            read_source(link)


if __name__ == "__main__":
    unittest.main()
