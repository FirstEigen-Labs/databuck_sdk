import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("DATABUCK_SPARK_SDK_AUTO_DOWNLOAD", "0")

from databuck.agentic_rules import (
    _build_audit_prompt,
    _build_prompt,
    _get_reference_paths,
    _read_docx_text,
)


class ReferenceDocumentTests(unittest.TestCase):
    def test_docx_paragraphs_and_table_cells_reach_generation_and_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.docx"
            document_xml = (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/'
                'wordprocessingml/2006/main"><w:body>'
                '<w:p><w:r><w:t>Subscriber ID is required.</w:t></w:r></w:p>'
                '<w:tbl><w:tr><w:tc><w:p><w:r><w:t>Account ID is required.'
                '</w:t></w:r></w:p></w:tc></w:tr></w:tbl>'
                '</w:body></w:document>'
            )
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("word/document.xml", document_xml)

            self.assertEqual(_get_reference_paths({"pdf_paths": [str(path)]}), [str(path)])
            contents = _read_docx_text(str(path))
            self.assertEqual(contents, "Subscriber ID is required.\nAccount ID is required.")
            df = SimpleNamespace(schema=SimpleNamespace(fields=[]))
            prompt = _build_prompt(
                df, {"pdf_paths": [str(path)]}, 10, [str(path)],
                [{"name": path.name, "text": contents}],
            )
            self.assertIn("Account ID is required.", prompt)
            self.assertIn("policy.docx", prompt)
            self.assertIn("Subscriber ID is required.", _build_audit_prompt(prompt, []))

    def test_invalid_docx_has_clear_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.docx"
            path.write_text("not a Word document", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Could not read Word document"):
                _read_docx_text(str(path))


if __name__ == "__main__":
    unittest.main()
