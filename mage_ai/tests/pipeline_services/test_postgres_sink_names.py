"""
The stream PostgreSQL sink (Rust) names columns as MageML's Python sink does: its reserved
words are MageML's, so the same stream makes the same table in MageML and in a service.
"""
import re
import unittest
from pathlib import Path

from mage_ai.io.constants import SQL_RESERVED_WORDS

RUST = (Path(__file__).resolve().parents[2] / 'pipeline_services' / 'service' / 'crates'
        / 'connector-postgres' / 'src' / 'reserved.rs')


class ReservedWordsTest(unittest.TestCase):
    def test_the_rust_list_is_mages(self):
        words = set(re.findall(r'^\s+"([^"]+)",$', RUST.read_text(), re.MULTILINE))
        self.assertEqual(words, set(SQL_RESERVED_WORDS))
