"""
Schema and table names in PostgreSQL statements: plain names stay unquoted and fold to
lower case, as SQL that writes them unquoted reads them; other names are quoted as they
are.
"""
from unittest import TestCase

from mage_ai.io.postgres import identifier, stored_name


class IdentifierTest(TestCase):
    def test_plain_names_fold_and_others_are_quoted(self):
        for name, written, stored in [
            ('sales', 'sales', 'sales'),
            ('MyTable', 'MyTable', 'mytable'),
            ('ñandú', 'ñandú', 'ñandú'),
            ('t$1', 't$1', 't$1'),
            ('with space', '"with space"', 'with space'),
            ('2024_sales', '"2024_sales"', '2024_sales'),
            ('a-b', '"a-b"', 'a-b'),
            ('say "hi"', '"say ""hi"""', 'say "hi"'),
            ('"Quoted"', '"Quoted"', 'Quoted'),
        ]:
            with self.subTest(name):
                self.assertEqual(identifier(name), written)
                self.assertEqual(stored_name(name), stored)
