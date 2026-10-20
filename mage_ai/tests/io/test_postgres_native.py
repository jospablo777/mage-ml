import unittest

from mage_ai.io.postgres_native import compatible, normalized_type


class TypeRulesTest(unittest.TestCase):
    def test_spellings_of_the_catalog_and_of_create_table_are_one(self):
        self.assertEqual(normalized_type('timestamp with time zone'), 'timestamptz')
        self.assertEqual(normalized_type('timestamp without time zone'), 'timestamp')
        self.assertEqual(normalized_type('time without time zone'), 'time')
        self.assertEqual(normalized_type('NUMERIC( 18 ,4 )'), 'numeric(18, 4)')
        self.assertEqual(normalized_type('int8[]'), 'bigint[]')
        self.assertEqual(normalized_type('double  precision'), 'double precision')

    def test_numeric_goes_into_any_numeric_and_text_into_varchar(self):
        self.assertTrue(compatible('numeric(25, 20)', 'numeric'))
        self.assertTrue(compatible('numeric(20, 0)', 'numeric(12, 3)'))
        self.assertTrue(compatible('text', 'character varying(10)'))
        self.assertTrue(compatible('numeric(10, 2)[]', 'numeric[]'))

    def test_types_that_differ_in_meaning_are_not_compatible(self):
        self.assertFalse(compatible('timestamp', 'timestamptz'))
        self.assertFalse(compatible('bigint', 'integer'))
        self.assertFalse(compatible('text', 'jsonb'))
        self.assertFalse(compatible('bigint[]', 'bigint'))
        self.assertFalse(compatible('double precision', 'numeric'))
