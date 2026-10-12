"""What Google Sheets says when it finds no spreadsheet."""
from unittest import TestCase
from unittest.mock import patch

from mage_ai.io.google_sheets import GoogleSheets


class GoogleSheetsTest(TestCase):
    def test_a_missing_spreadsheet_names_what_was_asked_and_what_to_check(self):
        sheets = GoogleSheets.__new__(GoogleSheets)
        sheets.verbose = False

        with patch.object(GoogleSheets, 'fetch_sheet', return_value=None), \
                self.assertRaises(ValueError) as raised:
            sheets.fetch_worksheet(sheet_name='Orders 2026')

        self.assertEqual(str(raised.exception), (
            "Google Sheets found no spreadsheet for sheet_name 'Orders 2026'. Check the value, "
            'and share the spreadsheet with the service account of the credentials.'
        ))
