import unittest
from unittest.mock import patch  # For mocking functions
from fund_analysis import connect_to_db, split_filename
from modules import fund_count, fund_record_count

class TestScriptFunctions(unittest.TestCase):

    @patch('fund_analysis.engine')  # Mocking create_engine
    def test_connect_to_db(self, mock_engine):
        # Mock successful connection
        mock_engine.return_value = 'engine'
        user = 'user'
        password = 'password'
        host = 'localhost'
        port = 5432
        dbname = 'dbname'

        engine = connect_to_db(user, password, host, port, dbname)
        self.assertEqual(engine, 'engine')

        # Mock connection error (FileNotFoundError)
        mock_engine.side_effect = FileNotFoundError('config.yaml not found')
        with self.assertRaises(FileNotFoundError):
            connect_to_db(user, password, host, port, dbname)

    def test_split_filename(self):
        # Test splitting filename with format: filename.YYYYMMDD.csv
        test_filename = 'Applebead.28-02-2023 breakdown.csv'
        expected_output = ('Applebead', '28-02-2023', '2023-02-28')
        self.assertEqual(split_filename(test_filename), expected_output)

    def test_fund_names(self):
        expected_output = ['Virtous', 'mend-report Wallington', 'Belaware', 'Report-of-Gohen', 'Applebead',
                          'TT_monthly_Trustmind', 'Leeder', 'Magnum', 'rpt-Catalysm', 'Fund Whitestone']
        self.assertEqual(fund_count(), expected_output)

    def test_fund_record_count(self):
        expected_output = 10309  # number of records
        self.assertEqual(fund_record_count(), expected_output)


if __name__ == '__main__':
    unittest.main()

