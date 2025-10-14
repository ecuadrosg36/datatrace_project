import unittest
from unittest.mock import patch

import pandas as pd
import sqlglot.expressions as exp
from sqlglot import parse_one

from main.datatrace.base.statements.select_statement import BasicSelectStatement


class TestBasicSelectStatement(unittest.TestCase):

    def setUp(self):
        self.raw_statement = "SELECT col1, col2 FROM table1 WHERE col1 = 'value1' AND col2 = 'value2'"
        self.basic_select_statement = BasicSelectStatement(self.raw_statement)

    def test_parse_statement(self):
        result = self.basic_select_statement.parse_statement()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, exp.Select)

    def test_get_tables_from_raw_select(self):
        select_statement = parse_one(self.raw_statement, read="oracle")
        result = self.basic_select_statement.get_tables_from_raw_select(select_statement)
        self.assertIsInstance(result, list)

    def test_get_conditions(self):
        parsed = self.basic_select_statement.components_dict.get("where")
        result = self.basic_select_statement.get_conditions(parsed, [])
        self.assertIsInstance(result, list)

    def test_get_components(self):
        result = self.basic_select_statement.components_dict
        self.assertIsInstance(result, dict)

    def test_get_expressions_from_clause(self):
        statement = parse_one(self.raw_statement, read="oracle")
        result = self.basic_select_statement.get_expressions_from_clause(statement)
        self.assertIsInstance(result, list)

    def test_get_string_or_null(self):
        result = self.basic_select_statement.get_string_or_null("  ")
        self.assertIsNone(result)
        result = self.basic_select_statement.get_string_or_null("value")
        self.assertEqual(result, "value")

    def test_get_table_name_from_alias(self):
        self.basic_select_statement.table_dict = {"alias": {"table_full_name": "db.table"}}
        result = self.basic_select_statement.get_table_name_from_alias("alias", "field")
        self.assertEqual(result, "db.table")

    def test_get_alias_from_expression(self):
        expression = exp.Column(this="col1", alias="alias1")
        result = self.basic_select_statement.get_alias_from_expression(expression)
        self.assertEqual(result, "alias1")

    def test_get_fields_from_expression(self):
        expression = exp.Column(this="col1", table="table1")
        result = self.basic_select_statement.get_fields_from_expression(expression)
        self.assertIsInstance(result, list)

    @patch('pandas.DataFrame')
    def test_get_field_mapping(self, mock_df):
        mock_df.return_value = pd.DataFrame()
        result = self.basic_select_statement.get_field_mapping()
        self.assertIsInstance(result, dict)

if __name__ == '__main__':
    unittest.main()
