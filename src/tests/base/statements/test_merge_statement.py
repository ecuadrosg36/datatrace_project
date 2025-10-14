import unittest
from unittest.mock import patch, MagicMock

from sqlglot import exp

from main.datatrace.base.statements.merge_statement import MergeStatement
from main.datatrace.base.statements.select_statement import SelectStatement


class TestMergeStatement(unittest.TestCase):

    def setUp(self):
        self.raw_statement = """MERGE INTO MM_EPLEADTRADIC PARTITION (PMES_PIVOT) A USING (SELECT DISTINCT C.CODCLAVECIC FROM ODS_V.MD_CLIENTEG94MARCALPDP C WHERE TRIM(C.TIPCONSENTIMIENTOBCP) = 'SI') B ON (A.CODCLAVECIC=B.CODCLAVECIC) WHEN MATCHED THEN UPDATE SET A.FLGENVIADO = NULL;"""
        self.statement_id = 1
        self.merge_statement = MergeStatement(self.raw_statement, self.statement_id)

    def test_clean_statement(self):
        expected_clean_statement = """MERGE INTO MM_EPLEADTRADIC A USING (SELECT DISTINCT C.CODCLAVECIC FROM ODS_V.MD_CLIENTEG94MARCALPDP C WHERE TRIM(C.TIPCONSENTIMIENTOBCP) = 'SI') B ON (A.CODCLAVECIC=B.CODCLAVECIC) WHEN MATCHED THEN UPDATE SET A.FLGENVIADO = NULL;"""
        result = self.merge_statement.clean_statement()
        self.assertEqual(result, expected_clean_statement)

    def test_parse_statement(self):
        result_parse_statement = self.merge_statement.parse_statement()
        self.assertIsInstance(result_parse_statement, exp.Merge)

    def test_get_components(self):
        result = self.merge_statement.get_components()
        expected_type_dict = {
            "into": exp.Table,
            "using": exp.Subquery,
            "on": exp.Paren,
            "whens": list
        }
        self.assertListEqual(list(result.keys()), list(expected_type_dict.keys()))
        self.assertIsInstance(result["into"], expected_type_dict["into"])
        self.assertIsInstance(result["using"], expected_type_dict["using"])
        self.assertIsInstance(result["on"], expected_type_dict["on"])
        self.assertIsInstance(result["whens"], expected_type_dict["whens"])

    def test_get_conditions(self):
        expected = [exp.EQ(this=exp.Column(this=exp.Identifier(this="CODCLAVECIC", quoted=False),
                                          table=exp.Identifier(this="A", quoted=False)),
                          expression=exp.Column(this=exp.Identifier(this="CODCLAVECIC", quoted=False),
                                                table=exp.Identifier(this="B", quoted=False)))]
        result = self.merge_statement.get_conditions(self.merge_statement.components_dict["on"])
        self.assertEqual(result, expected)

    def test_get_parsed_table_cfg(self):
        parse_table = exp.Table(this=exp.Identifier(this="table_name"), db=exp.Identifier(this="db_name"), alias=exp.Identifier(this="alias_name"))
        result = self.merge_statement.get_parsed_table_cfg(parse_table, "into")
        expected = {
            "table_name": "table_name",
            "table_db": "db_name",
            "table_alias": "alias_name",
            "table_full_name": "db_name.table_name"
        }
        self.assertEqual(result, expected)

    def test_get_parsed_column_cfg(self):
        parse_column = exp.Column(this=exp.Identifier(this="column_name"), table=exp.Identifier(this="table_name"))
        self.merge_statement.alias_table_dict = {"TABLE_NAME": {"table_name": "table_name"}}
        result = self.merge_statement.get_parsed_column_cfg(parse_column)
        expected = {
            "column_name": "column_name",
            "column_table_alias": None,
            "column_table_name": "table_name"
        }
        self.assertEqual(result, expected)

    def test_get_alias_table_dict(self):
        self.merge_statement.components_dict = {
            "into": exp.Table(this=exp.Identifier(this="target_table"), alias=exp.Identifier(this="t")),
            "using": exp.Table(this=exp.Identifier(this="source_table"), alias=exp.Identifier(this="s"))
        }
        result = self.merge_statement.get_alias_table_dict()
        expected = {
            "T": {"table_name": "target_table", "table_type": "output"},
            "S": {"table_name": "source_table", "table_type": "input"}
        }
        self.assertEqual(result, expected)

    def test_get_output_table(self):
        self.merge_statement.components_dict = {
            "into": exp.Table(this=exp.Identifier(this="target_table"), db=exp.Identifier(this="db_name"))
        }
        result = self.merge_statement.get_output_table()
        self.assertEqual(result, "db_name.target_table")

    @patch.object(SelectStatement, 'get_field_mapping', return_value=[])
    def test_get_field_mapping(self, mock_get_field_mapping):
        self.merge_statement.components_dict = {
            "whens": [MagicMock(args={"then": exp.Update(expressions=[exp.EQ(this=exp.Column(this="col1"), expression=exp.Column(this="col2"))])})],
            "on": exp.And(this=exp.Column(this="col1"), expression=exp.Column(this="col2"))
        }
        self.merge_statement.alias_table_dict = {"TABLE_NAME": {"table_name": "table_name"}}
        result = self.merge_statement.get_field_mapping()
        self.assertEqual(result["flag_mapping_success"], 1)
        self.assertIsNone(result["field_mapping_exception"])

if __name__ == '__main__':
    unittest.main()