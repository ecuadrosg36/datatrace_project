import unittest

from main.datatrace.base.statements.create_table_as_statement import CreateTableAsStatement


class TestCreateTableAsStatement(unittest.TestCase):

    def setUp(self):
        self.raw_statement = """CREATE TABLE STOCK_FFMM TABLESPACE D_cRM AS SELECT A.CODCLAVECIC, ROW_NUMBER() OVER(PARTITION BY A.CODCLAVECIC ORDER BY A.CODCLAVECIC) AS NUM FROM ODS_V.HD_SALDODIARIOMDC A WHERE CODPRODUCTO IN ('CPDADM');"""
        self.statement_id = 1
        self.create_table_as_statement = CreateTableAsStatement(self.raw_statement, self.statement_id)

    def test_get_components(self):
        expected_components_dict = {
            "raw_output_table": "STOCK_FFMM",
            "raw_select_statement": """SELECT A.CODCLAVECIC, ROW_NUMBER() OVER(PARTITION BY A.CODCLAVECIC ORDER BY A.CODCLAVECIC) AS NUM FROM ODS_V.HD_SALDODIARIOMDC A WHERE CODPRODUCTO IN ('CPDADM')""",
            "tablespace": "D_cRM",
            "flag_success": 1
        }
        result_components = self.create_table_as_statement.get_components()
        self.assertEqual(result_components, expected_components_dict)

    def test_get_output_table(self):
        output_table = self.create_table_as_statement.get_output_table()
        self.assertEqual(output_table, "STOCK_FFMM")

    def test_get_field_mapping(self):
        expected_field_mapping = [{'input_field': 'CODCLAVECIC', 'input_table_name': 'ODS_V.HD_SALDODIARIOMDC', 'input_table_alias': 'A', 'output_table': 'STOCK_FFMM', 'output_field_alias': 'CODCLAVECIC', 'expression_sql': 'A.CODCLAVECIC', 'clause': 'select', 'expression_order_clause_level': 0},
                                  {'input_field': 'CODCLAVECIC', 'input_table_name': 'ODS_V.HD_SALDODIARIOMDC', 'input_table_alias': 'A', 'output_table': 'STOCK_FFMM', 'output_field_alias': 'NUM', 'expression_sql': 'ROW_NUMBER() OVER (PARTITION BY A.CODCLAVECIC ORDER BY A.CODCLAVECIC) AS NUM', 'clause': 'select', 'expression_order_clause_level': 1},
                                  {'input_field': 'CODCLAVECIC', 'input_table_name': 'ODS_V.HD_SALDODIARIOMDC', 'input_table_alias': 'A', 'output_table': 'STOCK_FFMM', 'output_field_alias': 'NUM', 'expression_sql': 'ROW_NUMBER() OVER (PARTITION BY A.CODCLAVECIC ORDER BY A.CODCLAVECIC) AS NUM', 'clause': 'select', 'expression_order_clause_level': 1},
                                  {'input_field': 'CODPRODUCTO', 'input_table_name': 'ODS_V.HD_SALDODIARIOMDC', 'input_table_alias': None, 'output_table': 'STOCK_FFMM', 'output_field_alias': None, 'expression_sql': "CODPRODUCTO IN ('CPDADM')", 'clause': 'where', 'expression_order_clause_level': 0}
                                  ]
        field_mapping = self.create_table_as_statement.get_field_mapping()
        self.assertEqual(field_mapping["field_mapping_list"], expected_field_mapping)
        self.assertEqual(field_mapping["flag_mapping_success"], 1)
        self.assertIsNone(field_mapping["field_mapping_exception"])

if __name__ == '__main__':
    unittest.main()
