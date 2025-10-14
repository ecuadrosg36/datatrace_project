import unittest

from main.datatrace.base.statements.insert_into_select_statement import InsertIntoSelectStatement


class TestInsertIntoSelectStatement(unittest.TestCase):

    def setUp(self):
        self.raw_statement = """INSERT INTO MM_EPLEADTRADIC SELECT DISTINCT A.CODLOTEOFERTA,CLI.CODINTERNOCOMPUTACIONAL FROM UM_EP_PORTAFOLIO A INNER JOIN ( SELECT CODCLAVECIC, CODINTERNOCOMPUTACIONAL FROM ODS_V.MD_CLIENTEG94 WHERE FLGREGELIMINADO='N' AND TIPCLI<>'CD') CLI ON A.CODCLAVECIC=CLI.CODCLAVECIC WHERE NVL(A.FLGREGELIMINADO,'N')='N';"""
        self.statement_id = 1
        self.create_table_as_statement = InsertIntoSelectStatement(self.raw_statement, self.statement_id)

    def test_get_components(self):
        expected_components_dict = {
            "raw_output_table": "MM_EPLEADTRADIC",
            "raw_select_statement": """SELECT DISTINCT A.CODLOTEOFERTA,CLI.CODINTERNOCOMPUTACIONAL FROM UM_EP_PORTAFOLIO A INNER JOIN ( SELECT CODCLAVECIC, CODINTERNOCOMPUTACIONAL FROM ODS_V.MD_CLIENTEG94 WHERE FLGREGELIMINADO='N' AND TIPCLI<>'CD') CLI ON A.CODCLAVECIC=CLI.CODCLAVECIC WHERE NVL(A.FLGREGELIMINADO,'N')='N'""",
            'raw_output_table_struct': None,
            "flag_success": 1
        }
        result_components = self.create_table_as_statement.get_components()
        self.assertEqual(result_components, expected_components_dict)

    def test_get_output_table(self):
        output_table = self.create_table_as_statement.get_output_table()
        self.assertEqual(output_table, "MM_EPLEADTRADIC")

    def test_get_field_mapping(self):
        expected_field_mapping = [{'clause': 'select',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': 'CODCLAVECIC',
                                   'input_field': 'CODCLAVECIC',
                                   'input_table_alias': None,
                                   'input_table_name': 'ODS_V.MD_CLIENTEG94',
                                   'output_field_alias': 'CODCLAVECIC',
                                   'output_table': 'SUBQUERY_001_00'},
                                  {'clause': 'select',
                                   'expression_order_clause_level': 1,
                                   'expression_sql': 'CODINTERNOCOMPUTACIONAL',
                                   'input_field': 'CODINTERNOCOMPUTACIONAL',
                                   'input_table_alias': None,
                                   'input_table_name': 'ODS_V.MD_CLIENTEG94',
                                   'output_field_alias': 'CODINTERNOCOMPUTACIONAL',
                                   'output_table': 'SUBQUERY_001_00'},
                                  {'clause': 'where',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': "TIPCLI <> 'CD'",
                                   'input_field': 'TIPCLI',
                                   'input_table_alias': None,
                                   'input_table_name': 'ODS_V.MD_CLIENTEG94',
                                   'output_field_alias': None,
                                   'output_table': 'SUBQUERY_001_00'},
                                  {'clause': 'where',
                                   'expression_order_clause_level': 1,
                                   'expression_sql': "FLGREGELIMINADO = 'N'",
                                   'input_field': 'FLGREGELIMINADO',
                                   'input_table_alias': None,
                                   'input_table_name': 'ODS_V.MD_CLIENTEG94',
                                   'output_field_alias': None,
                                   'output_table': 'SUBQUERY_001_00'},
                                  {'clause': 'select',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': 'A.CODLOTEOFERTA',
                                   'input_field': 'CODLOTEOFERTA',
                                   'input_table_alias': 'A',
                                   'input_table_name': 'UM_EP_PORTAFOLIO',
                                   'output_field_alias': 'CODLOTEOFERTA',
                                   'output_table': 'MM_EPLEADTRADIC'},
                                  {'clause': 'select',
                                   'expression_order_clause_level': 1,
                                   'expression_sql': 'CLI.CODINTERNOCOMPUTACIONAL',
                                   'input_field': 'CODINTERNOCOMPUTACIONAL',
                                   'input_table_alias': 'CLI',
                                   'input_table_name': 'SUBQUERY_001_00',
                                   'output_field_alias': 'CODINTERNOCOMPUTACIONAL',
                                   'output_table': 'MM_EPLEADTRADIC'},
                                  {'clause': 'join_on',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': 'A.CODCLAVECIC = CLI.CODCLAVECIC',
                                   'input_field': 'CODCLAVECIC',
                                   'input_table_alias': 'A',
                                   'input_table_name': 'UM_EP_PORTAFOLIO',
                                   'output_field_alias': None,
                                   'output_table': 'MM_EPLEADTRADIC'},
                                  {'clause': 'join_on',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': 'A.CODCLAVECIC = CLI.CODCLAVECIC',
                                   'input_field': 'CODCLAVECIC',
                                   'input_table_alias': 'CLI',
                                   'input_table_name': 'SUBQUERY_001_00',
                                   'output_field_alias': None,
                                   'output_table': 'MM_EPLEADTRADIC'},
                                  {'clause': 'where',
                                   'expression_order_clause_level': 0,
                                   'expression_sql': "NVL(A.FLGREGELIMINADO, 'N') = 'N'",
                                   'input_field': 'FLGREGELIMINADO',
                                   'input_table_alias': 'A',
                                   'input_table_name': 'UM_EP_PORTAFOLIO',
                                   'output_field_alias': None,
                                   'output_table': 'MM_EPLEADTRADIC'}
                                  ]
        field_mapping = self.create_table_as_statement.get_field_mapping()
        self.assertEqual(field_mapping["field_mapping_list"], expected_field_mapping)
        self.assertEqual(field_mapping["flag_mapping_success"], 1)
        self.assertIsNone(field_mapping["field_mapping_exception"])


if __name__ == '__main__':
    unittest.main()
