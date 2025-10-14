from datatrace.base.statements.generator_statement import GeneratorStatement


class NotCoveredStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        """
        """
        self.raw_statement = raw_statement
        self.statement_id = statement_id

    def get_field_mapping(self):
        return {"field_mapping_list": [], "flag_mapping_success": 0, "field_mapping_exception": None}