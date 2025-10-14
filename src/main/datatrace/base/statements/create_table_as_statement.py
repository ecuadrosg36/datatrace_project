import datatrace.base.params as params
import datatrace.utils.utils as utils
from datatrace.base.statements.generator_statement import GeneratorStatement
from datatrace.base.statements.select_statement import SelectStatement


class CreateTableAsStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        """
        """
        self.raw_statement = raw_statement
        self.statement_id = statement_id
        self.components_dict = self.get_components()
        self.output_table = self.get_output_table()

    def get_components(self) -> dict():
        """
        """
        components_dict = utils.extract_regex_as_dict(params.STATEMENT_STRUCT_DICT["CREATE_TABLE_AS"],
                                                      self.raw_statement)
        return components_dict

    def get_output_table(self) -> str:
        """
        """
        output_table = self.components_dict.get("raw_output_table")
        return output_table

    def get_field_mapping(self) -> dict:
        """
        """
        field_mapping_dict = {"field_mapping_list": [],
                              "flag_mapping_success": 0,
                              "field_mapping_exception": None}
        try:
            if self.components_dict.get("raw_select_statement"):
                raw_select_statement = str(self.components_dict.get("raw_select_statement")).replace("&",
                                                                                                     "VAR_TO_DELETE_")
                field_mapping_dict["field_mapping_list"] = SelectStatement(raw_select_statement, self.statement_id,
                                                                           self.output_table).get_field_mapping()
                field_mapping_dict["flag_mapping_success"] = 1

            return field_mapping_dict
        except Exception as e:
            return {"field_mapping_list": [],
                    "flag_mapping_success": 0,
                    "field_mapping_exception": str(e)}