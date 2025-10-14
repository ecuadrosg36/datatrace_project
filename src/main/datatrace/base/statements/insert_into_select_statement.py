import re
import datatrace.base.params as params
import datatrace.utils.utils as utils
from datatrace.base.statements.generator_statement import GeneratorStatement
from datatrace.base.statements.select_statement import SelectStatement

class InsertIntoSelectStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        self.raw_statement = raw_statement
        self.statement_id = statement_id
        self.components_dict = self.get_components()
        #print(self.components_dict)
        self.output_table = self.get_output_table()
        #print(self.output_table)

    def get_components(self) -> dict:
        # El bloque WITH es opcional y puede estar seguido de SELECT
        pattern = (
                r"INSERT\s+INTO\s+(?P<raw_output_table>[^\s(]+)(?:\s+NOLOGGING)?\s*(?P<raw_output_table_struct>\([^\)]*\))?\s*(WITH[\s\S]+?)?(SELECT[\s\S]+?);"
        )
        match = re.search(pattern, self.raw_statement, re.IGNORECASE)
        raw_output_table = None
        raw_output_table_struct = None
        with_block = None
        raw_select_statement = None
        if match:
            raw_output_table = match.group(1).strip() if match.group(1) else None
            raw_output_table_struct = match.group(2).strip() if match.group(2) else None
            with_block = match.group(3).strip() if match.group(3) else None
            raw_select_statement = match.group(4).strip() if match.group(4) else None
        return {
            "raw_output_table": raw_output_table,
            "raw_output_table_struct": raw_output_table_struct,
            "with_block": with_block,
            "raw_select_statement": raw_select_statement
        }

    def get_output_table(self) -> str:
        return self.components_dict.get("raw_output_table")

    def get_field_mapping(self) -> dict:
        """
        """
        field_mapping_dict = {
            "field_mapping_list": [],
            "flag_mapping_success": 0,
            "field_mapping_exception": None
        }
        insert_columns_list = []
        try:
            if self.components_dict.get("raw_output_table_struct"):
                raw_output_statement = str(self.components_dict.get("raw_output_table_struct")).strip(
                    " \t()\n").replace("\n", "").split(',')
                insert_columns_list = [column_output.strip() for column_output in raw_output_statement]

            # Siempre hay select, el WITH es opcional
            raw_select_statement = self.components_dict.get("raw_select_statement", "")
            with_block = self.components_dict.get("with_block")
            if with_block:
                # Asegúrate de que no haya doble SELECT
                if raw_select_statement.upper().startswith("SELECT"):
                    full_select = f"{with_block} {raw_select_statement}"
                else:
                    full_select = f"{with_block} SELECT {raw_select_statement}"
            else:
                full_select = raw_select_statement

            if full_select:
                full_select = full_select.replace("&", "VAR_TO_DELETE_")
                field_mapping_dict["field_mapping_list"] = SelectStatement(
                    full_select, self.statement_id, self.output_table
                ).get_field_mapping(insert_columns_list)
                field_mapping_dict["flag_mapping_success"] = 1

            return field_mapping_dict
        except Exception as e:
            return {
                "field_mapping_list": [],
                "flag_mapping_success": 0,
                "field_mapping_exception": str(e)
            }