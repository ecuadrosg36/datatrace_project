from datatrace.base.statements.generator_statement import GeneratorStatement
from datatrace.base.statements.select_statement import SelectStatement
from sqlglot import parse_one, exp

class UpdateStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        """
        Initialize the UpdateStatement with the raw SQL statement and an optional statement ID.
        """
        self.raw_statement = raw_statement
        self.field_mapping_exception = None
        self.statement_id = statement_id
        self.statement = self.parse_statement()
        self.statement_components_dict = self.get_components()
        self.output_table = self.get_output_table()

    def parse_statement(self):
        """
        """
        try:
            if self.raw_statement:
                return parse_one(self.raw_statement, dialect="oracle")
            else:
                self.field_mapping_exception = "Source raw query is null."
                return None
        except Exception as e:
            self.field_mapping_exception = str(e)
            return None
        
    def get_components(self) -> dict():
        """
        """
        statement_components_dict = dict()
        if self.statement:
            parsed_dict = self.statement.find(exp.Update)
            statement_components_dict = {
                "output_table": parsed_dict.args.get("this"),
                "output_field_alias": parsed_dict.this.alias if parsed_dict.this.alias else None,
                "set_clause": parsed_dict.expressions,
                "where_clause": parsed_dict.args.get("where")
            }

        return statement_components_dict

    def get_output_table(self) -> str:
        """
        """
        output_table_component = self.statement_components_dict.get("output_table")
        if output_table_component:
            if isinstance(output_table_component, exp.Table):
                return output_table_component.name
            elif isinstance(output_table_component, exp.Subquery):
                return output_table_component.sql(dialect="oracle")

        return output_table_component

    def get_output_table_alias(self) -> str:
        return self.statement_components_dict.get("output_table_alias")

    def get_field_mapping(self) -> dict:
        """
        Get the field mapping from the UPDATE statement.
        """
        field_mapping_dict = {
            "field_mapping_list": [],
            "flag_mapping_success": 0,
            "field_mapping_exception": None
        }
        try:
            if self.statement_components_dict.get("set_clause"):
                set_clause = self.statement_components_dict.get("set_clause")
                field_mapping_list = self.parse_set_clause(set_clause)
                field_mapping_dict["field_mapping_list"] = field_mapping_list
                field_mapping_dict["flag_mapping_success"] = 1

            return field_mapping_dict
        except Exception as e:
            return {
                "field_mapping_list": [],
                "flag_mapping_success": 0,
                "field_mapping_exception": str(e)
            }

    def parse_set_clause(self, set_clause: str) -> list:
        """
        Parse the SET clause to extract field mappings.
        """
        field_mapping_list = []
        for clause in set_clause:
            field_mapping_list.append({
                "input_object_type": "TABLA",
                "input_schema_path": None,
                "input_table_name": None,
                "input_table_alias": None,
                "input_field": None,
                "filter_expression_sql": f"{self.statement_components_dict.get('where_clause').this}" if self.statement_components_dict.get("where_clause") else None,
                "output_object_type": "TABLA",
                "output_schema_path": None,
                "output_table": self.output_table,
                "output_field_alias": clause.this.name,
                "expression_sql": f"{clause.sql(dialect='oracle')}",
                "clause": "update_set",
            })

        for s in self.statement.find_all(exp.Subquery):
            lista = SelectStatement(str(s),self.statement_id,self.output_table).get_field_mapping()
            for output in lista:
                field_mapping_list.append({
                    "input_object_type": output["output_object_type"] if output["output_table"].startswith("SUBQUERY") else output["input_object_type"],
                    "input_schema_path": output["input_schema_path"],
                    "input_table_name": output["input_table_name"],
                    "input_table_alias": output["input_table_alias"],
                    "input_field": output["input_field"],
                    "output_object_type": output["output_object_type"],
                    "output_schema_path": output["output_schema_path"],
                    "output_table": output["output_table"],
                    "output_field_alias": output["output_field_alias"],
                    "output_subquery_sql": output["output_subquery_sql"],
                    "expression_sql": output["expression_sql"] if (output["clause"]=="where" and output["expression_sql"].find(output["input_field"])!=-1)  else (output["filter_expression_sql"].replace("WHERE ","") if (output["clause"]=="where") else None),
                    "clause": output["clause"]
                })

        return field_mapping_list