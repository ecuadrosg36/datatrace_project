from sqlglot import parse_one, exp

from datatrace.base.statements.generator_statement import GeneratorStatement
from datatrace.base.statements.select_statement import SelectStatement
import datatrace.utils.utils as utils


class MergeStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        """
        """
        self.select_raw_query = raw_statement
        self.statement_id = statement_id
        self.field_mapping_exception = None
        self.cleaned_statement = self.clean_statement()
        self.statement = self.parse_statement()
        self.components_dict = self.get_components()
        self.output_table = self.get_output_table()
        self.prev_field_mapping_list = []
        self.alias_table_dict = self.get_alias_table_dict()

    def clean_statement(self):
        """
        """
        sub_list = [{"pattern_to_sub": r"PARTITION\s*\(.*?\)\s*", "value_to_sub": ""}]
        clean_statement = utils.sub_patterns(self.select_raw_query, sub_list)
        return clean_statement

    def parse_statement(self):
        """
        """
        try:
            if self.cleaned_statement:
                return parse_one(self.cleaned_statement, read="oracle")
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
            parsed_dict = self.statement.args
            statement_components_dict = {
                "into": parsed_dict.get("this"),
                "using": parsed_dict.get("using"),
                "on": parsed_dict.get("on"),
                "whens": parsed_dict.get("whens").args.get("expressions") if parsed_dict.get("whens") else parsed_dict.get("expressions") if parsed_dict.get("expressions") else []
            }

        return statement_components_dict

    def get_conditions(self, parsed, condition_list: list = []) -> list:
        """
        """
        if isinstance(parsed, exp.Paren):
            parsed = parsed.this
        if (isinstance(parsed, exp.And) or isinstance(parsed, exp.Or)):
            condition_list.append(parsed.expression)
            if not (isinstance(parsed.this, exp.And) or isinstance(parsed.this, exp.Or)):
                condition_list.append(parsed.this)
                return condition_list
            else:
                return self.get_conditions(parsed.this, condition_list)
        elif isinstance(parsed, exp.EQ):
            condition_list.append(parsed)
            return condition_list
        else:
            return []

    def get_parsed_table_cfg(self, parse_table: exp.Table, component_type: str) -> dict():
        """
        """
        table_cfg = {"table_name": "", "table_alias": "", "table_full_name": "", "table_db": "", "table_object_type": ""}
        if isinstance(parse_table, exp.Table):
            table_cfg = {"table_name": parse_table.args.get("this").name if parse_table.args.get("this") else None,
                         "table_db": parse_table.args.get("db").name if parse_table.args.get("db") else None,
                         "table_alias": parse_table.args.get("alias").name if parse_table.args.get("alias") else None,
                         "table_object_type": "TABLA"
                         }
            table_cfg["table_full_name"] = (
                f"{table_cfg['table_db']}.{table_cfg['table_name']}" if table_cfg['table_db'] else table_cfg[
                    'table_name'])
        elif isinstance(parse_table, exp.Subquery):
            table_cfg = {"table_name": f"MERGE_{component_type.upper()}_SUBQUERY_{str(self.statement_id).zfill(3)}",
                         "table_db": "",
                         "table_alias": parse_table.args.get("alias").name if parse_table.args.get("alias") else None,
                         "table_full_name": f"MERGE_{component_type.upper()}_SUBQUERY_{str(self.statement_id).zfill(3)}",
                         "table_object_type": "SUBCONSULTA"
                         }
            self.prev_field_mapping_list += SelectStatement(parse_table.this.sql(dialect="oracle"),
                                                           statement_id=self.statement_id,
                                                           output_table=f"MERGE_{component_type.upper()}_SUBQUERY_{str(self.statement_id).zfill(3)}").get_field_mapping()
        return table_cfg

    def get_parsed_column_cfg(self, parse_column: exp.Column, source_type: str = None) -> dict():
        """
        """
        column_cfg = {"column_name": "", "column_table_alias": "", "column_table_name": "", "column_table_db": "", "column_table_object_type": ""}
        if isinstance(parse_column, exp.Column):
            column_cfg["column_name"] = parse_column.args.get("this").name if parse_column.args.get("this") else None
            column_table = parse_column.args.get("table").name if parse_column.args.get("table") else None
            if column_table:
                if column_table.upper() in self.alias_table_dict.keys():
                    column_cfg["column_table_alias"] = column_table
                    column_cfg["column_table_name"] = self.alias_table_dict[column_table.upper()]['table_name']
                    column_cfg["column_table_db"] = self.alias_table_dict[column_table.upper()]['table_db']
                    column_cfg["column_table_object_type"] = self.alias_table_dict[column_table.upper()]['table_object_type']
                elif column_table in [d["table_name"] for d in self.alias_table_dict.values()]:
                    column_table_cfg = next(iter([d for d in self.alias_table_dict.values() if column_table == d["table_name"]]))
                    column_cfg["column_table_alias"] = None
                    column_cfg["column_table_name"] = column_table
                    column_cfg["column_table_db"] = column_table_cfg["table_db"]
                    column_cfg["column_table_object_type"] = column_table_cfg["table_object_type"]
            elif source_type == "output" :
                column_table_cfg = next(iter([d for d in self.alias_table_dict.values() if d["table_type"] == "output"]))
                column_cfg["column_table_alias"] = None
                column_cfg["column_table_name"] = column_table_cfg["table_name"]
                column_cfg["column_table_db"] = column_table_cfg["table_db"]
                column_cfg["column_table_object_type"] = column_table_cfg["table_object_type"]

            elif source_type == "input":
                column_table_cfg_input_list = [d for d in self.alias_table_dict.values() if d["table_type"] == "input"]
                if len(column_table_cfg_input_list) == 1:
                    column_table_cfg = column_table_cfg_input_list[0]
                    column_cfg["column_table_alias"] = None
                    column_cfg["column_table_name"] = column_table_cfg["table_name"]
                    column_cfg["column_table_db"] = column_table_cfg["table_db"]
                    column_cfg["column_table_object_type"] = column_table_cfg["table_object_type"]
        return column_cfg

    def get_alias_table_dict(self) -> dict():
        """
        """
        table_dict = dict()
        into_component = self.components_dict.get("into")
        output_table_cfg = self.get_parsed_table_cfg(into_component, "into")
        if output_table_cfg.get("table_alias"):
            table_dict[output_table_cfg.get("table_alias").upper()] = {"table_name": output_table_cfg.get("table_name"),
                                                                       "table_db": output_table_cfg.get("table_db"),
                                                                       "table_object_type": output_table_cfg.get("table_object_type"),
                                                                       "table_type": "output"}

        using_component = self.components_dict.get("using")
        input_table_cfg = self.get_parsed_table_cfg(using_component, "using")
        if input_table_cfg.get("table_alias"):
            table_dict[input_table_cfg.get("table_alias").upper()] = {"table_name": input_table_cfg.get("table_name"),
                                                                      "table_db": input_table_cfg.get("table_db"),
                                                                      "table_object_type": input_table_cfg.get("table_object_type"),
                                                                      "table_type": "input"}
        return table_dict

    def get_output_table(self) -> str:
        """
        """
        output_table = ""
        into_component = self.components_dict.get("into")
        if isinstance(into_component, exp.Table):
            output_table_dict = {
                "output_table_name": into_component.args.get("this").name if into_component.args.get("this") else None,
                "output_table_db": into_component.args.get("db").name if into_component.args.get("db") else None,
                "output_table_alias": into_component.args.get("alias").name if into_component.args.get(
                    "alias") else None
                }
            output_table = f"{output_table_dict['output_table_db']}.{output_table_dict['output_table_name']}" if \
            output_table_dict['output_table_db'] else output_table_dict['output_table_name']

        return output_table

    def get_field_mapping(self) -> dict:
        """
        """
        try:
            field_mapping_list = self.prev_field_mapping_list
            when_expr_list = self.components_dict["whens"]
            for when_expr in when_expr_list:
                then_expr = when_expr.args.get("then")
                if isinstance(then_expr, exp.Update):
                    eq_expr_list = then_expr.args.get("expressions") if then_expr.args.get("expressions") else []
                    for eq_expr in eq_expr_list:
                        output_column_expr = eq_expr.args.get("this").find(exp.Column) if eq_expr.args.get("this") else None
                        output_cfg = self.get_parsed_column_cfg(output_column_expr, source_type="output")
                        input_cfg_list = [self.get_parsed_column_cfg(expr_column, source_type="input") for expr_column in
                                          eq_expr.args.get("expression").find_all(exp.Column) if
                                          eq_expr.args.get("expression")]
                        if len(input_cfg_list) > 0:
                            for input_cfg in input_cfg_list:
                                mapping_dict = {
                                    "input_object_type": input_cfg["column_table_object_type"],
                                    "input_schema_path": input_cfg["column_table_db"],
                                    "input_table_name": input_cfg["column_table_name"],
                                    "input_table_alias": input_cfg["column_table_alias"],
                                    "input_field": input_cfg["column_name"],
                                    "output_object_type": output_cfg["column_table_object_type"],
                                    "output_schema_path": output_cfg["column_table_db"],
                                    "output_table": output_cfg["column_table_name"],
                                    "output_field_alias": output_cfg["column_name"],
                                    "expression_sql": eq_expr.sql(dialect="oracle"),
                                    "clause": "merge_update"
                                }
                                field_mapping_list.append(mapping_dict)
                        else:
                            mapping_dict = {
                                "input_object_type": "",
                                "input_schema_path": "",
                                "input_table_name": "",
                                "input_table_alias": "",
                                "input_field": "",
                                "output_object_type": output_cfg["column_table_object_type"],
                                "output_schema_path": output_cfg["column_table_db"],
                                "output_table": output_cfg["column_table_name"],
                                "output_field_alias": output_cfg["column_name"],
                                "expression_sql": eq_expr.sql(dialect="oracle"),
                                "clause": "merge_update"
                            }
                            field_mapping_list.append(mapping_dict)

            on_expr_list = self.get_conditions(self.components_dict["on"], [])
            for on_expr in on_expr_list:
                column_cfg_list = [self.get_parsed_column_cfg(c) for c in on_expr.find_all(exp.Column)]
                output_cfg_list = [d for d in column_cfg_list if
                                   d["column_table_alias"].upper() in [k.upper() for k, v in self.alias_table_dict.items() if
                                                                       v["table_type"] == "output"]]
                input_cfg_list = [d for d in column_cfg_list if
                                  d["column_table_alias"].upper() not in [k.upper() for k, v in self.alias_table_dict.items()
                                                                          if v["table_type"] == "output"]]
                output_cfg = output_cfg_list[0] if len(output_cfg_list) > 0 else {"column_table_name": "",
                                                                                  "column_name": "", "column_table_db": "",
                                                                                  "column_table_object_type": ""}
                for input_cfg in input_cfg_list:
                    mapping_dict = {
                        "input_object_type": input_cfg["column_table_object_type"],
                        "input_schema_path": input_cfg["column_table_db"],
                        "input_table_name": input_cfg["column_table_name"],
                        "input_table_alias": input_cfg["column_table_alias"],
                        "input_field": input_cfg["column_name"],
                        "output_object_type": output_cfg["column_table_object_type"],
                        "output_schema_path": output_cfg["column_table_db"],
                        "output_table": output_cfg["column_table_name"],
                        "output_field_alias": output_cfg["column_name"],
                        "expression_sql": on_expr.sql(dialect="oracle"),
                        "clause": "merge_on"
                    }
                    field_mapping_list.append(mapping_dict)
            return {"field_mapping_list": field_mapping_list, "flag_mapping_success": 1, "field_mapping_exception": None}
        except Exception as e:
            return {"field_mapping_list": [], "flag_mapping_success": 0, "field_mapping_exception": str(e)}