import re

import pandas as pd
from sqlglot import parse_one, exp
import datatrace.utils.utils as utils


class BasicSelectStatement:

    def __init__(self, raw_statement, output_table="__DEFAULT__", nested_query_map: dict = None):
        self.raw_statement = raw_statement
        self.nested_query_map = nested_query_map
        self.statement = self.parse_statement()
        self.components_dict = self.get_components()
        self.output_table = self.get_table_name(output_table)
        self.output_schema = self.get_schema_name(output_table)
        self.output_table_type = self.get_table_type(output_table)
        self.table_list = self.get_tables_from_raw_select(self.statement)
        self.table_dict = {d["table_alias"]: {"table_name": d["table_name"], "table_db": d["table_db"],
                                              "table_full_name": d["table_full_name"]} for d in self.table_list if
                           d["table_alias"]}

    def parse_statement(self):
        """
        """
        try:
            if self.raw_statement:
                return parse_one(self.raw_statement, read="oracle")
            else:
                self.field_mapping_exception = "Source raw query is null."
                return None
        except Exception as e:
            self.field_mapping_exception = str(e)
            return None

    @staticmethod
    def get_table_name(table_name):
        if table_name:
            if table_name.split(".")[-1].lower() in ["txt"]:
                return table_name.split("\\")[-1] if "\\" in table_name else table_name
            else:
                return table_name.split(".")[-1] if "." in table_name else table_name
        else:
            return None

    @staticmethod
    def get_schema_name(table_name):
        if table_name:
            if table_name.split(".")[-1].lower() in ["txt"]:
                return table_name.split("\\")[0] if "\\" in table_name else table_name
            else:
                return table_name.split(".")[0] if "." in table_name else None
        else:
            return None

    @staticmethod
    def get_table_type(table_name: str):
        if table_name:
            if table_name.startswith("SUBQUERY"):
                return "SUBCONSULTA"
            else:
                return "TABLA"
        else:
            return None

    def get_tables_from_raw_select(self, select_statement) -> list:
        """
        """
        if select_statement:
            table_list_nes_query = []
            for s in list(select_statement.find_all(exp.Subquery)):
                if isinstance(s.this, exp.Table):
                    table_dict_tmp = {"table_alias": None if s.alias == "" else s.alias, "table_name": s.this.name,
                                      "table_db": self.get_string_or_null(s.this.db)}
                    if table_dict_tmp["table_db"]:
                        table_dict_tmp["table_full_name"] = f"{table_dict_tmp['table_db']}.{table_dict_tmp['table_name']}"
                    else:
                        table_dict_tmp["table_full_name"] = table_dict_tmp['table_name']
                    table_list_nes_query.append(table_dict_tmp)

            table_list_tmp = [d["table_name"] for d in table_list_nes_query]
            table_list_uni_query = []
            for table in list(select_statement.find_all(exp.Table)):
                if table.name not in table_list_tmp:
                    table_dict_tmp = {"table_alias": self.get_string_or_null(table.alias), "table_name": table.name,
                                      "table_db": self.get_string_or_null(table.db)}
                    if table_dict_tmp["table_db"]:
                        table_dict_tmp["table_full_name"] = f"{table_dict_tmp['table_db']}.{table_dict_tmp['table_name']}"
                    else:
                        table_dict_tmp["table_full_name"] = table_dict_tmp['table_name']
                    table_list_uni_query.append(table_dict_tmp)

            table_list = table_list_nes_query + table_list_uni_query
        else:
            table_list = []

        return table_list

    def get_conditions(self, parsed, condition_list) -> list:
        """
        """
        if parsed:
            if (isinstance(parsed.this, exp.And) or isinstance(parsed.this, exp.Or)):
                nested_parsed = parsed.this
                condition_list.append(nested_parsed.expression)
                if not (isinstance(nested_parsed.this, exp.And) or isinstance(nested_parsed.this, exp.Or)):
                    condition_list.append(nested_parsed.this)
                    return condition_list
                else:
                    return self.get_conditions(nested_parsed, condition_list)
            else:
                condition_list.append(parsed.this)
                return condition_list
        else:
            return []

    def get_components(self) -> dict():
        """
        """
        statement_components_dict = dict()
        if self.statement:
            statement_components_dict = {"select": self.statement.find(exp.Select),
                                         "from": self.statement.find(exp.From),
                                         "join": list(self.statement.find_all(exp.Join)),
                                         "where": self.statement.find(exp.Where),
                                         "groupby": self.statement.find(exp.Group),
                                         "having": self.statement.find(exp.Having),
                                         "orderby": self.statement.find(exp.Order),
                                       }
        return statement_components_dict

    def get_expressions_from_clause(self, statement) -> list:
        """
        """
        select_exp_list = list(self.components_dict.get("select").expressions) if isinstance(self.components_dict.get("select"),
                                                                                exp.Select) else []
        where_exp_list = self.get_conditions(self.components_dict.get("where"), []) if isinstance(self.components_dict.get("where"),
                                                                                     exp.Where) else []
        join_on_exp_list = [_join.args.get("on") for _join in self.components_dict.get("join") if "on" in _join.args.keys()]

        exp_list = []
        exp_list += [{"expression": _exp, "expression_sql": _exp.sql(dialect="oracle"), "clause": "select",
                      "expression_order_clause_level": i} for i, _exp in enumerate(select_exp_list)]
        exp_list += [{"expression": _exp, "expression_sql": _exp.sql(dialect="oracle"), "clause": "join_on",
                      "expression_order_clause_level": i} for i, _exp in enumerate(join_on_exp_list)]
        exp_list += [{"expression": _exp, "expression_sql": _exp.sql(dialect="oracle"), "clause": "where",
                      "expression_order_clause_level": i} for i, _exp in enumerate(where_exp_list)]

        return exp_list

    def get_string_or_null(self, str_var):
        """
        """
        return None if str_var.strip() == "" else str_var.strip()

    def get_table_name_from_alias(self, input_table_alias, input_field):
        """
        """
        table_name = None
        if input_table_alias is None:
            if len(self.table_list) == 1:
                table_name = self.table_list[0]["table_full_name"]
            else:
                pass  ### Logica para en base a la estructura de las tablas inputs, identificar el campo con el argumento input_field
        elif input_table_alias in self.table_dict.keys():
            table_name = self.table_dict.get(input_table_alias)["table_full_name"]
        return table_name

    def get_alias_from_expression(self, expression):
        """
        """
        alias = None
        if isinstance(expression, exp.Column):
            alias = self.get_string_or_null(expression.alias_or_name)
        elif isinstance(expression, exp.Alias):
            alias = self.get_string_or_null(expression.alias_or_name)
        return alias

    def get_fields_from_expression(self, expression):
        """
        """
        field_list = []

        if isinstance(expression, exp.Star):
            for table_row in self.table_list:
                field_list.append({"input_field": "*",
                                   "input_object_type": self.get_table_type(table_row["table_full_name"]),
                                   "input_table_name": self.get_table_name(table_row["table_full_name"]),
                                   "input_schema_path": self.get_schema_name(table_row["table_full_name"]),
                                   "input_table_alias": table_row["table_alias"]})
        elif expression:
            column_expr_list = list(expression.find_all(exp.Column))
            if len(column_expr_list) > 0:
                for column in column_expr_list:
                    field_dict = {"input_field": self.get_string_or_null(column.name),
                                  "input_table_alias": self.get_string_or_null(column.table)
                                  }
                    full_table_name = self.get_table_name_from_alias(field_dict["input_table_alias"], field_dict["input_field"])
                    field_dict["input_object_type"] = self.get_table_type(full_table_name)
                    field_dict["input_table_name"] = self.get_table_name(full_table_name)
                    field_dict["input_schema_path"] = self.get_schema_name(full_table_name)
                    field_list.append(field_dict)
            else:
                field_list.append({"input_field": "", "input_object_type": None, "input_table_name": None, "input_schema_path": None, "input_table_alias": None})
        else:
            field_list.append({"input_field": "", "input_object_type": None, "input_table_name": None, "input_schema_path": None, "input_table_alias": None})

        return field_list

    def get_field_mapping(self, insert_columns_list: list = []):
        """
        """
        field_mapping_list, field_mapping_dict = [], dict()
        try:
            if self.statement:
                expr_list = self.get_expressions_from_clause(self.statement)
                df_expr = pd.DataFrame(expr_list)
                df_expr["output_object_type"] = self.output_table_type
                df_expr["output_schema_path"] = self.output_schema
                df_expr["output_table"] = self.output_table
                df_expr["exp_field_list"] = df_expr["expression"].apply(lambda ex: self.get_fields_from_expression(ex))
                df_expr["output_field_alias"] = df_expr.apply(lambda row: self.get_alias_from_expression(row["expression"]) if row["clause"] == "select" else None, axis=1)

                if len(insert_columns_list) == len(df_expr[df_expr["clause"] == "select"]):
                    lista_out_original = df_expr.loc[df_expr["clause"] == "select", "output_field_alias"].to_list()
                    # Simular coalesce: si el valor en lista_out_original es None, usar el de insert_columns_list
                    lista_coalesce = [
                        original if pd.notna(original) else fallback
                        for original, fallback in zip(lista_out_original, insert_columns_list)
                    ]
                    # Asignar la lista coalesce de nuevo al DataFrame
                    df_expr.loc[df_expr["clause"] == "select", "output_field_alias"] = lista_coalesce

                df_expr_explode = df_expr.explode("exp_field_list")
                df_field_mapping = pd.concat([df_expr_explode["exp_field_list"].apply(pd.Series)[
                                                    ["input_field", "input_object_type", "input_schema_path", "input_table_name", "input_table_alias"]],
                                                df_expr_explode[
                                                    ["output_object_type", "output_schema_path", "output_table", "output_field_alias", "expression_sql", "clause",
                                                     "expression_order_clause_level"]]], axis=1)
                df_field_mapping["filter_expression_sql"] = self.components_dict["where"].sql(dialect="oracle") if self.components_dict["where"] else None
                df_field_mapping["filter_expression_sql"] = df_field_mapping.apply(lambda row: row["filter_expression_sql"] if (row["clause"] == "where" and row["expression_sql"].find(row["input_field"]) != -1) else (row["filter_expression_sql"].replace("WHERE ", "") if row["clause"] == "where" else None), axis=1)
                df_field_mapping["input_subquery_sql"] = df_field_mapping["input_table_name"].map(self.nested_query_map)
                df_field_mapping["output_subquery_sql"] = df_field_mapping["output_table"].map(self.nested_query_map)
                # Elimina duplicados por input_field y output_field_alias
                df_field_mapping = df_field_mapping.drop_duplicates(
                    subset=["input_table_alias", "input_field", "output_field_alias", "expression_sql"])
                field_mapping_list = df_field_mapping.to_dict(orient="records")
                field_mapping_dict = {"field_mapping_list": field_mapping_list, "flag_field_mapping_success": 1,
                                      "field_mapping_exception": None}
            else:
                field_mapping_dict = {"field_mapping_list": field_mapping_list, "flag_field_mapping_success": 0,
                                      "field_mapping_exception": self.field_mapping_exception}
        except Exception as e:
            field_mapping_dict = {"field_mapping_list": field_mapping_list, "flag_field_mapping_success": 0,
                                  "field_mapping_exception": str(e)}  # traceback.format_exc()

        return field_mapping_dict


class SelectStatement:

    def __init__(self, raw_statement, statement_id=0, output_table="__DEFAULT__"):
        self.raw_statement = raw_statement
        self.cleaned_statement = self.clean_statement()
        self.statement = self.parse_statement()
        self.statement_id = statement_id
        self.output_table = output_table
        self.field_mapping_exception = None

    def clean_statement(self):
        """
        """
        sub_list = [{"pattern_to_sub": r"PARTITION\s*\(.*?\)", "value_to_sub": ""}]
        clean_statement = utils.sub_patterns(self.raw_statement, sub_list)
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

    def replace_nested_query(self, row, nested_query_list_total) -> str:
        """
        """
        query_refined = row["nested_query_refined"]
        for nq in nested_query_list_total:
            if (nq["nested_query"] in query_refined) & (nq["nested_query_nro_select"] < row["nested_query_nro_select"]):
                query_refined = query_refined.replace(nq["nested_query"], f"SUBQUERY_{nq['nested_query_id']}")
        return query_refined

    def search_union_components(self, select_statement, select_list=[]):
        """
        """
        if isinstance(select_statement.this, exp.Union):
            select_list.append(select_statement.args.get("expression").sql(dialect="oracle"))
            return self.search_union_components(select_statement.this, select_list)
        else:
            select_list.append(select_statement.args.get("expression").sql(dialect="oracle"))
            select_list.append(select_statement.this.sql(dialect="oracle"))
            return select_list

    def split_select_by_union(self, raw_select_statement, statement_id):
        """
        """
        statement = parse_one(raw_select_statement, read="oracle")
        if isinstance(statement, exp.Union):
            select_statement_list = self.search_union_components(statement, [])
        else:
            select_statement_list = [raw_select_statement]
        return select_statement_list

    def split_plsql_script_in_nested_queries(self) -> list:
        """
        """
        nested_queries_list = []
        try:
            parenthesis_pair_list_input = sorted(
                [s.sql(dialect="oracle") if "alias" not in s.args.keys() else s.this.sql(dialect="oracle")
                 for s in self.statement.find_all(exp.Subquery)], reverse=True)
            if isinstance(self.statement, exp.Subquery):
                parenthesis_pair_list = [
                    {"nested_query": nq, "flag_nested": 1} if nq != self.statement.this.sql(dialect="oracle") else
                    {"nested_query": nq, "flag_nested": 0} for nq in parenthesis_pair_list_input]
            else:
                parenthesis_pair_list = [{"nested_query": nq, "flag_nested": 1} for nq in parenthesis_pair_list_input]
                parenthesis_pair_list.append({"nested_query": self.statement.sql(dialect="oracle"), "flag_nested": 0})

            df = pd.DataFrame(parenthesis_pair_list).drop_duplicates()

            df["nested_query_nro_select"] = df["nested_query"].apply(
                lambda s: len(re.findall(r"\bSELECT\b", s, re.IGNORECASE)))
            df["nested_query_id"] = df.index
            df = df.sort_values(by=["nested_query_nro_select", "nested_query_id"]).reset_index(drop=True)
            df["nested_query_index"] = df.index
            df["nested_query_id"] = df["nested_query_index"].apply(
                lambda i: f"{str(self.statement_id).zfill(3)}_{str(i).zfill(2)}")
            del df["nested_query_index"]
            df["nested_query_name"] = df.apply(
                lambda row: f"SUBQUERY_{row['nested_query_id']}" if row["flag_nested"] == 1 else self.output_table,
                axis=1)
            df["nested_query_refined"] = df["nested_query"].apply(
                lambda q: self.split_select_by_union(q, self.statement_id))
            df = df.explode("nested_query_refined")

            nested_query_list_total = df.loc[
                df["flag_nested"] == 1, ["nested_query", "nested_query_id", "nested_query_nro_select"]].sort_values(
                by="nested_query_nro_select", ascending=False).to_dict(orient="records")
            df["nested_query_refined"] = df.apply(lambda row: self.replace_nested_query(row, nested_query_list_total),
                                                  axis=1)
            df.loc[df["flag_nested"] == 0, 'nested_query_name'] = self.output_table
            nested_queries_list = df.to_dict(orient="records")

            output_dict = {"flag_success": 1, "nested_queries_list": nested_queries_list}

        except Exception as e:
            nested_queries_list = [{"nested_query": self.statement.sql(dialect="oracle"), "nested_query_nro_select": -1,
                                    "nested_query_id": 0, "nested_query_name": None, "nested_query_refined": None,
                                    "flag_nested": 0
                                    }]
            output_dict = {"flag_success": 0, "nested_queries_list": nested_queries_list}

        return output_dict

    def get_field_mapping(self, insert_columns_list: list = []):
        """
        """
        field_mapping_list = []
        nested_query_result = self.split_plsql_script_in_nested_queries()
        if nested_query_result["flag_success"] == 1:
            nested_query_map = {d["nested_query_name"]: d["nested_query"] for d in nested_query_result["nested_queries_list"] if "SUBQUERY" in d["nested_query_name"]}
            for nested_query_dict in nested_query_result["nested_queries_list"]:
                bss = BasicSelectStatement(nested_query_dict["nested_query_refined"],
                                           output_table=nested_query_dict["nested_query_name"],
                                           nested_query_map=nested_query_map)
                if nested_query_dict["flag_nested"] == 1:
                    field_mapping_list += bss.get_field_mapping()["field_mapping_list"]
                else:
                    field_mapping_list += bss.get_field_mapping(insert_columns_list)["field_mapping_list"]
        return field_mapping_list