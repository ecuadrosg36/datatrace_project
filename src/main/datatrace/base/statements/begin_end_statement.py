from datatrace.base.statements.generator_statement import GeneratorStatement
import re
from sqlglot import parse_one, exp
import datatrace.base.params as params
import datatrace.utils.utils as utils

class BeginEndStatement(GeneratorStatement):
    def __init__(self, raw_statement, statement_id=0):
        self.raw_statement = raw_statement
        self.statement_id = statement_id
        self.components_list = self.get_components()
        #print(self.components_list)

    def get_components(self) -> list:
        # Extrae el cuerpo del bloque BEGIN ... END
        pattern = r"BEGIN\s*([\s\S]+?)\s*END;"
        match = re.search(pattern, self.raw_statement, re.IGNORECASE)
        components_list = []
        if match:
            block_body = match.group(1)
            # Extrae todas las sentencias SELECT ... INTO ... FROM ... [WHERE ...] [GROUP BY ...] [ORDER BY ...]
            select_pattern = (
                r"SELECT\s+(.*?)\s+INTO\s+(.*?)\s+FROM\s+(.*?)"
                r"(?:\s+WHERE\s+(.*?))?"
                r"(?:\s+GROUP BY\s+(.*?))?"
                r"(?:\s+ORDER BY\s+(.*?))?;"
            )
            for select_match in re.finditer(select_pattern, block_body, re.IGNORECASE | re.DOTALL):
                select_part = select_match.group(1).strip() if select_match.group(1) else None
                into_part = select_match.group(2).strip() if select_match.group(2) else None
                from_part = select_match.group(3).strip() if select_match.group(3) else None
                where_part = select_match.group(4).strip() if select_match.group(4) else None
                group_by_part = select_match.group(5).strip() if select_match.group(5) else None
                order_by_part = select_match.group(6).strip() if select_match.group(6) else None
                components_list.append({
                    "select_part": select_part,
                    "into_part": into_part,
                    "from_part": from_part,
                    "where_part": where_part,
                    "group_by_part": group_by_part,
                    "order_by_part": order_by_part
                })
        return components_list

    def split_sql_expressions(self, sql) -> list:
        """
        Divide una cadena SQL por comas, ignorando las que están dentro de paréntesis.
        """
        expressions = []
        current = ''
        depth = 0
        for char in sql:
            if char == ',' and depth == 0:
                expressions.append(current.strip())
                current = ''
            else:
                current += char
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
        if current:
            expressions.append(current.strip())
        return expressions

    def extract_first_column(self,expr_sql):
        try:
            parsed = parse_one(expr_sql, dialect="oracle")
            col = next(parsed.find_all(exp.Column), None)
            if col:
                return col.name
        except Exception:
            pass
        return None

    def get_field_mapping(self) -> dict:
        field_mapping_dict = {
            "field_mapping_list": [],
            "flag_mapping_success": 0,
            "field_mapping_exception": None
        }
        try:
            for comp in self.components_list:
                select_expressions = self.split_sql_expressions(comp["select_part"]) if comp["select_part"] else []
                output_field_aliases = [v.strip() for v in comp["into_part"].split(",")] if comp["into_part"] else []
                input_table_name = None
                input_schema_path = None
                if comp.get("from_part"):
                    if "." in comp["from_part"]:
                        input_schema_path, input_table_name = [x.strip() for x in comp["from_part"].split(".", 1)]
                    else:
                        input_table_name = comp["from_part"].strip()
                for idx, expr in enumerate(select_expressions):
                    output_field_alias = output_field_aliases[idx] if idx < len(output_field_aliases) else None
                    input_field = self.extract_first_column(expr)
                    field_mapping_dict["field_mapping_list"].append({
                        "input_object_type": "TABLA",
                        "input_schema_path": input_schema_path,
                        "input_table_name": input_table_name,
                        "input_subquery_sql": None,
                        "input_table_alias": None,
                        "input_field": input_field,
                        "filter_expression_sql": None,
                        "output_object_type": "TABLA",
                        "output_schema_path": None,
                        "output_table": None,
                        "output_subquery_sql": None,
                        "output_field_alias": output_field_alias,
                        "expression_sql": expr,
                        "clause": "begin_select_into"
                    })
                # Procesa columnas del WHERE si existe
                where_part = comp.get("where_part")
                if where_part:
                        try:
                            parsed_where = parse_one(where_part, dialect="oracle")
                            for col in parsed_where.find_all(exp.Column):
                                field_mapping_dict["field_mapping_list"].append({
                                    "input_object_type": "TABLA",
                                    "input_schema_path": input_schema_path,
                                    "input_table_name": input_table_name,
                                    "input_subquery_sql": None,
                                    "input_table_alias": None,
                                    "input_field": col.name,
                                    "filter_expression_sql": None,
                                    "output_object_type": "TABLA",
                                    "output_schema_path": None,
                                    "output_table": None,
                                    "output_subquery_sql": None,
                                    "output_field_alias": None,
                                    "expression_sql": where_part,
                                    "clause": "where"
                                })
                        except Exception:
                            pass
            if field_mapping_dict["field_mapping_list"]:
                field_mapping_dict["flag_mapping_success"] = 1
            return field_mapping_dict
        except Exception as e:
            field_mapping_dict["field_mapping_exception"] = str(e)
