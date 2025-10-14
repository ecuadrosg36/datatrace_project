import os
import re
import glob
import json
from typing import Optional

import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import datatrace.base.params as params
from datatrace.base.factory import GeneratorFactory
from datatrace.base.register import register_statement_types
from datatrace.utils.export import ExcelExporter
from datatrace.utils.logger import log
from datatrace import __version__

pd.set_option('display.max_columns', None)
spark = SparkSession.builder.getOrCreate()

def preprocess_dataframe_versionado(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesa un DataFrame para versionar relaciones recursivas entre tablas
    Ejemplo:
      ODS.TP_OJ_BASE_RASTREO_AUX [v1] -> ODS.TP_OJ_BASE_RASTREO_CTAS [v1]
      ODS.TP_OJ_BASE_RASTREO_CTAS [v1] -> ODS.TP_OJ_BASE_RASTREO_AUX [v2]
    """
    import networkx as nx
    from itertools import permutations
    from collections import defaultdict

    # 1. Copia base_old
    df = df_input.copy()

    # preprocesado
    df = df[['filename', 'input_table_name', 'output_table']].rename(columns={'input_table_name':'input_table'})
    df = df.replace({None: np.nan})

    filter_ = df.notna().any(axis=1) | (df['input_table'] != df['output_table'])
    df = df[filter_].drop_duplicates(keep='first').reset_index(drop=True)

    #add columns
    df['input_version'] = 1
    df['target_version'] = 1
    df['node'] = list(zip(df['input_table'], df['output_table']))

    # 2. Detectar relaciones recursivas (ciclos)
    G = nx.DiGraph()
    G.add_edges_from(df['node'])
    ciclos = list(nx.simple_cycles(G))

    relaciones_recursivas = set()
    for ciclo in ciclos:
        relaciones_recursivas.update(permutations(ciclo, 2))
    df_recursivas = df[df['node'].isin(relaciones_recursivas)].copy()

    # 3. Versionamiento de relaciones recursivas
    nuevas_relaciones = defaultdict(set)
    for _, row in df_recursivas.iterrows():
        filename = row['filename']
        node = row['node']
        node_rev = tuple(reversed(node))

        if node in nuevas_relaciones[filename] or node_rev in nuevas_relaciones[filename]:
            continue  # Ya procesado

        nuevas_relaciones[filename].add(node)

        # Encuentra el índice de la segunda aparición (recursiva)
        max_index = df[(df['filename'] == filename) & df['node'].isin([node, node_rev])].index.max()
        src_table, trg_table = df.loc[max_index, ['input_table', 'output_table']]

        # Incrementar versiones en recursividad
        df.loc[max_index, 'target_version'] += 1
        df.loc[(df['filename'] == filename) & (df.index > max_index) & (
                    df['input_table'] == trg_table), 'input_version'] += 1
        df.loc[(df['filename'] == filename) & (df.index > max_index) & (
                    df['output_table'] == trg_table), 'target_version'] += 1

    # 4. Crear columnas versionadas
    df['source'] = df.apply(
        lambda row: f"{row['input_table']} [v{row['input_version']}]" if row['input_version'] != 1 else row[
            'input_table'],
        axis=1
    )
    df['target'] = df.apply(
        lambda row: f"{row['output_table']} [v{row['target_version']}]" if row['target_version'] != 1 else row[
            'output_table'],
        axis=1
    )
    # nuevo nodo
    df['node'] = list(zip(df['source'], df['target']))
    return df[['filename', 'input_table', 'output_table','source', 'target', 'node']]

class DataTrace:
    """
    """

    def __init__(self):
        self.__dbutils = self.__getDbutils()#None
        register_statement_types()

    def __getDbutils(self, spark=None):
        """
        Instancia dbtutils ya que en un wheel no permite usar directamente el dbutils por lo que se debe de llamar a la libreria mediante pyspark
        from pyspark.dbutils import DBUtils

        Parameters
        --------
            spark: instancia de spark sesion  spark = SparkSession.builder.getOrCreate()
        """
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            if "dbutils" in IPython.get_ipython().user_ns.keys():
                dbutils = IPython.get_ipython().user_ns["dbutils"]
            else:
                dbutils = None
        return dbutils

    def extract_and_clean_comments(self, plsql_script) -> tuple[list, str]:
        """
        Extract comments from the plsql script

        :param plsql_script: The PL/SQL script as a string.
        :return: A set that contains a list of all identified comments and a plsql script after remove all of identified comments.
        """

        comment_pattern = re.compile(
            r"""
            (--[^\n]*          # Match single-line comments starting with --
            |                  # OR
            /\*.*?\*/          # Match multi-line comments starting with /* and ending with */
            )
            """
            ,
            re.DOTALL | re.VERBOSE
        )

        # Find all comments and COMMIT statements
        all_comments = comment_pattern.findall(plsql_script)
        plsql_script = comment_pattern.sub(r'', plsql_script)

        return all_comments, plsql_script

    def get_comments_df(self, comments:list, filename:str)-> tuple[pd.DataFrame, str, str]:

        if not comments:
            df_comments = pd.DataFrame(columns=params.COMMENT_COL_LIST)
            job_code = ''
            last_modif_date = ''
            return df_comments, job_code, last_modif_date

        #
        df_comments = pd.DataFrame(comments, columns=["comment"])
        df_comments["filename"] = os.path.basename(filename)
        df_comments["comment_id"] = df_comments.index

        # get parameters
        for statement_pattern, statement_type in params.COMMENT_TYPE_DICT.items():
            df_comments[statement_type] = df_comments['comment'].str.extract(statement_pattern)

        job_codes = list(df_comments[df_comments.JOB_CODE.notna()].JOB_CODE.unique())
        dates_list = list(df_comments[df_comments.DATE_MODIF.notna()].DATE_MODIF.unique())

        job_code = job_codes[0] if job_codes else None
        try:
            last_modif_date = (max([datetime.strptime(date, '%d/%m/%Y') for date in dates_list])).strftime(
            '%d/%m/%Y') if dates_list else None
        except:
            last_modif_date = None

        df_comments['job_code'] = job_code
        df_comments['last_modif_date'] = last_modif_date

        return df_comments[params.COMMENT_COL_LIST], job_code, last_modif_date

    def get_statement_type(self, sql: str, pattern_dict: dict) -> Optional[str]:
        """
        :param sql:
        :param pattern_dict:
        :return:
        """
        for pattern, stmt_type in pattern_dict.items():
            if re.search(pattern, sql, re.IGNORECASE | re.DOTALL):
                return stmt_type
        return None

    def refine_statement(self, raw_statement, raw_statement_type):
        """
        Refina sentencias dinámicas EXECUTE IMMEDIATE dentro de bloques BEGIN ... END;
        Si no hay EXECUTE IMMEDIATE, devuelve el bloque tal cual.
        """
        statement_dict = {
            "statement": raw_statement,
            "statement_type": raw_statement_type,
            "variable_list": ""
        }

        # Caso 1: BEGIN
        if raw_statement_type == "BEGIN_END":
            # Buscar bloque BEGIN ... END;
            begin_block_pattern = re.compile(r"BEGIN\s+(.*?)\s+END\s*;", re.DOTALL | re.IGNORECASE)
            begin_match = begin_block_pattern.search(raw_statement)

            if begin_match:
                block_content = begin_match.group(1)

                # Buscar EXECUTE IMMEDIATE dentro del bloque
                pattern = re.compile(r"EXEC(?:UTE)?\s+IMMEDIATE\s+(.*?)\s*;",
                                     re.DOTALL | re.IGNORECASE | re.MULTILINE)
                match = pattern.search(block_content)
                if match:
                    statement = match.group(1)

                    var_to_replace_list = []
                    if hasattr(params, "pattern_list"):
                        for _pattern in params.pattern_list:
                            pattern_regex = re.compile(_pattern)
                            var_to_replace_list += pattern_regex.findall(raw_statement)

                    static_statement = statement
                    for _var_tuple in var_to_replace_list:
                        static_statement = static_statement.replace(_var_tuple[0], _var_tuple[-1])

                    variable_list = set([t[-1].strip() for t in var_to_replace_list])
                    static_statement = static_statement.strip().replace("''", "'") + ";"
                    # Limpieza adicional para quitar comillas y espacios/comas extra
                    static_statement = static_statement.replace("'", "")
                    static_statement = re.sub(r"\s+,", ",", static_statement)
                    static_statement = re.sub(r",\s+", ",", static_statement)
                    static_statement = re.sub(r"\s+", " ", static_statement)
                    statement_type = self.get_statement_type(static_statement, params.CHILD_STATEMENT_TYPE_DICT)

                    statement_dict = {
                        "statement": static_statement,
                        "statement_type": statement_type,
                        "variable_list": ", ".join(variable_list)
                    }

                    # Caso 2: EXECUTE_IMMEDIATE directo
        # Caso 2: EXECUTE_IMMEDIATE directo
        elif raw_statement_type == "EXECUTE_IMMEDIATE":
            pattern = re.compile(r"EXEC(?:UTE)?\s+IMMEDIATE\s+'(.*?)'\s*;", re.DOTALL | re.IGNORECASE | re.MULTILINE)
            match = pattern.search(raw_statement)
            if match:
                statement = match.group(1)

                var_to_replace_list = []
                if hasattr(params, "pattern_list"):
                    for _pattern in params.pattern_list:
                        pattern_regex = re.compile(_pattern)
                        var_to_replace_list += pattern_regex.findall(raw_statement)

                static_statement = statement
                for _var_tuple in var_to_replace_list:
                    static_statement = static_statement.replace(_var_tuple[0], _var_tuple[-1])

                variable_list = set([t[-1].strip() for t in var_to_replace_list])
                static_statement = static_statement.strip().replace("''", "'") + ";"

                statement_type = self.get_statement_type(static_statement, params.CHILD_STATEMENT_TYPE_DICT)

                statement_dict = {
                    "statement": static_statement,
                    "statement_type": statement_type,
                    "variable_list": ", ".join(variable_list)
                }

        return statement_dict

    def get_statements_by_pattern_dict(self, plsql_script: str) -> list:
        """
        :param plsql_script:
        :return:
        """
        general_pattern = "|".join(f"(?P<{stmt_type}>{pattern})" for pattern, stmt_type in params.GENERAL_STATEMENT_TYPE_DICT.items())
        statement_pattern = re.compile(general_pattern, re.DOTALL | re.IGNORECASE | re.MULTILINE)

        child_pattern = "|".join(f"(?P<{stmt_type}>{pattern})" for pattern, stmt_type in params.CHILD_STATEMENT_TYPE_DICT.items())
        child_statement_pattern = re.compile(child_pattern, re.DOTALL | re.IGNORECASE | re.MULTILINE)

        parent_stmt_id = 1
        all_statements = []
        for match in statement_pattern.finditer(plsql_script):
            # Get which named group matched
            statement_type = next(
                (key for key in params.GENERAL_STATEMENT_TYPE_DICT.values() if match.group(key)), None
            )
            statement_text = match.group().strip()

            start_pos = match.start()
            end_pos = match.end()

            start_line = plsql_script[:start_pos].count("\n") + 1
            end_line = plsql_script[:end_pos].count("\n") + 1

            statement_child = []
            if statement_type in ["DECLARE_PROCEDURE", "DECLARE_FUNCTION"]:#"DECLARE_BEGIN",
                for child_match in child_statement_pattern.finditer(statement_text):
                    child_statement_type = next(
                        (key for key in params.CHILD_STATEMENT_TYPE_DICT.values() if child_match.group(key)), None
                    )
                    child_statement_text = child_match.group().strip()

                    child_refined_dict = self.refine_statement(child_statement_text, child_statement_type)

                    child_start_pos = child_match.start()
                    child_end_pos = child_match.end()

                    child_start_line = statement_text[:child_start_pos].count("\n")
                    child_end_line = statement_text[:child_end_pos].count("\n")

                    statement_child.append({
                        "parent_statement_id": parent_stmt_id,
                        "parent_statement": statement_text,
                        "parent_statement_type": statement_type,
                        "parent_start_line": start_line,
                        "parent_end_line": end_line,
                        "raw_statement": child_statement_text,
                        "raw_statement_type": child_statement_type,
                        "start_line": start_line + child_start_line,
                        "end_line": start_line + child_end_line,
                        "statement": child_refined_dict["statement"],
                        "statement_type": child_refined_dict["statement_type"],
                        "variable_list": child_refined_dict["variable_list"]
                    })
                all_statements += statement_child
                parent_stmt_id += 1
            else:
                child_refined_dict = self.refine_statement(statement_text, statement_type)

                all_statements.append({
                    "parent_statement_id": None,
                    "parent_statement": None,
                    "parent_statement_type": None,
                    "parent_start_line": None,
                    "parent_end_line": None,
                    "raw_statement": statement_text,
                    "raw_statement_type": statement_type,
                    "start_line": start_line,
                    "end_line": end_line,
                    "statement": child_refined_dict["statement"],
                    "statement_type": child_refined_dict["statement_type"],
                    "variable_list": child_refined_dict["variable_list"]
                })

        return all_statements

    def extract_statements(self, filename) -> tuple[pd.DataFrame, pd.DataFrame, str]:
        """
        Extract SQL statements from the PL/SQL script.

        :param filename: The PL/SQL script filename.
        :return: A set that contains a DataFrame with all SQL statements and the plsql remainder after remove all of identified statements.
        """

        file_plsql_script = open(filename, 'r', errors='ignore')
        plsql_script = "".join(file_plsql_script.readlines())
        comments, clean_plsql_script = self.extract_and_clean_comments(plsql_script)
        all_statements = self.get_statements_by_pattern_dict(clean_plsql_script)

        # obteniendo datos de cabecera
        df_comments, job_code, last_modif_date = self.get_comments_df(comments, filename)

        # trabajando statements
        if len(all_statements) > 0:
            df_statements = pd.DataFrame(all_statements)
            df_statements["filename"] = os.path.basename(filename)
            df_statements["filename_path"] = os.path.dirname(filename)
            df_statements["statement_id"] = df_statements.index
            df_statements["job_code"] = job_code
            df_statements["last_modif_date"] = last_modif_date
        else:
            df_statements = pd.DataFrame(columns=params.BASE_STMT_COL_LIST)

        plsql_script_remainder = clean_plsql_script

        for stmt_dict in all_statements:
            if stmt_dict["parent_statement"]:
                plsql_script_remainder = plsql_script_remainder.replace(stmt_dict["parent_statement"], "")
            else:
                plsql_script_remainder = plsql_script_remainder.replace(stmt_dict["raw_statement"], "")

        plsql_script_remainder = "\n".join([line.strip() for line in plsql_script_remainder.split("\n") if line.strip() != ""])
        plsql_analysis_script = clean_plsql_script
        parent_statement_list = df_statements.loc[df_statements["parent_statement_type"].isin(["DECLARE_BEGIN","DECLARE_PROCEDURE","DECLARE_FUNCTION"]), ["parent_statement_type", "parent_statement_id", "parent_statement"]].drop_duplicates().rename(columns={"parent_statement_type": "statement_type", "parent_statement_id": "statement_id", "parent_statement": "statement"}).to_dict(orient="records")
        raw_statement_list = df_statements.loc[df_statements["raw_statement_type"].isin(["EXECUTE_IMMEDIATE"]) & df_statements["statement_type"].isin(GeneratorFactory.allowed_statement_types.keys()), ["raw_statement_type", "statement_id","raw_statement"]].drop_duplicates().rename(columns={"raw_statement_type": "statement_type", "raw_statement": "statement"}).to_dict(orient="records")
        statement_list = df_statements.loc[df_statements["statement_type"].isin(GeneratorFactory.allowed_statement_types.keys()), ["statement_type", "statement_id", "statement"]].drop_duplicates().to_dict(orient="records")

        statement_list_to_replace = parent_statement_list + raw_statement_list + statement_list
        for stmt_dict in statement_list_to_replace:
            if stmt_dict['statement_type'] in ["DECLARE_BEGIN","DECLARE_PROCEDURE","DECLARE_FUNCTION"]:
                to_replace_str = f"-- [Datatrace] Parent Statement ID: {stmt_dict['statement_id']}\n{stmt_dict['statement']}\n-- [Datatrace]"
            else:
                to_replace_str = f"-- [Datatrace] Statement ID: {stmt_dict['statement_id']}\n{stmt_dict['statement']}\n-- [Datatrace]"
            plsql_analysis_script = plsql_analysis_script.replace(stmt_dict['statement'], to_replace_str)

        remain_path = os.path.join(os.path.dirname(filename), "remain", os.path.basename(filename))
        analysis_path = os.path.join(os.path.dirname(filename), "analysis", os.path.basename(filename))

        self.write_script(plsql_script_remainder, remain_path)
        self.write_script(plsql_analysis_script, analysis_path)

        plsql_script_dict = {
                             "filename": os.path.basename(filename),
                             "filename_path": os.path.dirname(filename),
                             "job_code": job_code,
                             "last_modif_date": last_modif_date,
                             "q_lines_script": len(plsql_script.split("\n")),
                             "q_lines_no_coverage": len(plsql_script_remainder.split("\n")),
                             "%_lines_coverage": round(100.0*(1 - (len(plsql_script_remainder.split("\n")) / len(plsql_script.split("\n")))), 1),
                             "q_statements": len(all_statements),
                             "q_comments": len(comments),
                             "plsql_script_remainder": plsql_script_remainder
                            }

        return df_statements, df_comments[params.COMMENT_COL_LIST], plsql_script_dict

    def get_current_user(self) -> str:
        """
        """
        try:
            user_name = self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except:
            user_name = "DEFAULT"

        return user_name

    def generate_log(self, data_dict):
        """
        """
        log.info(f"LAST TASK: GENERATE LOG")
        json_log = {
                    "employee_code": data_dict["employee_code"],
                    "user_name": self.get_current_user(),
                    "version": __version__,
                    "start_time": data_dict["start_time"],
                    "end_time": data_dict["end_time"],
                    "general_list": data_dict["general_list"]
        }
        json_log_path = f"{params.WORKSPACE_SHARED_LOG_PATH}/{json_log['employee_code']}/{json_log['start_time']}.json"
        if not os.path.exists(os.path.dirname(json_log_path)):
            try:
                os.makedirs(os.path.dirname(json_log_path))
            except OSError as error:
                log.error(f"ERROR CREATING THE DIRECTORY OF THE PATH: {str(error)}")

        try:
            log.info(f"WRITE JSON LOG: {json_log_path}")
            with open(json_log_path, "w") as log_file:
                json.dump(json_log, log_file)
        except Exception as e:
            log.error(f"WRITE JSON ERROR: {str(e)}")

    @staticmethod
    def create_folder(path):
        try:
            if os.path.exists(path):
                log.info(f"\tDIRECTORY PATH: {path} EXISTS.")
            else:
                os.makedirs(path)
        except OSError as error:
            log.error(f"ERROR CREATING THE DIRECTORY OF THE PATH: {error}")

    @staticmethod
    def write_script(script, path):
        if os.path.exists(os.path.dirname(path)):
            with open(path, "w") as f:
                f.write(script)
        else:
            log.error(f"ERROR WRITING THE PATH: {path}")

    def run_data_tracer(self, script_path: str, output_path: str, employee_code: str = 'DEFAULT') -> dict:
        """
        Extract SQL statements from the PL/SQL script.

        :param script_path: Path for PL/SQL scripts.
        :param output_path: The output path for data lineage report.
        :return: A dictionary that contains all DataFrames from Data Lineage Process.
        """
        start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_all_statements_prev, df_comments = pd.DataFrame(), pd.DataFrame()
        general_list = []

        log.info(f"TASK 1: CREATING FOLDERS")

        if os.path.isfile(script_path):
            self.create_folder(os.path.join(os.path.dirname(script_path), "remain"))
            self.create_folder(os.path.join(os.path.dirname(script_path), "analysis"))
            log.info(f"\tPARAMETER SCRIPT_PATH: {script_path}. IT´S A UNIQUE FILE.")
            log.info(f"TASK 2: IDENTIFY STATEMENTS")
            df_all_statements_prev, df_comments, plsql_script_dict = self.extract_statements(script_path)
            general_list.append(plsql_script_dict)

        elif os.path.isdir(script_path):
            self.create_folder(os.path.join(script_path, "remain"))
            self.create_folder(os.path.join(script_path, "analysis"))
            log.info(f"\tPARAMETER SCRIPT_PATH: {script_path}. IT´S A DIRECTORY")
            log.info(f"TASK 2: IDENTIFY STATEMENTS")
            exclude_dirs = {os.path.join(script_path, "remain"), os.path.join(script_path, "analysis")}
            all_files = [f for f in glob.glob(os.path.join(script_path, "**", "*"), recursive=True) if f.lower().endswith(".sql")]
            filtered_files = [f for f in all_files if not any(os.path.commonpath([f, ex]) == ex for ex in exclude_dirs)]
            for filename in filtered_files:
                log.info(f"\tFILENAME: {filename}.")
                df_statements_prev_tmp, df_comments_tmp, plsql_script_dict = self.extract_statements(filename)
                #log.info(f"\tFILENAME: {filename} - {df_statements_prev_tmp.shape[0]} rows.\n{df_statements_prev_tmp.head(5)}")
                if df_statements_prev_tmp.shape[0] > 0:
                    df_all_statements_prev = pd.concat([df_all_statements_prev, df_statements_prev_tmp])
                if df_comments_tmp.shape[0] > 0:
                    df_comments = pd.concat([df_comments, df_comments_tmp])
                general_list.append(plsql_script_dict)

        else:
            log.error(f"YOU HAVE NOT ENTERED A CORRECT DIRECTORY PATH OR SCRIPT: {script_path}.")

        df_general_prev = pd.DataFrame(general_list)
        df_statements_prev = df_all_statements_prev.reset_index(drop=True).copy()

        log.info(f"TASK 3: ANALYZE CORE STATEMENTS")
        if df_statements_prev.shape[0] == 0:
            log.error(f"""NO STATEMENTS HAVE BEEN IDENTIFIED IN THE SCRIPT. \nREMEMBER THAT IT ONLY COVERS STATEMENTS OF THE CREATE_TABLE_AS AND INSERT_INTO_SELECT TYPE.\nLINEAGE IS NOT CAPTURED.""")
            return dict()

        df_statements_prev["flag_to_analyze"] = df_statements_prev["statement_type"].apply(lambda s: 1 if s in GeneratorFactory.allowed_statement_types.keys() else 0)
        df_field_mapping = df_statements_prev.apply(lambda row: GeneratorFactory.from_statement_type(row["statement"], row["statement_type"], row["statement_id"]).get_field_mapping(), axis=1).apply(pd.Series)
        df_statements = pd.concat([df_statements_prev, df_field_mapping], axis=1)
        df_statements_agg = df_statements.groupby(["filename_path", "filename"], as_index=False).agg(q_statements_to_analyze=("flag_to_analyze", "sum"),
                                                                                                         q_statements_success_analysis=("flag_mapping_success", "sum")
                                                                                                        )
        df_statements_agg["%_statement_coverage"] = df_statements_agg.apply(lambda row: round(100 * (row["q_statements_success_analysis"]/row["q_statements_to_analyze"]), 1) if row["q_statements_to_analyze"] > 0 else None, axis=1)
        df_general = df_general_prev.merge(df_statements_agg, how="left", on=["filename_path", "filename"])[params.GENERAL_COL_LIST]
        df_statements_core = df_statements.loc[df_statements["flag_mapping_success"] == 1]

        if df_statements_core.shape[0] > 0:
            df_field_mapping = df_statements_core.explode("field_mapping_list")
            df_field_mapping = pd.concat([df_field_mapping, df_field_mapping["field_mapping_list"].apply(pd.Series)], axis=1)[params.FIELD_MAPPING_RESUME_LIST]
            df_field_mapping = df_field_mapping.reset_index(drop=True)

            # Filtra todas las filas BEGIN_END con alias que empiezan con ':'
            begin_rows = df_field_mapping[
                (df_field_mapping['statement_type'] == 'BEGIN_END') &
                (df_field_mapping['output_field_alias'].str.startswith(':'))
                ]

            # Filtra todas las filas INSERT_INTO_SELECT con expresión que empieza con ':'
            insert_rows = df_field_mapping[
                (df_field_mapping['statement_type'] == 'INSERT_INTO_SELECT') &
                (df_field_mapping['expression_sql'].str.startswith(':'))
                ]

            # Recorre cada combinación BEGIN_END → INSERT_INTO_SELECT
            for _, begin_row in begin_rows.iterrows():
                v_expr = begin_row['expression_sql']
                v_mes_expr = begin_row['output_field_alias']

                # Busca coincidencias en INSERT_INTO_SELECT
                matching_insert = insert_rows[insert_rows['expression_sql'] == v_mes_expr]

                for idx, insert_row in matching_insert.iterrows():
                    # Aplica la transformación solo a esa fila específica
                    df_field_mapping.loc[
                        idx, ['expression_sql', 'input_table_name', 'input_schema_path', 'input_object_type']] = [
                        v_expr,
                        begin_row['input_table_name'],
                        begin_row['input_schema_path'],
                        begin_row['input_object_type']
                    ]

            df_export = df_field_mapping.copy()
            for null_col in [c for c in params.EXPORT_COL_LIST if c not in df_field_mapping.columns]:
                df_export[null_col] = None
            df_export = df_export[params.EXPORT_COL_LIST]
        else:
            df_field_mapping = pd.DataFrame(columns=params.FIELD_MAPPING_RESUME_LIST)
            df_export = pd.DataFrame(columns=params.EXPORT_COL_LIST)

        df_statements = df_statements[params.BASE_STMT_COL_LIST]

        df_procedures = df_statements[df_statements.statement_type == "EXECUTE_PROCEDURE"].reset_index(drop=True)
        if df_procedures.shape[0] > 0:
            df_procedures["is_control_procedure"] = False
            df_procedures.loc[df_procedures['statement'].str.contains('_CONTROL', case=False), 'is_control_procedure'] = True
            df_procedures = df_procedures[params.PROCEDURES_COL_LIST]
        else:
            df_procedures = pd.DataFrame(columns=params.PROCEDURES_COL_LIST)

        sheet_config = {
            "general_insights": df_general,
            "statements": df_statements,
            "field_mapping_resume": df_field_mapping,
            "procedure_execution": df_procedures,
            "comments": df_comments,
            "datatrace_report": df_export
        }
        exporter = ExcelExporter(output_path, sheet_config)
        exporter.write()

        log.info(f"""TASK 5: REPORT EXPORTED TO {output_path}""")
        output_dict = {
                        "general_insights": df_general,
                        "statements": df_statements,
                        "comments": df_comments,
                        "field_mapping_resume": df_field_mapping,
                        "field_mapping_detail": df_field_mapping
                        }

        end_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.generate_log({"general_list": general_list, "start_time": start_time, "end_time": end_time, "employee_code": employee_code})

        return output_dict