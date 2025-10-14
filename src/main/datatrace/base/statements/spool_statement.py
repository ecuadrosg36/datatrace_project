from datatrace.base.statements.select_statement import SelectStatement
import datatrace.base.params as params
import datatrace.utils.utils as utils
import re


class SpoolStatement:

    def __init__(self, raw_statement, statement_id):
        """
        """
        self.raw_statement = raw_statement
        self.statement_id = statement_id
        self.components_dict = self.get_components()
        self.output_table = self.get_output_table()
        pass

    def get_components(self) -> dict():
        """
        """
        components_dict = utils.extract_regex_as_dict(params.STATEMENT_STRUCT_DICT["SPOOL_CUSTOM_SELECT"], self.raw_statement)
        if components_dict["raw_select_statement"]:
            components_dict["clean_select_statement"] = self.clean_statement(components_dict["raw_select_statement"])
        else:
            components_dict["clean_select_statement"] = None
        return components_dict

    def get_output_col_list(self, raw_output_table_struct):
        """
        """
        output_col_list = []
        if utils.match_pattern(r"SELECT\s*(.*?)\s*FROM", raw_output_table_struct):
            output_col_str = utils.get_pattern_text(r"SELECT\s*(.*?)\s*FROM", raw_output_table_struct, 1)
            sep_regex = utils.get_match_pattern_from_list(output_col_str, [r"\|\|';'\|\|", r"\|\|'\|'\|\|"])#self.get_spool_separator(output_col_str)
            output_col_list = [col.strip().replace("'", "") for col in re.split(sep_regex, output_col_str)]
        elif utils.match_pattern(r"\bPROMPT\b\s*(.*)", raw_output_table_struct):
            output_col_str = utils.get_pattern_text(r"\bPROMPT\b\s*(.*)", raw_output_table_struct, 1)
            output_col_list = [col.strip() for col in output_col_str.split(";")]

        return output_col_list

    def get_spool_separator(self, select_clause_str):
        """
        """
        sep_regex_list = [r"\|\|';'\|\|", r"\|\|'\|'\|\|"]
        sep_regex = None
        if select_clause_str:
            for sep_regex_temp in sep_regex_list:
                if utils.match_pattern(sep_regex_temp, select_clause_str):
                    sep_regex = sep_regex_temp
                    break
        return sep_regex

    def clean_statement(self, raw_statement):
        """
        """
        clean_statement = raw_statement
        select_clause_str = utils.get_pattern_text(r"SELECT\s*(.*?)\s*FROM", raw_statement, 1)
        sep_regex = utils.get_match_pattern_from_list(select_clause_str, [r"\|\|';'\|\|", r"\|\|'\|'\|\|"]) #self.get_spool_separator()
        if sep_regex:
            sub_list = [{"pattern_to_sub": sep_regex, "value_to_sub": ","}]
            clean_statement = utils.sub_patterns(raw_statement, sub_list)
        return clean_statement

    def clean_spool_statement(self, text):
        """
        :return:
        """
        # Remove HTML header
        pattern_spool = r"'<HEAD>'\s*\|\|(?:.|\s)*?'<BODY><TABLE BORDER=1 ALIGN=CENTER>'\s*\|\|"
        text = re.sub(pattern_spool, '', text, flags=re.DOTALL)

        # Remove HTML tags TD
        patter_tag = r"<(td|/td)[^>]*>"
        text = re.sub(patter_tag, '', text, flags=re.IGNORECASE | re.MULTILINE)

        #
        patter_tag = r"</TABLE></BODY>"
        text = re.sub(patter_tag, '', text, flags=re.IGNORECASE | re.MULTILINE)

        # Remove all row html tags
        pattern_html_tag = r"\'<.*?>.*\n"
        text = re.sub(pattern_html_tag, '', text)

        # remove ''||
        #patter_concat_2 = r"(''\s*)?\|\|"
        #patter_concat = r"(\|\|\s*''\s\|\|\n''\s*\|\|)"
        #text = re.sub(patter_concat, '\n,', text, flags=re.IGNORECASE | re.MULTILINE)
        #text = re.sub(patter_concat_2, '', text, flags=re.IGNORECASE | re.MULTILINE)

        # Remove CHR() functions
        #text = re.sub(r'(CHR|chr)\(\d+\)', ',', text, flags=re.IGNORECASE | re.MULTILINE)
        return text


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
                insert_columns_list = self.get_output_col_list(self.components_dict["raw_output_table_struct"])
                raw_select_statement = str(self.components_dict.get("clean_select_statement")).replace("&", "VAR_TO_DELETE_")
                #raw_select_statement = self.clean_spool_statement(raw_select_statement)

                field_mapping_dict["field_mapping_list"] = SelectStatement(raw_select_statement, self.statement_id, self.output_table).get_field_mapping(insert_columns_list)
                field_mapping_dict["flag_mapping_success"] = 1

            return field_mapping_dict
        except Exception as e:
            return {"field_mapping_list": [],
                    "flag_mapping_success": 0,
                    "field_mapping_exception": str(e)}