import pandas as pd


class traceScanner:

    def __init__(self, mapping_df):
        self.df_rel_table = self.get_rel(mapping_df, "table")
        self.df_root_table = self.get_root_nodes(mapping_df, "table")
        self.df_leaf_table = self.get_leaf_nodes(mapping_df, "table")
        self.df_rel_col = self.get_rel(mapping_df, "column")
        self.df_root_col = self.get_root_nodes(mapping_df, "column")
        self.df_leaf_col = self.get_leaf_nodes(mapping_df, "column")
        pass

    def get_back_table_trace(self):
        """
        """
        level = 0
        map_dict = {"source_table_y": "source_table", "source_table_x": "target_table"}

        df_tmp = self.df_leaf_table.rename(columns={"output_table": "source_table"})
        df_tmp["target_table"] = ""
        df_tmp["level"] = level

        n_predecessors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        while n_predecessors_to_search > 0:
            level -= 1
            df_tmp_to_append_0 = df_tmp.merge(self.df_rel_table, how="inner", left_on=["filename", "source_table"],
                                              right_on=["filename", "target_table"])
            df_tmp_to_append = df_tmp_to_append_0[["filename"] + list(map_dict.keys())].rename(columns=map_dict)
            df_tmp_to_append["level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()

            n_predecessors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_predecessors_to_search}")

        return df_trace

    def get_forw_table_trace(self):
        """
        """
        level = 0
        map_dict = {"target_table_x": "source_table", "target_table_y": "target_table"}

        df_tmp = self.df_root_table.rename(columns={"input_table": "target_table"})
        df_tmp["source_table"] = ""
        df_tmp["origin_table"] = df_tmp["target_table"]
        df_tmp["level"] = level
        df_tmp = df_tmp[["filename", "origin_table", "source_table", "target_table", "level"]]

        n_successors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        #print(f"Nivel: {level}, Relaciones: {n_successors_to_search}")
        while n_successors_to_search > 0:
            level += 1
            df_tmp_to_append_0 = df_tmp.merge(self.df_rel_table, how="inner", left_on=["filename", "target_table"],
                                              right_on=["filename", "source_table"])
            df_tmp_to_append = df_tmp_to_append_0[["filename", "origin_table"] + list(map_dict.keys())].rename(
                columns=map_dict).drop_duplicates(keep='first')
            df_tmp_to_append["level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()
            df_tmp = df_tmp.loc[~(df_tmp["source_table"] == df_tmp["target_table"])].reset_index(drop=True)
            n_successors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_successors_to_search}, {df_tmp.drop_duplicates().shape[0]} - {df_trace.shape[0]} - {df_trace.drop_duplicates().shape[0]}")

        df_general_trace = df_trace[
            ["filename", "source_table", "target_table", "level"]].drop_duplicates(keep='first').sort_values(
            by=["filename", "level"])

        df_origin_trace = df_trace.sort_values(by=["filename", "origin_table", "level"])

        return df_general_trace, df_origin_trace

    def get_table_lineage(self):
        """
        """

        df_general_trace, df_origin_trace = self.get_forw_table_trace()

        # df_origin_max_level = df_origin_trace.groupby(["filename", "origin_table"], as_index=False).agg({"level": "max"})
        # df_origin_destiny = df_origin_trace.merge(df_origin_max_level, how="inner", on=["filename", "origin_table", "level"])[["filename", "origin_table", "target_table"]].rename(columns={"target_table": "destiny_table"})
        df_origin_destiny = \
        df_origin_trace.merge(self.df_leaf_table, how="inner", left_on=["filename", "target_table"],
                              right_on=["filename", "output_table"])[
            ["filename", "origin_table", "output_table"]].drop_duplicates(keep='first').sort_values(
            by=["filename", "origin_table", "output_table"]).rename(
            columns={"output_table": "destiny_table"}).reset_index(drop=True)

        level = 0
        map_dict = {"source_table_y": "source_table", "source_table_x": "target_table"}

        df_tmp = df_origin_destiny.copy()
        df_tmp["source_table"] = df_tmp["destiny_table"]
        df_tmp["target_table"] = ""
        df_tmp["back_level"] = level

        n_predecessors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        while n_predecessors_to_search > 0:
            level -= 1
            df_tmp_to_append_0 = df_tmp.merge(df_origin_trace, how="inner",
                                              left_on=["filename", "origin_table", "source_table"],
                                              right_on=["filename", "origin_table", "target_table"])
            df_tmp_to_append = df_tmp_to_append_0[
                ["filename", "origin_table", "destiny_table"] + list(map_dict.keys())].rename(
                columns=map_dict).drop_duplicates(keep='first')
            df_tmp_to_append["back_level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()
            df_tmp = df_tmp.loc[~(df_tmp["source_table"] == df_tmp["target_table"])].reset_index(drop=True)
            n_predecessors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_predecessors_to_search}")

        df_lineage_table_origin_destiny = df_trace.sort_values(
            by=["filename", "origin_table", "destiny_table", "back_level"])
        df_lineage_table_origin_destiny["level"] = \
        df_lineage_table_origin_destiny.groupby(["filename", "origin_table", "destiny_table"])["back_level"].rank(
            "dense").astype(int) - 1

        # df_lineage_table_origin_destiny = df_lineage_table_origin_destiny[["filename", "origin_table", "destiny_table", "source_table", "target_table", "level"]]
        df_lineage_table_origin_destiny = df_lineage_table_origin_destiny.groupby(
            ["filename", "origin_table", "destiny_table", "target_table"], as_index=False).agg({"level": "min"}).rename(
            columns={"target_table": "step_table"}).sort_values(
            by=["filename", "origin_table", "destiny_table", "level"])
        df_lineage_table_origin_destiny = df_lineage_table_origin_destiny.loc[
            df_lineage_table_origin_destiny["step_table"].str.strip() != ""].reset_index(drop=True)

        return df_general_trace, df_lineage_table_origin_destiny

    def get_forw_column_trace(self):
        """
        """
        level = 0
        map_dict = {"target_table_x": "source_table", "target_field_x": "source_field",
                    "target_table_y": "target_table", "target_field_y": "target_field"}

        df_tmp = self.df_root_col.rename(columns={"input_table": "target_table", "input_field": "target_field"})
        df_tmp["source_table"] = ""
        df_tmp["source_field"] = ""
        df_tmp["origin_table"] = df_tmp["target_table"]
        df_tmp["origin_field"] = df_tmp["target_field"]
        df_tmp["level"] = level
        df_tmp = df_tmp[
            ["filename", "origin_table", "origin_field", "source_table", "source_field", "target_table", "target_field",
             "level"]]

        n_successors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        #print(f"Nivel: {level}, Relaciones: {n_successors_to_search}")
        while n_successors_to_search > 0:
            level += 1
            df_tmp_to_append_0 = df_tmp.merge(self.df_rel_col, how="inner",
                                              left_on=["filename", "target_table", "target_field"],
                                              right_on=["filename", "source_table", "source_field"])
            df_tmp_to_append = df_tmp_to_append_0[
                ["filename", "origin_table", "origin_field"] + list(map_dict.keys()) + ["clause",
                                                                                        "expression_sql"]].rename(
                columns=map_dict).drop_duplicates(keep='first')
            df_tmp_to_append["level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()[
                ["filename", "origin_table", "origin_field", "source_table", "source_field", "target_table",
                 "target_field"]]

            df_tmp = df_tmp.loc[~((df_tmp["source_table"] == df_tmp["target_table"]) & (
                        df_tmp["source_field"] == df_tmp["target_field"]))].reset_index(drop=True)
            n_successors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_successors_to_search}, {df_tmp.drop_duplicates().shape[0]} - {df_trace.shape[0]} - {df_trace.drop_duplicates().shape[0]}")

        df_general_trace = df_trace[
            ["filename", "source_table", "source_field", "target_table", "target_field", "level", "clause",
             "expression_sql"]].drop_duplicates(keep='first').sort_values(by=["filename", "level", "source_table"])
        df_origin_trace = df_trace.drop_duplicates(keep='first').sort_values(
            by=["filename", "origin_table", "origin_field", "level"])

        return df_general_trace, df_origin_trace

    def get_back_column_trace(self):
        """
        """
        level = 0
        map_dict = {"source_table_y": "source_table", "source_field_y": "source_field",
                    "source_table_x": "target_table", "source_field_x": "target_field"}

        df_tmp = self.df_leaf_col.rename(columns={"output_table": "source_table", "output_field": "source_field"})
        df_tmp["target_table"] = ""
        df_tmp["target_field"] = ""
        df_tmp["level"] = level

        n_predecessors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        while n_predecessors_to_search > 0:
            level -= 1
            df_tmp_to_append_0 = df_tmp.merge(self.df_rel_col, how="inner",
                                              left_on=["filename", "source_table", "source_field"],
                                              right_on=["filename", "target_table", "target_field"])
            df_tmp_to_append = df_tmp_to_append_0[
                ["filename"] + list(map_dict.keys()) + ["clause", "expression_sql"]].rename(columns=map_dict)
            df_tmp_to_append["level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()[
                ["filename", "source_table", "source_field", "target_table", "target_field"]]
            n_predecessors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_predecessors_to_search}")

        return df_trace

    def get_column_lineage(self):
        """
        """
        df_general_trace, df_origin_trace = self.get_forw_column_trace()

        # df_origin_max_level = df_origin_trace.groupby(["filename", "origin_table", "origin_field"], as_index=False).agg({"level": "max"})
        # df_origin_destiny = df_origin_trace.merge(df_origin_max_level, how="inner", on=["filename", "origin_table", "origin_field", "level"])[["filename", "origin_table", "origin_field", "target_table", "target_field"]].rename(columns={"target_table": "destiny_table", "target_field": "destiny_field"})
        df_origin_destiny = df_origin_trace.merge(self.df_leaf_col, how="inner",
                                                  left_on=["filename", "target_table", "target_field"],
                                                  right_on=["filename", "output_table", "output_field"])[
            ["filename", "origin_table", "origin_field", "output_table", "output_field"]] \
            .drop_duplicates(keep='first') \
            .sort_values(by=["filename", "origin_table", "origin_field", "output_table", "output_field"]) \
            .rename(columns={"output_table": "destiny_table", "output_field": "destiny_field"}).reset_index(drop=True)

        level = 0
        map_dict = {"source_table_y": "source_table", "source_field_y": "source_field", "clause_y": "clause",
                    "expression_sql_y": "expression_sql", "source_table_x": "target_table",
                    "source_field_x": "target_field"}

        df_tmp = df_origin_destiny.copy()
        df_tmp["source_table"] = df_tmp["destiny_table"]
        df_tmp["source_field"] = df_tmp["destiny_field"]
        df_tmp["target_table"] = ""
        df_tmp["target_field"] = ""
        df_tmp["clause"] = ""
        df_tmp["expression_sql"] = ""
        df_tmp["back_level"] = level

        n_predecessors_to_search = df_tmp.shape[0]
        df_trace = df_tmp.copy()

        while n_predecessors_to_search > 0:
            level -= 1
            df_tmp_to_append_0 = df_tmp.merge(df_origin_trace, how="inner",
                                              left_on=["filename", "origin_table", "origin_field", "source_table",
                                                       "source_field"],
                                              right_on=["filename", "origin_table", "origin_field", "target_table",
                                                        "target_field"])

            df_tmp_to_append = df_tmp_to_append_0[
                ["filename", "origin_table", "origin_field", "destiny_table", "destiny_field"] + list(
                    map_dict.keys())].rename(columns=map_dict).drop_duplicates(keep='first')
            df_tmp_to_append["back_level"] = level
            df_trace = pd.concat([df_trace, df_tmp_to_append])

            df_tmp = df_tmp_to_append.copy()
            df_tmp = df_tmp.loc[~((df_tmp["source_table"] == df_tmp["target_table"]) & (
                        df_tmp["source_field"] == df_tmp["target_field"]))].reset_index(drop=True)
            n_predecessors_to_search = df_tmp.shape[0]

            #print(f"Nivel: {level}, Relaciones: {n_predecessors_to_search}")

        df_lineage_column_origin_destiny = df_trace.sort_values(
            by=["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "back_level"])
        df_lineage_column_origin_destiny["level"] = df_lineage_column_origin_destiny.groupby(
            ["filename", "origin_table", "origin_field", "destiny_table", "destiny_field"])["back_level"].rank(
            "dense").astype(int) - 1

        df_lineage_column_origin_destiny["filter_unique"] = df_lineage_column_origin_destiny.groupby(
            ["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "target_table",
             "target_field"])["level"].rank("min")
        df_lineage_column_origin_destiny = df_lineage_column_origin_destiny.loc[
            df_lineage_column_origin_destiny["filter_unique"] == 1, ["filename", "origin_table", "origin_field",
                                                                     "destiny_table", "destiny_field", "target_table",
                                                                     "target_field", "clause", "expression_sql",
                                                                     "level"]].drop_duplicates(keep='first').rename(
            columns={"target_table": "step_table", "target_field": "step_field"}).sort_values(
            by=["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "level"])
        df_lineage_column_origin_destiny = df_lineage_column_origin_destiny.loc[
            df_lineage_column_origin_destiny["step_table"].str.strip() != ""].reset_index(drop=True)

        # df_lineage_column_origin_destiny = df_lineage_column_origin_destiny[["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "source_table", "source_field", "target_table", "target_field", "level"]]
        # df_lineage_column_origin_destiny = df_lineage_column_origin_destiny.groupby(["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "target_table", "target_field", "clause", "expression_sql"], as_index=False).agg({"level": "min"}).rename(columns={"target_table": "step_table", "target_field": "step_field"}).sort_values(by=["filename", "origin_table", "origin_field", "destiny_table", "destiny_field", "level"])

        return df_general_trace, df_lineage_column_origin_destiny

    def get_rel(self, mapping_df, _type='table'):
        if _type == 'table':
            col_list = ["filename", "input_table", "output_table"]
            df_rel = mapping_df.loc[pd.notnull(mapping_df["input_table"]) &
                                    pd.notnull(mapping_df["output_table"]), col_list].rename(
                columns={"input_table": "source_table", "output_table": "target_table"}
                ).drop_duplicates(keep='first').reset_index(drop=True)
        elif _type == 'column':
            col_list = ["filename", "input_table", "input_field", "output_table", "output_field", "clause",
                        "expression_sql"]
            df_rel = mapping_df.loc[pd.notnull(mapping_df["input_table"]) &
                                    pd.notnull(mapping_df["input_field"]) &
                                    pd.notnull(mapping_df["output_table"]) &
                                    pd.notnull(mapping_df["output_field"]), col_list].rename(
                columns={"input_table": "source_table", "input_field": "source_field",
                         "output_table": "target_table", "output_field": "target_field"}
                ).drop_duplicates(keep='first').reset_index(drop=True)  #
        else:
            df_rel = pd.DataFrame(columns=["filename"])

        return df_rel

    def get_leaf_nodes(self, mapping_df, _type='table'):
        if _type == 'table':
            df_tmp_leaf_0 = mapping_df[["filename", "output_table"]].drop_duplicates(keep='first')
            df_src = mapping_df[["filename", "input_table"]].drop_duplicates(keep='first')
            df_tmp_leaf_1 = df_tmp_leaf_0.merge(df_src, how="left", left_on=["filename", "output_table"],
                                                right_on=["filename", "input_table"])
            df_leaf = df_tmp_leaf_1.loc[
                ~pd.notnull(df_tmp_leaf_1["input_table"]), ["filename", "output_table"]].reset_index(drop=True)
        elif _type == 'column':
            df_tmp_leaf_0 = mapping_df.loc[
                mapping_df["clause"].isin(["select"]), ["filename", "output_table", "output_field"]].drop_duplicates(keep='first')
            df_src = mapping_df[["filename", "input_table", "input_field"]].drop_duplicates(keep='first')
            df_tmp_leaf_1 = df_tmp_leaf_0.merge(df_src, how="left",
                                                left_on=["filename", "output_table", "output_field"],
                                                right_on=["filename", "input_table", "input_field"])
            df_leaf = df_tmp_leaf_1.loc[
                ~pd.notnull(df_tmp_leaf_1["input_table"]), ["filename", "output_table", "output_field"]].reset_index(
                drop=True)
        else:
            df_leaf = pd.DataFrame("filename")

        return df_leaf

    def get_root_nodes(self, mapping_df, _type='table'):
        df_tmp_root_0 = mapping_df.loc[
            pd.notnull(mapping_df["input_table"]), ["filename", "input_table"]].drop_duplicates(keep='first')
        df_target = mapping_df[["filename", "output_table"]].drop_duplicates(keep='first')
        df_tmp_root_1 = df_tmp_root_0.merge(df_target, how="left", left_on=["filename", "input_table"],
                                            right_on=["filename", "output_table"])
        df_table_root = df_tmp_root_1.loc[
            ~pd.notnull(df_tmp_root_1["output_table"]), ["filename", "input_table"]].reset_index(drop=True)

        if _type == 'table':
            df_root = df_table_root.copy()
        elif _type == 'column':
            df_tmp_root_0 = mapping_df.loc[
                pd.notnull(mapping_df["input_table"]) & mapping_df["clause"].isin(["select"]), ["filename",
                                                                                                "input_table",
                                                                                                "input_field"]].drop_duplicates(keep='first')
            df_root = df_tmp_root_0.merge(df_table_root, how="inner", on=["filename", "input_table"])
        else:
            df_root = pd.DataFrame("filename")

        return df_root

    def get_root_nodes_backup(self, mapping_df, _type='table'):
        if _type == 'table':
            df_tmp_root_0 = mapping_df.loc[
                pd.notnull(mapping_df["input_table"]), ["filename", "input_table"]].drop_duplicates(keep='first')
            df_target = mapping_df[["filename", "output_table"]].drop_duplicates(keep='first')
            df_tmp_root_1 = df_tmp_root_0.merge(df_target, how="left", left_on=["filename", "input_table"],
                                                right_on=["filename", "output_table"])
            df_root = df_tmp_root_1.loc[
                ~pd.notnull(df_tmp_root_1["output_table"]), ["filename", "input_table"]].reset_index(drop=True)
        elif _type == 'column':
            df_tmp_root_0 = mapping_df.loc[
                pd.notnull(mapping_df["input_table"]) & mapping_df["clause"].isin(["select"]), ["filename",
                                                                                                "input_table",
                                                                                                "input_field"]].drop_duplicates(keep='first')
            df_target = df[["filename", "output_table", "output_field"]].drop_duplicates(keep='first')
            df_tmp_root_1 = df_tmp_root_0.merge(df_target, how="left",
                                                left_on=["filename", "input_table", "input_field"],
                                                right_on=["filename", "output_table", "output_field"])
            df_root = df_tmp_root_1.loc[
                ~pd.notnull(df_tmp_root_1["output_table"]), ["filename", "input_table", "input_field"]].reset_index(
                drop=True)
        else:
            df_root = pd.DataFrame("filename")

        return df_root