# Data wrangling
import pandas as pd
import numpy as np

# SQL
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import (
    TIMESTAMP,
    BIGINT,
    VARCHAR,
    TEXT,
    NUMERIC,
    DATE,
    BOOLEAN,
    ARRAY,
    JSONB,
)
import re
import time

from siphon.PostgresConnection import PostgresConnection
from siphon.database_utils import (
    check_table_exists,
    declare_primary_key,
    get_reference_table,
)
from siphon.type_checking_utils import get_dataframe_dtypes, get_database_dtypes
from siphon.type_conversion_utils import (
    convert_dtypes,
    pre_convert_data,
    convert_dataframe_columns,
)


class PostgresDatabase:
    def __init__(self, schema="raw", database_var="CAM_DATABASE_URL"):
        self.schema = schema
        self.database_var = database_var

    def get_table(self, table, schema=None) -> pd.DataFrame:
        """
        Retrieves table from appropriate database
        :param schema:
        :param table_name:
        :param flavor:
        :return:
        """

        schema = schema or self.schema

        with PostgresConnection(database_var=self.database_var) as connection:
            df = pd.read_sql_table(
                table_name=table, con=connection.connection, schema=schema
            )
            db_dtype_dict = get_database_dtypes(table, schema, connection.connection)
            df_dtype_dict = convert_dtypes(
                dtype_dict=db_dtype_dict,
                from_dtype="postgres_description",
                to_dtype="dataframe_dtype",
            )
            df = pre_convert_data(df)
            df = convert_dataframe_columns(df, df_dtype_dict)

        return df

    def get_filtered_export(
        self, df, table, schema, connection, id_col="id", if_exists="replace"
    ):
        if (
            not check_table_exists(table, schema, connection)
            or if_exists == "replace"
            or "id" not in df.columns
        ):
            return df
        table_df = self.get_table(table, schema=schema)
        filtered_ids = set(df[id_col].values).difference(table_df[id_col].values)
        filtered_df = df[df[id_col].isin(filtered_ids)].copy()
        return filtered_df

    def export_table(
        self,
        df: pd.DataFrame,
        table,
        schema=None,
        if_exists="replace",
        method="multi",
        show_confirmation=True,
    ):
        with PostgresConnection(database_var=self.database_var) as connection:
            schema = schema or self.schema
            df = self.get_filtered_export(
                df, table, schema, connection, if_exists=if_exists
            )
            if df.shape[0] == 0:
                return
            df = pre_convert_data(df)
            df_dtype_dict = get_dataframe_dtypes(df)
            df = convert_dataframe_columns(df, df_dtype_dict)
            table_already_exists = check_table_exists(table, schema, connection)
            dtype_param = convert_dtypes(
                dtype_dict=df_dtype_dict,
                from_dtype="dataframe_dtype",
                to_dtype="postgres_dtype",
            )

            # Attempting to overwrite mismatched data results in error
            if if_exists == "replace":
                connection.connection.execute(
                    f"drop table if exists {schema}.{table} cascade"
                )
            if show_confirmation:
                print(f"Exporting {table} {df.shape} to {schema}", end="")
            start = time.time()
            df.to_sql(
                table,
                method=method,
                if_exists=if_exists,
                dtype=dtype_param,
                schema=schema,
                con=connection.connection,
                index=False,
            )
            end = time.time()
            elapsed_time = end - start
            if not table_already_exists:
                declare_primary_key(df, table, schema, connection)
            if show_confirmation:
                print(f" in {elapsed_time} seconds")

    def declare_foreign_keys(self):
        """
        Adds foreign key relationships
        :param schema:
        :return:
        """

        with PostgresConnection(database_var=self.database_var) as connection:
            table_query = (
                f"select table_name\n"
                f"from information_schema.tables\n"
                f"where table_schema = '{self.schema}'"
            )
            table_df = pd.read_sql_query(table_query, con=connection.connection)
            for table in table_df.table_name.values:
                df = self.get_table(table, schema=self.schema)
                for col in df.columns:

                    # Case 1: Non-id column
                    if col.split("_")[-1] != "id":
                        continue
                    # Case 2: Primary key
                    elif col == "id":
                        continue
                    # Case 3: Foreign key
                    else:
                        reference_table = get_reference_table(col)
                        query = f"Alter table {self.schema}.{table} add foreign key ({col}) references {self.schema}.{reference_table}"

                    connection.connection.execute(query)
