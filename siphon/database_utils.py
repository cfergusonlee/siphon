import pandas as pd

# SQL
import siphon.type_conversion_utils
from siphon.type_checking_utils import (
    check_dtype_date,
    check_dtype_array,
    check_dtype_boolean,
)
from siphon.type_conversion_utils import (
    convert_to_tuple,
    convert_to_boolean,
)


def convert_csv_dtypes(df: pd.DataFrame):
    for col in df.columns:
        # Dates
        if check_dtype_date(df, col):
            df[col] = pd.to_datetime(df[col])
        # Arrays
        elif check_dtype_array(df, col):
            df[col] = df[col].apply(convert_to_tuple)
        # Everything else
        else:
            df[col] = siphon.type_conversion_utils.convert_dtypes()

    return df


def convert_database_dtypes(df: pd.DataFrame, dtype_df: pd.DataFrame) -> pd.DataFrame:
    dtype_dict = dtype_df.set_index("column_name")["data_type"].to_dict()
    for col, database_dtype in dtype_dict.items():
        # Arrays
        if check_dtype_array(database_dtype=database_dtype):
            continue
        # Booleans
        elif check_dtype_boolean(database_dtype=database_dtype):
            df[col] = df[col].apply(convert_to_boolean)
        # Dates
        elif check_dtype_date(database_dtype=database_dtype):
            continue
        # Everything else
        else:
            df[col] = siphon.type_conversion_utils.convert_dtypes().copy()

    return df


def check_table_exists(table, schema, connection):
    query = (
        f"select * from information_schema.tables\n"
        f"where table_schema = '{schema}' and table_name = '{table}'"
    )
    df = pd.read_sql_query(query, con=connection.connection)
    return df.shape[0] > 0


def get_reference_table(col):
    return col.replace("_id", "")


def declare_primary_key(df, table, schema, connection):
    """
    Adds primary key
    :param table:
    :param schema:
    :param connection:
    :return:
    """
    id_col = "id"

    # Add singular primary key if possible
    if id_col in df.columns:
        primary_key_query = f"alter table {schema}.{table} add primary key ({id_col})"
        connection.connection.execute(primary_key_query)
