# Checking col values
import pandas as pd


# Checking Values
def check_col_tuple(series):
    values = series.dropna().values
    for value in values:
        if type(value) == tuple:
            continue
        elif type(value) == str:
            try:
                if type(eval(value)) != tuple:
                    return False
            except:
                return False
        else:
            return False
    return True


def check_col_list(series):
    values = series.dropna().values
    for value in values:
        if type(value) == list:
            continue
        elif type(value) == str:
            try:
                if type(eval(value)) != list:
                    return False
            except:
                return False
        else:
            return False
    return True


def check_col_tuple_or_list(series):
    """
    Checks whether series values are lists/tuples or lists/tuples stored as strings
    :param series:
    :param dtype:
    :return:
    """
    values = series.dropna().values
    for value in values:
        if type(value) in {list, tuple}:
            continue
        elif type(value) == str:
            try:
                if type(eval(value)) not in {list, tuple}:
                    return False
            except:
                return False
        else:
            return False
    return True


def check_col_boolean(series):
    """
    Checks whether series values are booleans or booleans stored as strings
    :param series:
    :return:
    """
    value_set = set(series.dropna().unique())
    # Case 1: Boolean dtype
    if not value_set.difference({True, False}):
        return True
    # Case 2: True, False strings
    elif not value_set.difference({"True", "False"}):
        return True
    # Case 3: T, F strings
    elif not value_set.difference({"T", "F"}):
        return True
    return False


# Checking Dtypes
def check_dtype_array(df=None, col=None, infer_array_col=True, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "varchar_array"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Check whether column values are lists/tuples
    if current_dtype in {"dtype('O')"}:
        return check_col_tuple_or_list(df[col])
    # Case 3: Lists/tuples stored as strings
    elif infer_array_col and current_dtype in {"StringDtype"}:
        return check_col_tuple_or_list(df[col])
    return False


def check_dtype_boolean(df=None, col=None, infer_bool_col=True, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "bool"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Column has already been converted to an extension boolean dtype
    if current_dtype in {"BooleanDtype"}:
        return True
    # Case 3: Booleans stored as strings
    elif infer_bool_col and current_dtype in {"StringDtype"}:
        return check_col_boolean(df[col])
    return False


def check_dtype_date(df=None, col=None, infer_date_col=True, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "date"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Proper date values
    if current_dtype in {"datetime64[ns, UTC]"}:
        return True
    # Case 3: Inferred date values, but not in proper form yet
    elif infer_date_col and "date" in col.split("_"):
        return True
    return False


def check_dtype_float(df=None, col=None, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "float"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Column has already been converted to an extension float dtype
    return current_dtype in {
        "Float32Dtype()",
        "Float64Dtype()",
    }


def check_dtype_int(df=None, col=None, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "int"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Column has already been converted to an extension int dtype
    return current_dtype in {
        "Int32Dtype()",
        "Int64Dtype()",
    }


def check_dtype_string(df=None, col=None, dtype=None):
    # Case 1: Check whether dataframe dtype explicitly stated
    if dtype:
        return dtype == "string"
    else:
        current_dtype = df[col].dtype.__repr__()
    # Case 2: Column has already been converted to an extension string dtype
    return current_dtype == "StringDtype"


# Check Dataframes
def get_dataframe_dtypes(df):
    """
    Checks dataframe dtypes from user. Assumes dataframe is already preconverted.
    Creates a dtype dictionary from the dataframe where each key is a column
    and each value is the column's dtype
    :param df:
    :return:
    """
    dtype_dict = {}

    for col in df.columns:
        # Dates
        if check_dtype_date(df, col):
            dtype_dict[col] = "date"
        # Arrays
        elif check_dtype_array(df, col):
            dtype_dict[col] = "varchar_array"
        # Booleans
        elif check_dtype_boolean(df, col):
            dtype_dict[col] = "bool"
        # Ints
        elif check_dtype_int(df, col):
            dtype_dict[col] = "int"
        # Floats
        elif check_dtype_float(df, col):
            dtype_dict[col] = "float"
        # Strings
        elif check_dtype_string(df, col):
            dtype_dict[col] = "string"
        else:
            raise Exception(f"Dtype of Column {col} could not be determined")

    return dtype_dict


def get_database_dtypes(table, schema, connection):
    dtype_query = (
        f"select column_name, data_type\n"
        f"from information_schema.columns\n"
        f"where table_schema = '{schema}'\n"
        f"and table_name = '{table}'\n"
    )
    dtype_df = pd.read_sql_query(dtype_query, con=connection.connection)
    dtype_dict = dtype_df.set_index("column_name")["data_type"].to_dict()
    return dtype_dict
