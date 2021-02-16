import numpy as np
import pandas as pd

from sqlalchemy.dialects.postgresql import (
    TIMESTAMP,
    BIGINT,
    VARCHAR,
    NUMERIC,
    BOOLEAN,
    ARRAY,
)

from siphon.type_checking_utils import (
    check_dtype_date,
    check_dtype_array,
    check_dtype_boolean,
    check_dtype_int,
    check_dtype_float,
    check_dtype_string,
)


# Converting column values
def convert_to_list(value):
    if type(value) in {pd._libs.missing.NAType} or value != value:
        return value
    if type(value) == list:
        return value
    elif type(value) == tuple:
        return list(value)
    elif type(value) != str:
        raise Exception(f"Cannot convert {value} to a list")
    evaluated_value = eval(value)
    if type(evaluated_value) == list:
        return evaluated_value
    elif type(evaluated_value) == tuple:
        return list(evaluated_value)
    else:
        raise Exception


def convert_to_tuple(value):
    if type(value) in {pd._libs.missing.NAType} or value != value:
        return value
    if type(value) == list:
        return tuple(value)
    elif type(value) == tuple:
        return value
    elif type(value) != str:
        raise Exception(f"Cannot convert {value} to a tuple")
    evaluated_value = eval(value)
    if type(evaluated_value) == tuple:
        return evaluated_value
    elif type(evaluated_value) == list:
        return tuple(evaluated_value)
    else:
        raise Exception


def convert_to_boolean(value):
    if pd.isna(value):
        return value
    elif value in {True, False}:
        return value
    elif type(value) == str:
        if value.lower() in {"true", "t"}:
            return True
        elif value.lower() in {"false", "f"}:
            return False
    else:
        raise Exception(f"Unsupported boolean type: {type(value)}")


# Converting column dtypes
def convert_dtype_date(df, col):
    dtype = df[col].dtype.__repr__()
    if dtype in {"datetime64[ns, UTC]"}:
        return df[col]
    else:
        return pd.to_datetime(df[col], utc=True).copy()


def convert_dtype_array(df, col):
    """
    Converts a list or tuple column to a tuple column and fills missing
    values with pd.NA
    :param df:
    :param col:
    :return:
    """
    df[col] = df[col].apply(convert_to_tuple).fillna(pd.NA).copy()
    return df[col]


def convert_dtype_boolean(df, col):
    """
    Converts a boolean column to the extension dtype BooleanDtype
    :param df:
    :param col:
    :return:
    """
    dtype = df[col].dtype.__repr__()
    if dtype in {"BooleanDtype"}:
        return df[col]
    elif dtype in {"dtype('bool')"}:
        return df[col].convert_dtypes().copy()
    else:
        return df[col].apply(convert_to_boolean).convert_dtypes().copy()


def convert_dtype_int(df, col):
    """
    Converts an int column to the extension dtype Int32Dtype() or Int64Dtype()
    :param df:
    :param col:
    :return:
    """
    dtype = df[col].dtype.__repr__()
    if dtype in {"Int32Dtype()", "Int64Dtype()"}:
        return df[col]
    else:
        return df[col].convert_dtypes().copy()


def convert_dtype_float(df, col):
    """
    Converts a float column to the extension dtype Float32Dtype() or Float64Dtype()
    :param df:
    :param col:
    :return:
    """
    dtype = df[col].dtype.__repr__()
    if dtype in {"Float32Dtype()", "Float64Dtype()"}:
        return df[col]
    else:
        return df[col].convert_dtypes().copy()


def convert_dtype_string(df, col):
    """
    Converts a string column to the extension dtype StringDtype
    :param df:
    :param col:
    :return:
    """
    dtype = df[col].dtype.__repr__()
    if dtype in {"StringDtype"}:
        return df[col]
    else:
        return df[col].convert_dtypes().copy()


# Converting dtypes
def get_dtype_lookup_df():
    dataframe_dtypes = ["date", "varchar_array", "bool", "int", "float", "string"]
    postgres_descriptions = [
        "timestamp with time zone",
        "ARRAY",
        "boolean",
        "bigint",
        "numeric",
        "character varying",
    ]
    postgres_dtypes = [
        TIMESTAMP(timezone=True),
        ARRAY(item_type=VARCHAR),
        BOOLEAN,
        BIGINT,
        NUMERIC,
        VARCHAR,
    ]
    data = {
        "dataframe_dtype": dataframe_dtypes,
        "postgres_description": postgres_descriptions,
        "postgres_dtype": postgres_dtypes,
    }
    lookup_df = pd.DataFrame(
        data=data,
    )
    return lookup_df


def get_merged_dtype_df(dtype_df, merge_col):
    lookup_df = get_dtype_lookup_df()
    merged_dtype_df = dtype_df.merge(lookup_df, on=merge_col, how="left")
    return merged_dtype_df


def convert_dtypes(dtype_dict, from_dtype="dataframe_dtype", to_dtype="postgres_dtype"):
    data = {
        "column_name": list(dtype_dict.keys()),
        from_dtype: list(dtype_dict.values()),
    }
    dtype_df = pd.DataFrame(data=data)
    merged_dtype_df = get_merged_dtype_df(dtype_df=dtype_df, merge_col=from_dtype)
    converted_dtype_dict = merged_dtype_df.set_index("column_name")[to_dtype].to_dict()
    return converted_dtype_dict


# Dataframe Conversions
def pre_convert_data(df):
    """
    Standardizes dataframes before checking column types
    :param df:
    :return:
    """
    df.fillna(np.nan, inplace=True)
    df = df.convert_dtypes().copy()
    return df


def convert_dataframe_columns(df, dtype_dict):
    """
    Converts each column to its appropriate data type
    :param df:
    :return:
    """

    for col, dtype in dtype_dict.items():
        # Dates
        if check_dtype_date(dtype=dtype):
            df[col] = convert_dtype_date(df=df, col=col)
        # Arrays
        elif check_dtype_array(dtype=dtype):
            df[col] = convert_dtype_array(df=df, col=col)
        # Booleans
        elif check_dtype_boolean(dtype=dtype):
            df[col] = convert_dtype_boolean(df=df, col=col)
        # Ints
        elif check_dtype_int(dtype=dtype):
            df[col] = convert_dtype_int(df=df, col=col)
        # Floats
        elif check_dtype_float(dtype=dtype):
            df[col] = convert_dtype_float(df=df, col=col)
        # Strings
        elif check_dtype_string(dtype=dtype):
            df[col] = convert_dtype_string(df=df, col=col)
        else:
            raise Exception(f"Dtype of {col} could not be determined")

    return df
