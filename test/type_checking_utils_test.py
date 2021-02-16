import pandas as pd
import numpy as np
import pytest

from siphon.type_checking_utils import (
    check_dtype_boolean,
    check_dtype_date,
    check_dtype_float,
    check_dtype_int,
    check_dtype_string,
    check_col_boolean,
    check_col_list,
    check_col_tuple,
    check_col_tuple_or_list,
    check_dtype_array,
    get_dataframe_dtypes,
    get_database_dtypes,
)
from siphon.type_conversion_utils import convert_dataframe_columns, convert_dtypes
from siphon.PostgresConnection import PostgresConnection


@pytest.fixture
def test_df():
    boolean_data = {
        "booleans": [True, False, True],
        "missing_value_booleans": [True, np.nan, False],
        "float_booleans": [1.0, 0, 1.0],
        "int_booleans": [1, 0, 1],
        "string_booleans": ["True", "False", "True"],
        "string_abbreviated_booleans": ["T", "F", "T"],
        "string_missing_value_booleans": ["True", np.nan, "False"],
        "string_abbreviated_missing_value_booleans": ["T", np.nan, "F"],
        "string_abbreviated_missing_value_true": ["T", np.nan, "T"],
        "string_abbreviated_missing_value_false": ["F", np.nan, "F"],
    }

    date_data = {
        "concatenated_date_string": ["19840815", "19840920", "19840318"],
        "birthdate": ["19840815", "19840920", "19840318"],
        "start_date": ["2020-03-18", "2020-02-18", "2020-11-18"],
        "end_date": ["2020/03/18", "2020/02/18", "2020/11/18"],
        "date_of_birth": ["August 15, 1984", "September 20, 1984", "March 18, 1984"],
    }

    list_data = {
        "list": [["1", "2", "3"], ["a", "b"], ["cat", "dog"]],
        "string_list": ['["1", "2", "3"]', '["a", "b"]', '["cat", "dog"]'],
        "missing_value_list": [["1", "2", "3"], np.nan, ["cat", "dog"]],
    }

    tuple_data = {
        "tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_tuple": ['("1", "2", "3")', '("a", "b")', '("cat", "dog")'],
        "missing_value_tuple": [("1", "2", "3"), np.nan, ("cat", "dog")],
    }

    numeric_data = {
        "floats": [123.3, 2.3, 9.0],
        "string_floats": ["123.3", "2.3", "9.0"],
        "ints": [1, 13, 9000],
        "int_strings": ["2", "300", "430"],
    }

    string_data = {
        "strings": ["octopus", "lion", "st. bernard"],
    }
    data = {}
    for data_type in [
        boolean_data,
        date_data,
        list_data,
        tuple_data,
        numeric_data,
        string_data,
    ]:
        data.update(data_type)

    df = pd.DataFrame(data)
    for col in [
        "concatenated_date_string",
        "birthdate",
        "start_date",
        "end_date",
    ]:
        df[col] = pd.to_datetime(df[col], utc=True).copy()
    df = df.convert_dtypes().copy()
    return df


# Check Column Values
def test_check_list_col(test_df):
    expected_values = {
        "list": True,
        "string_list": True,
        "missing_value_list": True,
        "tuple": False,
        "string_tuple": False,
        "missing_value_tuple": False,
    }
    for col, expected_value in expected_values.items():
        assert check_col_list(test_df[col]) == expected_value


def test_check_tuple_col(test_df):
    expected_values = {
        "list": False,
        "string_list": False,
        "missing_value_list": False,
        "tuple": True,
        "string_tuple": True,
        "missing_value_tuple": True,
    }
    for col, expected_value in expected_values.items():
        assert check_col_tuple(test_df[col]) == expected_value


def test_check_tuple_or_list_col(test_df):
    expected_values = {
        "list": True,
        "string_list": True,
        "missing_value_list": True,
        "tuple": True,
        "string_tuple": True,
        "missing_value_tuple": True,
    }
    for col, expected_value in expected_values.items():
        assert check_col_tuple_or_list(test_df[col]) == expected_value


def test_check_col_boolean(test_df):
    expected_values = {
        "booleans": True,
        "missing_value_booleans": True,
        "float_booleans": True,
        "int_booleans": True,
        "string_booleans": True,
        "string_abbreviated_booleans": True,
        "string_missing_value_booleans": True,
        "string_abbreviated_missing_value_booleans": True,
        "string_abbreviated_missing_value_true": True,
        "string_abbreviated_missing_value_false": True,
    }
    for col, expected_value in expected_values.items():
        assert check_col_boolean(test_df[col]) == expected_value, f"Column: {col}"


# Check Dtypes
def test_check_dtype_array(test_df):
    expected_values = {
        "list": True,
        "string_list": True,
        "missing_value_list": True,
        "tuple": True,
        "string_tuple": True,
        "missing_value_tuple": True,
    }
    for col, expected_value in expected_values.items():
        assert check_dtype_array(test_df, col) == expected_value


def test_check_dtype_boolean(test_df):
    expected_values = {
        "booleans": True,
        "missing_value_booleans": True,
        "float_booleans": False,
        "int_booleans": False,
        "string_booleans": True,
        "string_abbreviated_booleans": True,
        "string_missing_value_booleans": True,
        "string_abbreviated_missing_value_booleans": True,
        "string_abbreviated_missing_value_true": True,
        "string_abbreviated_missing_value_false": True,
    }
    for col, expected_value in expected_values.items():
        assert check_dtype_boolean(test_df, col) == expected_value, f"Column: {col}"


def test_check_dtype_date(test_df):
    expected_values = {
        "concatenated_date_string": True,
        "birthdate": True,
        "start_date": True,
        "end_date": True,
        "date_of_birth": True,
        "string_tuple": False,
    }
    for col, expected_value in expected_values.items():
        assert check_dtype_date(test_df, col) == expected_value, f"Column: {col}"


def test_check_dtype_float(test_df):
    expected_values = {
        "floats": True,
        "string_floats": False,
        "ints": False,
        "int_strings": False,
    }
    for col, expected_value in expected_values.items():
        assert (
            check_dtype_float(test_df, col) == expected_value
        ), f"Column: {col}, {test_df[col].dtype.__repr__()}"


def test_check_dtype_int(test_df):
    expected_values = {
        "floats": False,
        "string_floats": False,
        "ints": True,
        "int_strings": False,
    }
    for col, expected_value in expected_values.items():
        assert check_dtype_int(test_df, col) == expected_value, f"Column: {col}"


def test_check_dtype_string(test_df):
    expected_values = {
        "strings": True,
        "string_booleans": True,
        "string_list": True,
        "string_tuple": True,
        "floats": False,
        "string_floats": True,
    }
    for col, expected_value in expected_values.items():
        assert (
            check_dtype_string(test_df, col) == expected_value
        ), f"Column: {col}, {test_df[col].dtype.__repr__()}"


# Check Data Table
def test_get_dataframe_dtypes(test_df):
    expected_values = {
        "booleans": "bool",
        "missing_value_booleans": "bool",
        "float_booleans": "int",
        "int_booleans": "int",
        "string_booleans": "bool",
        "string_abbreviated_booleans": "bool",
        "string_missing_value_booleans": "bool",
        "string_abbreviated_missing_value_booleans": "bool",
        "string_abbreviated_missing_value_true": "bool",
        "string_abbreviated_missing_value_false": "bool",
        "concatenated_date_string": "date",
        "birthdate": "date",
        "start_date": "date",
        "end_date": "date",
        "date_of_birth": "date",
        "list": "varchar_array",
        "string_list": "varchar_array",
        "missing_value_list": "varchar_array",
        "tuple": "varchar_array",
        "string_tuple": "varchar_array",
        "missing_value_tuple": "varchar_array",
        "floats": "float",
        "string_floats": "string",
        "ints": "int",
        "int_strings": "string",
        "strings": "string",
    }
    df_dtype_dict = get_dataframe_dtypes(test_df)
    for col in test_df.columns:
        assert df_dtype_dict[col] == expected_values[col], f"Column: {col}"


def test_get_database_dtypes(test_df):
    expected_values = {
        "booleans": "boolean",
        "missing_value_booleans": "boolean",
        "float_booleans": "bigint",
        "int_booleans": "bigint",
        "string_booleans": "boolean",
        "string_abbreviated_booleans": "boolean",
        "string_missing_value_booleans": "boolean",
        "string_abbreviated_missing_value_booleans": "boolean",
        "string_abbreviated_missing_value_true": "boolean",
        "string_abbreviated_missing_value_false": "boolean",
        "concatenated_date_string": "timestamp with time zone",
        "birthdate": "timestamp with time zone",
        "start_date": "timestamp with time zone",
        "end_date": "timestamp with time zone",
        "date_of_birth": "timestamp with time zone",
        "list": "ARRAY",
        "string_list": "ARRAY",
        "missing_value_list": "ARRAY",
        "tuple": "ARRAY",
        "string_tuple": "ARRAY",
        "missing_value_tuple": "ARRAY",
        "floats": "numeric",
        "string_floats": "character varying",
        "ints": "bigint",
        "int_strings": "character varying",
        "strings": "character varying",
    }
    with PostgresConnection(database_var="SIPHON_DATABASE_URL") as connection:
        df_dtype_dict = get_dataframe_dtypes(test_df)
        test_df = convert_dataframe_columns(test_df, df_dtype_dict)
        dtype_param = convert_dtypes(
            dtype_dict=df_dtype_dict,
            from_dtype="dataframe_dtype",
            to_dtype="postgres_dtype",
        )
        test_df.to_sql(
            "mock",
            method="multi",
            if_exists="replace",
            dtype=dtype_param,
            schema="test",
            con=connection.connection,
            index=False,
        )

        db_dtype_dict = get_database_dtypes(
            table="mock", schema="test", connection=connection.connection
        )
        for col, expected_dtype in expected_values.items():
            assert db_dtype_dict[col] == expected_dtype, f"Col: {col}"
