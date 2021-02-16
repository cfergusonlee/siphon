import pandas as pd
import numpy as np
import pytest
from sqlalchemy.dialects.postgresql import (
    TIMESTAMP,
    BIGINT,
    VARCHAR,
    NUMERIC,
    BOOLEAN,
    ARRAY,
)

from siphon.type_conversion_utils import (
    convert_to_list,
    convert_to_tuple,
    convert_to_boolean,
    convert_dtype_array,
    convert_dtype_string,
    convert_dtype_float,
    convert_dtype_int,
    convert_dtype_boolean,
    convert_dtypes,
    convert_dataframe_columns,
    convert_dtype_date,
    pre_convert_data,
)

from siphon.type_checking_utils import check_col_tuple, get_dataframe_dtypes


@pytest.fixture
def test_df():
    boolean_data = {
        "booleans": [True, False, True],
        "missing_value_booleans": [True, pd.NA, False],
        "float_booleans": [1.0, 0, 1.0],
        "int_booleans": [1, 0, 1],
        "string_booleans": ["True", "False", "True"],
        "string_abbreviated_booleans": ["T", "F", "T"],
        "string_missing_value_booleans": ["True", pd.NA, "False"],
        "string_abbreviated_missing_value_booleans": ["T", pd.NA, "F"],
        "string_abbreviated_missing_value_true": ["T", pd.NA, "T"],
        "string_abbreviated_missing_value_false": ["F", pd.NA, "F"],
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
        "missing_value_list": [["1", "2", "3"], pd.NA, ["cat", "dog"]],
    }

    tuple_data = {
        "tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_tuple": ['("1", "2", "3")', '("a", "b")', '("cat", "dog")'],
        "missing_value_tuple": [("1", "2", "3"), pd.NA, ("cat", "dog")],
    }

    numeric_data = {
        "floats": [123.3, 2.3, 9.0],
        "int_floats": [22.0, 9.0, 7.0],
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
    return df


# Convert column values
def test_convert_to_list(test_df):
    expected_values = {
        "list": [["1", "2", "3"], ["a", "b"], ["cat", "dog"]],
        "string_list": [["1", "2", "3"], ["a", "b"], ["cat", "dog"]],
        "missing_value_list": [["1", "2", "3"], np.nan, ["cat", "dog"]],
        "tuple": [["1", "2", "3"], ["a", "b"], ["cat", "dog"]],
        "string_tuple": [["1", "2", "3"], ["a", "b"], ["cat", "dog"]],
        "missing_value_tuple": [["1", "2", "3"], np.nan, ["cat", "dog"]],
    }
    test_df = pre_convert_data(test_df)
    for col, expected_value in expected_values.items():
        test_df[col] = test_df[col].apply(convert_to_list).copy()
        actual_value = test_df[col].values.tolist()
        assert actual_value == expected_value


def test_convert_to_tuple(test_df):
    expected_values = {
        "list": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_list": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "missing_value_list": [("1", "2", "3"), np.nan, ("cat", "dog")],
        "tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "missing_value_tuple": [("1", "2", "3"), np.nan, ("cat", "dog")],
    }
    test_df = pre_convert_data(test_df)
    for col, expected_value in expected_values.items():
        test_df[col] = test_df[col].apply(convert_to_tuple).copy()
        actual_value = test_df[col].values.tolist()
        assert actual_value == expected_value


def test_convert_to_boolean(test_df):
    expected_values = {
        "booleans": [True, False, True],
        "missing_value_booleans": [True, pd.NA, False],
        "float_booleans": [1.0, 0, 1.0],
        "int_booleans": [1, 0, 1],
        "string_booleans": [True, False, True],
        "string_abbreviated_booleans": [True, False, True],
        "string_missing_value_booleans": [True, pd.NA, False],
        "string_abbreviated_missing_value_booleans": [True, pd.NA, False],
        "string_abbreviated_missing_value_true": [True, pd.NA, True],
        "string_abbreviated_missing_value_false": [False, pd.NA, False],
    }
    test_df = pre_convert_data(test_df)
    for col, expected_value in expected_values.items():
        actual_value = test_df[col].apply(convert_to_boolean).values.tolist()
        assert actual_value == expected_value, f"Col: {col}"


# Convert Dtypes
def test_convert_dtype_array(test_df):
    expected_values = {
        "list": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_list": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "missing_value_list": [("1", "2", "3"), pd.NA, ("cat", "dog")],
        "tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "string_tuple": [("1", "2", "3"), ("a", "b"), ("cat", "dog")],
        "missing_value_tuple": [("1", "2", "3"), pd.NA, ("cat", "dog")],
    }
    for col, expected_value in expected_values.items():
        test_df[col] = convert_dtype_array(test_df, col)
        actual_value = test_df[col].values.tolist()
        assert actual_value == expected_value


def test_convert_dtype_boolean(test_df):
    expected_values = {
        "booleans": [True, False, True],
        "missing_value_booleans": [True, pd.NA, False],
        "float_booleans": [1.0, 0, 1.0],
        "int_booleans": [1, 0, 1],
        "string_booleans": [True, False, True],
        "string_abbreviated_booleans": [True, False, True],
        "string_missing_value_booleans": [True, pd.NA, False],
        "string_abbreviated_missing_value_booleans": [True, pd.NA, False],
        "string_abbreviated_missing_value_true": [True, pd.NA, True],
        "string_abbreviated_missing_value_false": [False, pd.NA, False],
    }
    for col, expected_value in expected_values.items():
        actual_value = list(convert_dtype_boolean(test_df, col).values)
        assert actual_value == expected_value, f"Col: {col}"


def test_convert_dtype_date(test_df):
    expected_values = {
        "concatenated_date_string": [
            np.datetime64("1984-08-15 00:00:00+0000"),
            np.datetime64("1984-09-20 00:00:00+0000"),
            np.datetime64("1984-03-18 00:00:00+0000"),
        ],
        "birthdate": [
            np.datetime64("1984-08-15 00:00:00+0000"),
            np.datetime64("1984-09-20 00:00:00+0000"),
            np.datetime64("1984-03-18 00:00:00+0000"),
        ],
        "start_date": [
            np.datetime64("2020-03-18 00:00:00+0000"),
            np.datetime64("2020-02-18 00:00:00+0000"),
            np.datetime64("2020-11-18 00:00:00+0000"),
        ],
        "end_date": [
            np.datetime64("2020-03-18 00:00:00+0000"),
            np.datetime64("2020-02-18 00:00:00+0000"),
            np.datetime64("2020-11-18 00:00:00+0000"),
        ],
        "date_of_birth": [
            np.datetime64("1984-08-15 00:00:00+0000"),
            np.datetime64("1984-09-20 00:00:00+0000"),
            np.datetime64("1984-03-18 00:00:00+0000"),
        ],
    }
    for col, expected_value in expected_values.items():
        actual_value = list(convert_dtype_date(test_df, col).values)
        assert actual_value == expected_value, f"Col: {col}"


def test_convert_dtype_float(test_df):
    expected_values = {
        "floats": [123.3, 2.3, 9.0],
        "int_floats": [22.0, 9.0, 7.0],
        "string_floats": ["123.3", "2.3", "9.0"],
        "ints": [1, 13, 9000],
        "int_strings": ["2", "300", "430"],
    }
    for col, expected_value in expected_values.items():
        actual_value = list(convert_dtype_float(test_df, col).values)
        assert actual_value == expected_value, f"Col: {col}"


def test_convert_dtype_int(test_df):
    expected_values = {
        "floats": [123.3, 2.3, 9.0],
        "int_floats": [22, 9, 7],
        "string_floats": ["123.3", "2.3", "9.0"],
        "ints": [1, 13, 9000],
        "int_strings": ["2", "300", "430"],
    }
    for col, expected_value in expected_values.items():
        actual_value = list(convert_dtype_int(test_df, col).values)
        assert actual_value == expected_value, f"Col: {col}"


def test_convert_dtype_string(test_df):
    expected_values = {
        "string_booleans": ["True", "False", "True"],
        "string_abbreviated_booleans": ["T", "F", "T"],
        "string_missing_value_booleans": ["True", pd.NA, "False"],
        "string_abbreviated_missing_value_booleans": ["T", pd.NA, "F"],
        "string_abbreviated_missing_value_true": ["T", pd.NA, "T"],
        "string_abbreviated_missing_value_false": ["F", pd.NA, "F"],
        "concatenated_date_string": ["19840815", "19840920", "19840318"],
        "string_list": ['["1", "2", "3"]', '["a", "b"]', '["cat", "dog"]'],
        "string_tuple": ['("1", "2", "3")', '("a", "b")', '("cat", "dog")'],
        "string_floats": ["123.3", "2.3", "9.0"],
        "int_strings": ["2", "300", "430"],
        "strings": ["octopus", "lion", "st. bernard"],
    }
    for col, expected_value in expected_values.items():
        actual_value = list(convert_dtype_string(test_df, col).values)
        assert actual_value == expected_value, f"Col: {col}"


def test_convert_dataframe_columns(test_df):
    expected_values = {
        "booleans": "BooleanDtype",
        "missing_value_booleans": "BooleanDtype",
        "float_booleans": "Int64Dtype()",
        "int_booleans": "Int64Dtype()",
        "string_booleans": "BooleanDtype",
        "string_abbreviated_booleans": "BooleanDtype",
        "string_missing_value_booleans": "BooleanDtype",
        "string_abbreviated_missing_value_booleans": "BooleanDtype",
        "string_abbreviated_missing_value_true": "BooleanDtype",
        "string_abbreviated_missing_value_false": "BooleanDtype",
        "concatenated_date_string": "datetime64[ns, UTC]",
        "birthdate": "StringDtype",
        "start_date": "datetime64[ns, UTC]",
        "end_date": "datetime64[ns, UTC]",
        "date_of_birth": "datetime64[ns, UTC]",
        "list": "dtype('O')",
        "string_list": "dtype('O')",
        "missing_value_list": "dtype('O')",
        "tuple": "dtype('O')",
        "string_tuple": "dtype('O')",
        "missing_value_tuple": "dtype('O')",
        "floats": "Float64Dtype()",
        "string_floats": "StringDtype",
        "ints": "Int64Dtype()",
        "int_strings": "StringDtype",
        "strings": "StringDtype",
    }
    test_df = pre_convert_data(test_df)
    df_dtype_dict = get_dataframe_dtypes(test_df)
    converted_test_df = convert_dataframe_columns(test_df, df_dtype_dict)
    for col, dtype in expected_values.items():
        assert converted_test_df[col].dtype.__repr__() == dtype, f"{col}"
        # Array column values need further inspection
        if dtype in {"dtype('O')"}:
            assert check_col_tuple(converted_test_df[col])


def test_convert_dtypes(test_df):
    expected_values = {
        "booleans": BOOLEAN,
        "missing_value_booleans": BOOLEAN,
        "float_booleans": BIGINT,
        "int_booleans": BIGINT,
        "string_booleans": BOOLEAN,
        "string_abbreviated_booleans": BOOLEAN,
        "string_missing_value_booleans": BOOLEAN,
        "string_abbreviated_missing_value_booleans": BOOLEAN,
        "string_abbreviated_missing_value_true": BOOLEAN,
        "string_abbreviated_missing_value_false": BOOLEAN,
        "concatenated_date_string": TIMESTAMP(timezone=True),
        "birthdate": VARCHAR,
        "start_date": TIMESTAMP(timezone=True),
        "end_date": TIMESTAMP(timezone=True),
        "date_of_birth": TIMESTAMP(timezone=True),
        "list": ARRAY(item_type=VARCHAR),
        "string_list": ARRAY(item_type=VARCHAR),
        "missing_value_list": ARRAY(item_type=VARCHAR),
        "tuple": ARRAY(item_type=VARCHAR),
        "string_tuple": ARRAY(item_type=VARCHAR),
        "missing_value_tuple": ARRAY(item_type=VARCHAR),
        "floats": NUMERIC,
        "string_floats": VARCHAR,
        "ints": BIGINT,
        "int_strings": VARCHAR,
        "strings": VARCHAR,
    }
    test_df = pre_convert_data(test_df)
    df_dtype_dict = get_dataframe_dtypes(test_df)
    db_dtype_dict = convert_dtypes(df_dtype_dict)
    for col, expected_dtype in expected_values.items():
        actual_visit_name = db_dtype_dict[col].__visit_name__
        expected_visit_name = expected_dtype.__visit_name__
        assert (
            actual_visit_name == expected_visit_name
        ), f"Col: {col}, Dtype: {type(db_dtype_dict[col])}"
