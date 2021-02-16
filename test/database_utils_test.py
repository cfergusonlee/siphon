import pandas as pd
import numpy as np
import pytest

from siphon.database_utils import (
    get_reference_table,
    check_table_exists,
    declare_primary_key,
)

# from siphon.type_checking_utils import
from siphon.PostgresConnection import PostgresConnection
from siphon.PostgresDatabase import PostgresDatabase


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


def test_check_table_exists():
    expected_values = [
        {
            "schema": "test",
            "table": "mock",
            "value": True,
        },
        {"schema": "test", "table": "dne", "value": False},
        {
            "schema": "dne",
            "table": "dne",
            "value": False,
        },
    ]
    with PostgresConnection(database_var="SIPHON_DATABASE_URL") as connection:
        for kwarg in expected_values:
            actual_value = check_table_exists(
                kwarg["table"], kwarg["schema"], connection.connection
            )
            assert actual_value == kwarg["value"], f"kwargs: {kwarg}"
