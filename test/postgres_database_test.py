from siphon.PostgresDatabase import PostgresDatabase


def test_get_table():
    expected_values = [
        {
            "schema": "test",
            "table": "mock",
            "num_rows": 3,
            "num_columns": 26,
            "columns": {
                "booleans",
                "missing_value_booleans",
                "float_booleans",
                "int_booleans",
                "string_booleans",
                "string_abbreviated_booleans",
                "string_missing_value_booleans",
                "string_abbreviated_missing_value_booleans",
                "string_abbreviated_missing_value_true",
                "string_abbreviated_missing_value_false",
                "concatenated_date_string",
                "birthdate",
                "start_date",
                "end_date",
                "date_of_birth",
                "list",
                "string_list",
                "missing_value_list",
                "tuple",
                "string_tuple",
                "missing_value_tuple",
                "floats",
                "string_floats",
                "ints",
                "int_strings",
                "strings",
            },
        }
    ]
    db = PostgresDatabase(database_var="SIPHON_DATABASE_URL")
    for item in expected_values:

        df = db.get_table(table=item["table"], schema=item["schema"])
        assert df.shape[0] == item["num_rows"]
        assert df.shape[1] == item["num_columns"]
        assert set(df.columns) == item["columns"]


def test_export_table():
    ...
