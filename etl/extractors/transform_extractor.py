import pandas as pd

class TransformOperations:
    @staticmethod
    def filter_rows(data, condition):
        """Filter rows based on a condition function."""
        return data[data.apply(condition, axis=1)]

    @staticmethod
    def add_column(data, column_name, value_function):
        """Add a new column to the dataset."""
        data[column_name] = data.apply(value_function, axis=1)
        return data

    @staticmethod
    def aggregate(data, key, aggregation):
        """Aggregate data based on a key column."""
        return data.groupby(key).agg(aggregation).reset_index()
