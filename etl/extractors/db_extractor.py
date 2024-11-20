import pandas as pd

class DBExtractor:
    @staticmethod
    def from_database(query, connection):
        """Extract data from a database."""
        return pd.read_sql_query(query, connection)
