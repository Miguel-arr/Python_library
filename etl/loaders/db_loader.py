class DBLoader:
    @staticmethod
    def to_database(data, table_name, connection, if_exists='replace'):
        """Write data to a database table."""
        data.to_sql(table_name, connection, if_exists=if_exists, index=False)
