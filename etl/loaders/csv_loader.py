class CSVLoader:
    @staticmethod
    def to_csv(data, filepath, index=False):
        """Write data to a CSV file."""
        data.to_csv(filepath, index=index)