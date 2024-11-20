import pandas as pd

class CSVExtractor:
    @staticmethod
    def from_csv(filepath):
        """Read data from a CSV file."""
        return pd.read_csv(filepath)
