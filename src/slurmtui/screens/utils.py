from typing import Dict


class ColumnManager:
    def __init__(self, initial_columns: Dict[str, bool]):
        # OrderedDict to store column names and their enabled status
        self.columns = initial_columns.copy()

    def enable_column(self, column_name):
        """Enable a column by name."""
        if column_name in self.columns:
            self.columns[column_name] = True

    def disable_column(self, column_name):
        """Disable a column by name."""
        if column_name in self.columns:
            self.columns[column_name] = False

    def get_enabled_columns(self):
        """Return a list of enabled column names in order."""
        return [col for col, enabled in self.columns.items() if enabled]

    def get_all_columns(self):
        """Return a list of all column names in order."""
        return list(self.columns.keys())
