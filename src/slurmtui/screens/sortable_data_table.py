# Copied and modified from https://gitlab.com/pjhdekoning/textual-sortable-datatable
from dataclasses import dataclass
from typing import Any, Callable, Final, List, Union

from typing_extensions import Self

try:
    import pandas as pd
    DataFrame = pd.DataFrame
except ImportError:
    pd = None
    # pylint: disable=invalid-name
    DataFrame = None
    # pylint: enable=invalid-name

from rich.text import Text
from textual import on
from textual.binding import Binding
from textual.widgets import DataTable
from textual.widgets.data_table import CellKey, Column, ColumnKey

SORT_INDICATOR_UP: Final[str] = ' \u25b4'
SORT_INDICATOR_DOWN: Final[str] = ' \u25be'

# ll return values are tuples and Python can compare them without
# a type error — numeric values sort before strings (rank 0 < rank 1)
def sort_column(value: Any) -> Any:
    if value is None:
        return (1, '')

    if isinstance(value, Text):
        value = value.plain

    try:
        return (0, float(value))
    except (ValueError, TypeError):
        pass

    return (1, value)


@dataclass
class Sort:
    key: Union[ColumnKey, None] = None
    label: str = ''
    direction: bool = False

    def reverse(self) -> None:
        self.direction = not self.direction

    @property
    def indicator(self) -> str:
        return SORT_INDICATOR_UP if self.direction else SORT_INDICATOR_DOWN


class SortableDataTable(DataTable):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sort = Sort()
        self.cursor_type = 'row'  # type: ignore
        self.show_row_labels = False
        self.sort_function: Callable[[Any], Any] = sort_column

    @property
    def sort_column(self) -> Sort:
        return self._sort

    def _column_key(self, _label: str) -> Union[ColumnKey, None]:
        for key, label in self.columns.items():
            if label.label.plain == _label:
                return key

        return None

    @property
    def sort_column_label(self) -> Union[str, None]:
        if self._sort.key is None:
            return None

        label: Text = self.columns[self._sort.key].label
        label.remove_suffix(self._sort.indicator)
        return str(label)

    def clear(self, columns: bool = False) -> Self:
        super().clear(columns)
        # _sort contains a column key that becomes invalid when clearing the columns, so reset it.
        self._sort = Sort()
        return self

    def column_names(self) -> List[Column]:
        data = self.columns.copy()
        if self._sort.key:
            column = data[self._sort.key]
            column.label.remove_suffix(self._sort.indicator)

        return list(data.values())

    def set_data(self, data: DataFrame) -> None:  # type: ignore
        if DataFrame is None:
            self.notify('Pandas is not installed', severity='error')
            return

        self.clear(columns=True)
        self._sort = Sort()

        if data is None or len(data) == 0:
            self.border_title = 'Results (0 rows)'
            return

        self.border_title = f'Results ({len(data)} rows)'
        self.add_columns(*data.columns.values.tolist())
        data = data.itertuples(index=False, name=None)
        for index, row in enumerate(data, 1):
            label = Text(str(index), style='#B0FC38 italic')
            self.add_row(*row, label=label)

    @on(DataTable.HeaderSelected)
    def header_clicked(self, header: DataTable.HeaderSelected) -> None:
        self.sort_on_column(header.column_key)

    def sort_on_column(self, key: Union[ColumnKey, str], direction: Union[bool, None] = None) -> None:
        if isinstance(key, str):
            key = self._column_key(key)  # type: ignore
            if key is None:
                return

        assert isinstance(key, ColumnKey)

        if self._sort.key is not None:
            column = self.columns[self._sort.key]
            column.label.remove_suffix(self._sort.indicator)
            self._update_column_width(self._sort.key)

        sort_value = Sort(key=key)
        if self._sort.key == sort_value.key:
            sort_value = self._sort
            sort_value.reverse()

        assert sort_value.key

        self.columns[key].label += sort_value.indicator
        self._update_column_width(key)

        if direction is not None:
            sort_value.direction = direction

        try:
            self.sort(sort_value.key, reverse=sort_value.direction, key=self.sort_function)
            self._sort = sort_value
        except TypeError as e:
            self.columns[key].label.remove_suffix(self._sort.indicator)
            self.notify(f'Error sorting on column: {self.columns[key]} {e}', severity='error', timeout=15)

    def _update_column_width(self, key: ColumnKey) -> None:
        if len(self.rows) == 0:
            return

        self._update_column_widths({CellKey(row_key=next(iter(self.rows)), column_key=key)})

