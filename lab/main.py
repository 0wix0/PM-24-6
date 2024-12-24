import csv
import pickle
from datetime import datetime
from typing import List, Dict, Union, Optional

# Внутреннее представление таблицы
class Table:
    def __init__(self, headers: List[str], rows: List[List[Union[str, int, float, bool, datetime]]]):
        self.headers = headers
        self.rows = rows
        self.column_types = {header: str for header in headers}

    def __repr__(self):
        return f"Table(headers={self.headers}, rows={len(self.rows)} rows)"

# CSV Module
class CSVModule:
    @staticmethod
    def load_table(*file_paths: str, auto_detect_types: bool = False) -> Table:
        tables = []
        for file_path in file_paths:
            with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                rows = [row for row in reader]
                tables.append(Table(headers, rows))

        # Проверка структуры таблиц
        for i in range(1, len(tables)):
            if tables[i].headers != tables[0].headers:
                raise ValueError("Заголовки столбцов не совпадают в файлах.")

        combined_rows = [row for table in tables for row in table.rows]
        table = Table(tables[0].headers, combined_rows)

        if auto_detect_types:
            TableOperations.detect_column_types(table)

        return table

    @staticmethod
    def save_table(table: Table, file_path: str, max_rows: Optional[int] = None):
        if max_rows:
            num_files = (len(table.rows) + max_rows - 1) // max_rows
            for i in range(num_files):
                part_path = f"{file_path}_part{i + 1}.csv"
                with open(part_path, mode='w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(table.headers)
                    writer.writerows(table.rows[i * max_rows:(i + 1) * max_rows])
        else:
            with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(table.headers)
                writer.writerows(table.rows)

# Pickle Module
class PickleModule:
    @staticmethod
    def load_table(*file_paths: str) -> Table:
        tables = []
        for file_path in file_paths:
            with open(file_path, 'rb') as file:
                tables.append(pickle.load(file))

        # Проверка структуры таблиц
        for i in range(1, len(tables)):
            if tables[i].headers != tables[0].headers:
                raise ValueError("Заголовки столбцов не совпадают в файлах.")

        combined_rows = [row for table in tables for row in table.rows]
        return Table(tables[0].headers, combined_rows)

    @staticmethod
    def save_table(table: Table, file_path: str, max_rows: Optional[int] = None):
        if max_rows:
            num_files = (len(table.rows) + max_rows - 1) // max_rows
            for i in range(num_files):
                part_path = f"{file_path}_part{i + 1}.pkl"
                with open(part_path, 'wb') as file:
                    pickle.dump(Table(table.headers, table.rows[i * max_rows:(i + 1) * max_rows]), file)
        else:
            with open(file_path, 'wb') as file:
                pickle.dump(table, file)

# Basic Operations Module
class TableOperations:
    @staticmethod
    def concat(table1: Table, table2: Table) -> Table:
        if table1.headers != table2.headers:
            raise ValueError("Таблицы имеют разные заголовки.")
        return Table(table1.headers, table1.rows + table2.rows)

    @staticmethod
    def split(table: Table, row_number: int) -> (Table, Table):
        if not (0 <= row_number <= len(table.rows)):
            raise ValueError("Номер строки выходит за пределы диапазона.")
        return Table(table.headers, table.rows[:row_number]), Table(table.headers, table.rows[row_number:])

    @staticmethod
    def detect_column_types(table: Table):
        for col_index, header in enumerate(table.headers):
            col_values = [row[col_index] for row in table.rows if row[col_index] is not None]
            if all(isinstance(val, int) for val in col_values):
                table.column_types[header] = int
            elif all(isinstance(val, float) for val in col_values):
                table.column_types[header] = float
            elif all(isinstance(val, bool) for val in col_values):
                table.column_types[header] = bool
            elif all(isinstance(val, datetime) for val in col_values):
                table.column_types[header] = datetime
            else:
                table.column_types[header] = str

    @staticmethod
    def set_column_types(table: Table, types_dict: Dict[Union[int, str], type]):
        for key, value in types_dict.items():
            if isinstance(key, int):
                if key < 0 or key >= len(table.headers):
                    raise ValueError(f"Индекс столбца {key} выходит за пределы диапазона.")
                header = table.headers[key]
            elif isinstance(key, str):
                if key not in table.headers:
                    raise ValueError(f"Столбец {key} не найден в заголовках.")
                header = key
            else:
                raise ValueError(f"Некорректный тип ключа: {type(key)}. Ожидается int или str.")

            if value not in [int, float, bool, str, datetime]:
                raise ValueError(f"Некорректный тип: {value}. Допустимые типы: int, float, bool, str, datetime.")

            table.column_types[header] = value

    @staticmethod
    def _convert_to_numeric(value, target_type):
        try:
            return target_type(value)
        except ValueError:
            raise ValueError(f"Невозможно преобразовать значение {value} в {target_type}.")

    @staticmethod
    def _apply_operation(table: Table, column1: Union[int, str], column2: Union[int, str], result_column: str, operation):
        col_index1 = column1 if isinstance(column1, int) else table.headers.index(column1)
        col_index2 = column2 if isinstance(column2, int) else table.headers.index(column2)
        result_values = []
        for row in table.rows:
            try:
                val1 = TableOperations._convert_to_numeric(row[col_index1], float)
                val2 = TableOperations._convert_to_numeric(row[col_index2], float)
                result_values.append(operation(val1, val2))
            except (ValueError, TypeError):
                raise ValueError(f"Невозможно выполнить операцию для значений {row[col_index1]} и {row[col_index2]}.")
        table.headers.append(result_column)
        for i, value in enumerate(result_values):
            table.rows[i].append(value)

    @staticmethod
    def add(table: Table, column1: Union[int, str], column2: Union[int, str], result_column: str):
        TableOperations._apply_operation(table, column1, column2, result_column, lambda x, y: x + y)

    @staticmethod
    def sub(table: Table, column1: Union[int, str], column2: Union[int, str], result_column: str):
        TableOperations._apply_operation(table, column1, column2, result_column, lambda x, y: x - y)

    @staticmethod
    def mul(table: Table, column1: Union[int, str], column2: Union[int, str], result_column: str):
        TableOperations._apply_operation(table, column1, column2, result_column, lambda x, y: x * y)

    @staticmethod
    def div(table: Table, column1: Union[int, str], column2: Union[int, str], result_column: str):
        TableOperations._apply_operation(table, column1, column2, result_column, lambda x, y: x / y if y != 0 else float('inf'))

    @staticmethod
    def filter_rows(table: Table, bool_list: List[bool], copy_table: bool = False):
        if len(bool_list) != len(table.rows):
            raise ValueError("Длина bool_list должна совпадать с количеством строк в таблице.")
        filtered_rows = [row for row, include in zip(table.rows, bool_list) if include]
        return Table(table.headers, filtered_rows) if copy_table else filtered_rows

    @staticmethod
    def print_table(table: Table):
        print("\t".join(table.headers))
        for row in table.rows:
            print("\t".join(map(str, row)))

    @staticmethod
    def get_rows_by_number(table: Table, start: int, stop: Optional[int] = None, copy_table: bool = False):
        stop = stop if stop is not None else start + 1
        if not (0 <= start < len(table.rows)) or not (0 <= stop <= len(table.rows)):
            raise ValueError("Индексы строк выходят за пределы диапазона.")
        rows = table.rows[start:stop]
        return Table(table.headers, rows) if copy_table else rows

    @staticmethod
    def get_rows_by_index(table: Table, *indexes, copy_table: bool = False):
        rows = [row for row in table.rows if row[0] in indexes]
        return Table(table.headers, rows) if copy_table else rows

# Примеры использования

# 1. Загрузка таблицы
file_path = 'test.csv'
table = CSVModule.load_table(file_path, auto_detect_types=True)
print("Исходная таблица:")
TableOperations.print_table(table)

# 6. Объединение таблиц
concat_table = TableOperations.concat(table, table)
print("\nТаблица после объединения:")
TableOperations.print_table(concat_table)

# 7. Разделение таблицы
part1, part2 = TableOperations.split(table, row_number=2)
print("\nПервая часть таблицы после разделения:")
TableOperations.print_table(part1)
print("\nВторая часть таблицы после разделения:")
TableOperations.print_table(part2)

# 8. Определение типов столбцов
TableOperations.detect_column_types(table)
print("\nТипы столбцов:")
print(table.column_types)

# 9. Выполнение арифметических операций
TableOperations.add(table, 'Age', 'Age', 'AgeTwice')
print("\nТаблица после добавления нового столбца 'AgeTwice':")
TableOperations.print_table(table)

TableOperations.sub(table, 'Age', 'id', 'AgeMinusID')
print("\nТаблица после вычитания столбца 'id' из 'Age':")
TableOperations.print_table(table)

TableOperations.mul(table, 'Age', 'id', 'AgeTimesID')
print("\nТаблица после умножения 'Age' на 'id':")
TableOperations.print_table(table)

TableOperations.div(table, 'Age', 'id', 'AgeDivID')
print("\nТаблица после деления 'Age' на 'id':")
TableOperations.print_table(table)

# 10. Фильтрация строк
bool_filter = [row[4] for row in table.rows]  # Индекс столбца BoolVal = 4
filtered_table = TableOperations.filter_rows(table, bool_filter, copy_table=True)
print("\nОтфильтрованная таблица по BoolVal:")
TableOperations.print_table(filtered_table)

# 11. Сравнение значений в столбце Age
greater_than_30 = [float(row[2]) > 30 for row in table.rows]  # Индекс столбца Age = 2
filtered_by_age = TableOperations.filter_rows(table, greater_than_30, copy_table=True)
print("\nОтфильтрованная таблица (Age > 30):")
TableOperations.print_table(filtered_by_age)

# 12. Изменение типов столбцов
new_types = {'Age': float, 'id': int}
TableOperations.set_column_types(table, new_types)
print("\nОбновлённые типы столбцов:")
print(table.column_types)

# 13. Получение значений столбца
age_values = [row[table.headers.index('Age')] for row in table.rows]
print("\nЗначения столбца 'Age':", age_values)

# 15. Получение строк по номеру
rows_by_number = TableOperations.get_rows_by_number(table, 1, 3, copy_table=True)
print("\nСтроки таблицы по номерам (1-3):")
TableOperations.print_table(rows_by_number)

# 16. Получение строк по индексу
rows_by_index = TableOperations.get_rows_by_index(table, '1', '2', copy_table=True)
print("\nСтроки таблицы по индексам ('1', '2'):")
TableOperations.print_table(rows_by_index)
