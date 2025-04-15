import pandas as pd
import numpy as np
import re
import os
from typing import Dict, List, Tuple, Any, Optional

# --- Константы ---
# Индексы колонок (начиная с 0)
ARTIKUL_COL_IDX = 2          # Колонка с артикулом
FIRST_SIZE_COL_IDX = 5       # Первая колонка с размером/количеством

# --- Функции для очистки данных ---

def clean_artikul(artikul: Any) -> Optional[str]:
    """Очищает и валидирует артикул."""
    if pd.isna(artikul):
        return None
    artikul_str = str(artikul).strip()
    # Убираем возможное ".0" если pandas прочитал как float
    if artikul_str.endswith(".0"):
        artikul_str = artikul_str[:-2]
    # Проверяем, что это не пустая строка и похоже на артикул (например, содержит цифры)
    if artikul_str and re.search(r'\d', artikul_str):
        # Дополнительно убираем возможные непечатаемые символы или мусор в конце
        # Артикул может содержать буквы, цифры, -, /
        match = re.match(r'^[\w\-/]+', artikul_str)
        return match.group(0) if match else None
    return None

def clean_size(size: Any) -> Optional[str]:
    """Очищает и валидирует размер."""
    if pd.isna(size):
        return None
    # Сначала пытаемся преобразовать в строку
    try:
        size_str = str(size).strip().upper()
    except Exception: # На случай очень странных данных
        return None

    # Убираем возможное ".0" если pandas прочитал как float
    if size_str.endswith(".0"):
        size_str = size_str[:-2]

    # Если строка пустая после очистки
    if not size_str:
        return None

    # Проверка на типичный формат размера (цифры + буквы, просто цифры, цифры/цифры)
    # Добавлены также форматы типа '70/95', '164/92/98', '40-48'
    if re.fullmatch(r'(\d{2,3}[A-Z]{1,2}|\d{2,3}/\d{2,3}/\d{2,3}|\d{2,3}/\d{2,3}|\d{2,3}|\d{2}-\d{2})', size_str):
         # Дополнительная проверка для '40-48', т.к. паттерн выше его тоже ловит как \d{2}-\d{2}
         # но re.fullmatch требует точного совпадения всего паттерна
        return size_str

    # Если ни один паттерн не подошел
    # print(f"Предупреждение: Не удалось распознать как размер: '{size}' (тип: {type(size)})")
    return None


def clean_quantity(quantity: Any) -> int:
    """Очищает и валидирует количество, возвращает 0 при ошибке."""
    if pd.isna(quantity) or quantity == '':
        return 0
    try:
        # Убираем пробелы, заменяем запятую на точку для float конвертации
        quantity_str = str(quantity).replace(' ', '').replace(',', '.')
        # Конвертируем во float, затем в int, чтобы обработать числа типа "1.0"
        val = int(float(quantity_str))
        return max(0, val) # Количество не может быть отрицательным
    except (ValueError, TypeError):
        # Если не можем конвертировать, считаем количество нулевым
        # print(f"Предупреждение: Не удалось конвертировать количество '{quantity}' в число. Используется 0.")
        return 0

# --- Основная функция парсинга файла (исправленная логика) ---

def parse_supplier_excel(filename: str) -> Dict[str, Dict[str, int]]:
    """
    Парсит специфический формат XLS файла поставщика (логика от строки с размерами).

    Args:
        filename: Путь к файлу Excel.

    Returns:
        Словарь, где ключи - очищенные артикулы,
        значения - словари {очищенный размер: количество}.
    """
    data: Dict[str, Dict[str, int]] = {}
    if not os.path.exists(filename):
        print(f"Ошибка: Файл не найден '{filename}'")
        return data

    try:
        # Читаем все как строки, чтобы избежать авто-преобразования pandas/xlrd
        df = pd.read_excel(filename, header=None, engine='xlrd', dtype=str)
        # Заменяем 'nan' строки на реальные NaN для корректной работы isna() и др.
        df.replace('nan', np.nan, inplace=True)
    except ImportError:
         print("Ошибка: Библиотека xlrd не найдена. Установите ее: pip install xlrd")
         return data
    except Exception as e:
        print(f"Ошибка чтения файла '{filename}': {e}")
        return data

    print(f"Обработка файла: {filename}")
    num_rows = len(df)

    # Используем set для хранения индексов строк с количеством, которые уже были обработаны,
    # чтобы избежать двойной обработки, если размеры дублируются перед одной строкой количества
    processed_quantity_rows = set()

    for index in range(num_rows): # Идем по всем строкам, ища размеры
        current_row = df.iloc[index]

        # 1. Ищем размеры в текущей строке index
        # Проверяем, что в строке есть хоть какие-то значения начиная с колонки размеров
        if current_row.iloc[FIRST_SIZE_COL_IDX:].isna().all():
             continue # Если вся правая часть пустая, пропускаем

        row_values = current_row.iloc[FIRST_SIZE_COL_IDX:].tolist()
        cleaned_sizes = [clean_size(s) for s in row_values]

        # Если в текущей строке нет валидных размеров, переходим к следующей
        if not any(s is not None for s in cleaned_sizes):
            continue

        # print(f"Строка {index + 1}: Найдена потенциальная строка с размерами: {[s for s in cleaned_sizes if s]}")

        # 2. Ищем артикул и количество в следующей строке (index + 1) или через одну (index + 2, если есть 'Новинка')
        artikul = None
        quantity_row = None
        quantity_row_idx = -1

        # Проверяем строку index + 1
        if index + 1 < num_rows:
            row_plus_1 = df.iloc[index + 1]
            potential_artikul_1 = clean_artikul(row_plus_1.iloc[ARTIKUL_COL_IDX])
            # Проверяем также, что строка с кол-вом не пустая справа
            if potential_artikul_1 and not row_plus_1.iloc[FIRST_SIZE_COL_IDX:].isna().all():
                 # Проверяем, не обработали ли мы уже эту строку с количеством
                if index + 1 not in processed_quantity_rows:
                    artikul = potential_artikul_1
                    quantity_row = row_plus_1
                    quantity_row_idx = index + 1
                    # print(f"  Найден артикул '{artikul}' в следующей строке {quantity_row_idx + 1}.")
                # else:
                #     print(f"  Строка {index + 2} с артикулом '{potential_artikul_1}' уже обработана ранее.")

            # Если артикула нет в index + 1, проверяем на "Новинка" и смотрим index + 2
            # Ищем "Новинка" в первых колонках (до размеров)
            elif row_plus_1.iloc[:FIRST_SIZE_COL_IDX].astype(str).str.contains('Новинка', case=False, na=False).any():
                if index + 2 < num_rows:
                    # print(f"  В строке {index + 2} найдена 'Новинка'. Проверяем строку {index + 3}...")
                    row_plus_2 = df.iloc[index + 2]
                    potential_artikul_2 = clean_artikul(row_plus_2.iloc[ARTIKUL_COL_IDX])
                    # Проверяем также, что строка с кол-вом не пустая справа
                    if potential_artikul_2 and not row_plus_2.iloc[FIRST_SIZE_COL_IDX:].isna().all():
                        # Проверяем, не обработали ли мы уже эту строку с количеством
                        if index + 2 not in processed_quantity_rows:
                            artikul = potential_artikul_2
                            quantity_row = row_plus_2
                            quantity_row_idx = index + 2
                            # print(f"  Найден артикул '{artikul}' в строке {quantity_row_idx + 1} после 'Новинка'.")
                        # else:
                        #      print(f"  Строка {index + 3} с артикулом '{potential_artikul_2}' уже обработана ранее.")


        # 3. Если найден артикул и строка с количеством, которые еще не обработаны
        if artikul and quantity_row is not None and quantity_row_idx != -1:
            # print(f"  Сопоставляем размеры из строки {index + 1} с количеством из строки {quantity_row_idx + 1} для артикула '{artikul}'")
            if artikul not in data:
                data[artikul] = {}

            quantities_series = quantity_row.iloc[FIRST_SIZE_COL_IDX:]

            valid_pair_found = False
            for i, size in enumerate(cleaned_sizes):
                if size is not None: # Только для валидных размеров
                    if i < len(quantities_series):
                        quantity = clean_quantity(quantities_series.iloc[i])
                        if quantity > 0:
                             # Суммируем, если артикул/размер уже были добавлены
                            data[artikul][size] = data[artikul].get(size, 0) + quantity
                            valid_pair_found = True # Нашли хотя бы одно ненулевое количество
                            # print(f"    -> Размер '{size}', Количество: {quantity}, Итого: {data[artikul][size]}")
                    # else:
                        # print(f"    Предупреждение: Для размера '{size}' (индекс {i}) не найдено количество в строке {quantity_row_idx + 1}.")

            # Если мы успешно сопоставили данные, помечаем строку с количеством как обработанную
            if valid_pair_found:
                 processed_quantity_rows.add(quantity_row_idx)

        # else:
            # Если после строки с размерами не нашли соответствующую строку с артикулом и кол-вом
            # print(f"  Не найдена строка с артикулом/количеством после строки {index + 1} с размерами.")

    print(f"Завершено парсинг {filename}. Найдено уникальных артикулов: {len(data)}")
    return data

# --- Функция сравнения данных ---

def compare_data(data1: Dict[str, Dict[str, int]], data2: Dict[str, Dict[str, int]]) -> List[Dict[str, Any]]:
    """
    Сравнивает данные из двух словарей (старый и новый) и формирует список различий.

    Args:
        data1: Данные из первого файла (старые).
        data2: Данные из второго файла (новые).

    Returns:
        Список словарей, описывающих различия.
        Каждый словарь содержит ключи: "Артикул", "Размер", "Статус",
        "Количество (старое)", "Количество (новое)", "Разница".
    """
    results: List[Dict[str, Any]] = []
    # Получаем все уникальные артикулы из обоих наборов данных
    all_artikuls = set(data1.keys()) | set(data2.keys())

    print("Начало сравнения данных...")
    processed_count = 0
    total_artikuls = len(all_artikuls)

    for artikul in sorted(list(all_artikuls)):
        processed_count += 1
        if processed_count % 200 == 0: # Печатать прогресс каждые 200 артикулов
             print(f"Сравнение... обработано {processed_count}/{total_artikuls} артикулов")

        sizes1 = data1.get(artikul, {}) # Размеры и кол-во из старого файла
        sizes2 = data2.get(artikul, {}) # Размеры и кол-во из нового файла

        # Получаем все уникальные размеры для данного артикула из обоих файлов
        all_sizes = set(sizes1.keys()) | set(sizes2.keys())

        if not sizes1 and sizes2:
            # Случай 1: Артикул появился во втором файле (новый)
            for size, qty2 in sizes2.items():
                 if qty2 > 0: # Добавляем только если количество > 0
                    results.append({
                        "Артикул": artikul,
                        "Размер": size,
                        "Статус": "Новый артикул/размер",
                        "Количество (старое)": 0,
                        "Количество (новое)": qty2,
                        "Разница": qty2
                    })
        elif sizes1 and not sizes2:
            # Случай 2: Артикул исчез во втором файле (удален)
             for size, qty1 in sizes1.items():
                if qty1 > 0: # Добавляем только если количество было > 0
                    results.append({
                        "Артикул": artikul,
                        "Размер": size,
                        "Статус": "Удален артикул/размер",
                        "Количество (старое)": qty1,
                        "Количество (новое)": 0,
                        "Разница": -qty1
                    })
        elif sizes1 and sizes2:
            # Случай 3: Артикул есть в обоих файлах, сравниваем размеры
            for size in sorted(list(all_sizes)):
                qty1 = sizes1.get(size, 0) # Кол-во в старом файле (0 если размера не было)
                qty2 = sizes2.get(size, 0) # Кол-во в новом файле (0 если размер исчез)
                difference = qty2 - qty1

                # Добавляем в результат только если произошло реальное изменение
                # (т.е. разница не ноль ИЛИ было/стало не ноль)
                if difference != 0 or (qty1 > 0 and qty2 == 0) or (qty1 == 0 and qty2 > 0) :
                    # Исключаем случаи, когда оба количества 0 (например, размер был только в заголовках)
                    if qty1 == 0 and qty2 == 0:
                        continue

                    status = ""
                    if qty1 == 0 and qty2 > 0:
                        status = "Добавлен размер"
                    elif qty1 > 0 and qty2 == 0:
                        status = "Удален размер"
                    elif qty1 > 0 and qty2 > 0 and difference != 0:
                        status = "Изменилось количество"
                    elif difference == 0 and (qty1 > 0 or qty2 > 0):
                         # Этот случай не должен попадать сюда из-за if difference != 0...
                         # Но на всякий случай оставим - без изменений
                         continue
                    else:
                        status = "Неизвестное изменение" # Маловероятно

                    results.append({
                        "Артикул": artikul,
                        "Размер": size,
                        "Статус": status,
                        "Количество (старое)": qty1,
                        "Количество (новое)": qty2,
                        "Разница": difference
                    })
                # else: Изменений нет (qty1 == qty2), ничего не добавляем

    print("Сравнение завершено.")
    return results

# --- Главная функция запуска процесса ---

def run_main_process():
    """
    Главная функция скрипта: читает файлы, сравнивает, сохраняет результат.
    """
    # ----- НАСТРОЙКИ -----
    # !!! УКАЖИТЕ ПРАВИЛЬНЫЕ ПУТИ К ВАШИМ ФАЙЛАМ !!!
    file1_path = 'file_1.xls' # Путь к первому файлу (старый)
    file2_path = 'file_2.xls' # Путь ко второму файлу (новый)
    output_file_path = 'comparison_result.xlsx' # Имя выходного файла
    # --------------------

    # Убедимся, что необходимые библиотеки установлены
    try:
        import pandas
        import xlrd
        import openpyxl
    except ImportError as e:
        print(f"Ошибка: Необходимая библиотека не найдена ({e.name}).")
        print("Пожалуйста, установите ее, выполнив команду в терминале:")
        print(f"pip install {e.name}")
        # Или установите все сразу: pip install pandas xlrd openpyxl
        return # Выход из скрипта

    # Парсинг файлов
    print("-" * 30)
    data1 = parse_supplier_excel(file1_path)
    print("-" * 30)
    data2 = parse_supplier_excel(file2_path)
    print("-" * 30)

    # Проверка, были ли данные успешно загружены
    if not data1 and not data2:
        print("Не удалось прочитать данные ни из одного файла. Проверьте пути и формат файлов.")
        return
    elif not data1:
        print(f"Предупреждение: Не удалось прочитать данные из файла '{file1_path}'. Сравнение будет некорректным.")
    elif not data2:
         print(f"Предупреждение: Не удалось прочитать данные из файла '{file2_path}'. Сравнение будет некорректным.")

    # Сравнение данных
    differences = compare_data(data1, data2)

    # Сохранение результата
    if differences:
        print(f"Найдено различий: {len(differences)}")
        # Создаем DataFrame из списка словарей
        df_results = pd.DataFrame(differences)

        # Упорядочиваем колонки для лучшей читаемости
        column_order = [
            "Артикул",
            "Размер",
            "Статус",
            "Количество (старое)",
            "Количество (новое)",
            "Разница"
        ]
        # Убедимся, что все колонки существуют перед переупорядочиванием
        df_results = df_results[[col for col in column_order if col in df_results.columns]]


        # --- Сортировка результатов ---
        # Преобразуем размеры во временный ключ для корректной сортировки
        def sortable_size_key(size_str):
            size_str = str(size_str) # Преобразуем в строку на всякий случай
            # Паттерн: число + буквы (75B, 80AA)
            match_num_letter = re.fullmatch(r'(\d+)([A-Z]+)', size_str)
            if match_num_letter:
                num = int(match_num_letter.group(1))
                letters = match_num_letter.group(2)
                # Определяем приоритет букв (AA < A < B < C...)
                letter_priority = sum(ord(c) - ord('A') + (1 if len(letters) > 1 and c == 'A' else 0) for c in letters) # Простой способ сортировки
                return (0, num, letter_priority, letters) # 0 - тип размера

            # Паттерн: просто число (98, 102)
            match_num_only = re.fullmatch(r'(\d+)', size_str)
            if match_num_only:
                num = int(match_num_only.group(1))
                return (1, num, 0, '') # 1 - тип размера

            # Паттерн: число/число (70/95)
            match_num_slash_num = re.fullmatch(r'(\d+)/(\d+)', size_str)
            if match_num_slash_num:
                num1 = int(match_num_slash_num.group(1))
                num2 = int(match_num_slash_num.group(2))
                return (2, num1, num2, '') # 2 - тип размера

            # Паттерн: число/число/число (164/92/98)
            match_triple_slash = re.fullmatch(r'(\d+)/(\d+)/(\d+)', size_str)
            if match_triple_slash:
                 num1 = int(match_triple_slash.group(1))
                 num2 = int(match_triple_slash.group(2))
                 num3 = int(match_triple_slash.group(3))
                 return (3, num1, num2, num3) # 3 - тип размера

             # Паттерн: диапазон (40-48)
            match_range = re.fullmatch(r'(\d+)-(\d+)', size_str)
            if match_range:
                 num1 = int(match_range.group(1))
                 num2 = int(match_range.group(2))
                 return (4, num1, num2, '') # 4 - тип размера

            # Все остальное - в конец
            return (9, 0, 0, size_str)

        # Создаем ключ сортировки
        # Применяем ключ к колонке 'Размер', обрабатывая возможные ошибки
        df_results['sort_key'] = df_results['Размер'].apply(lambda x: sortable_size_key(x) if pd.notna(x) else (99,0,0,''))

        # Сортируем сначала по артикулу (как строку), потом по ключу размера
        # Преобразуем артикул во временную колонку для натуральной сортировки строк
        df_results['artikul_sort'] = df_results['Артикул'].astype(str)
        df_results = df_results.sort_values(by=['artikul_sort', 'sort_key'])

        # Удаляем временные колонки сортировки
        df_results = df_results.drop(columns=['sort_key', 'artikul_sort'])
        # --- Конец сортировки ---


        try:
            # Сохраняем в Excel (.xlsx), используя openpyxl
            df_results.to_excel(output_file_path, index=False, engine='openpyxl')
            print(f"\nРезультаты сравнения успешно сохранены в файл: '{output_file_path}'")
        except ImportError:
             print("Ошибка: Библиотека openpyxl не найдена. Установите ее: pip install openpyxl")
             print("Попытка сохранить в CSV...")
             try:
                 csv_output_path = output_file_path.replace('.xlsx', '.csv')
                 df_results.to_csv(csv_output_path, index=False, sep=';', encoding='utf-8-sig')
                 print(f"Результаты сохранены в CSV: '{csv_output_path}'")
             except Exception as e_csv:
                 print(f"Не удалось сохранить результат и в CSV: {e_csv}")
        except Exception as e:
            print(f"Ошибка при сохранении файла '{output_file_path}': {e}")
            # Попробуем сохранить в CSV как запасной вариант
            try:
                csv_output_path = output_file_path.replace('.xlsx', '.csv')
                # sep=';' для Excel, encoding для кириллицы
                df_results.to_csv(csv_output_path, index=False, sep=';', encoding='utf-8-sig')
                print(f"Результаты сохранены в CSV: '{csv_output_path}'")
            except Exception as e_csv:
                 print(f"Не удалось сохранить результат и в CSV: {e_csv}")
    else:
        print("Различий между файлами не найдено.")

# Точка входа в скрипт
if __name__ == "__main__":
    run_main_process()