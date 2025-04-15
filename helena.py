import pandas as pd
import os
import sys
import traceback # Для более детальных сообщений об ошибках

# --- Константы ---
FILE1_NAME = "helena_1.xlsx"
FILE2_NAME = "helena_2.xlsx"
OUTPUT_NAME = "helena_differences.xlsx"
KEY_COLUMNS_FOR_COMPARISON = ['Модель', 'Цвет', 'Размер']
MAX_HEADER_CHECK_ROWS = 20 # Макс. кол-во строк для поиска заголовка

def find_excel_header_row(filepath, expected_headers):
    """
    Tries to find the header row in an Excel file by reading cell values.
    Returns the 0-based index of the header row if found, otherwise None.
    """
    print(f"Searching for headers {expected_headers} in Excel file: {os.path.basename(filepath)}")
    try:
        df_check = pd.read_excel(filepath, header=None, nrows=MAX_HEADER_CHECK_ROWS, engine='openpyxl')
        expected_headers_lower = [str(h).lower().strip() for h in expected_headers]

        for i, row in df_check.iterrows():
            try:
                if row.isnull().all():
                    continue
                row_values_lower = [str(v).lower().strip() for v in row.dropna().tolist()]
            except Exception:
                continue

            headers_found_count = 0
            for expected_h in expected_headers_lower:
                if expected_h in row_values_lower:
                    headers_found_count += 1

            if headers_found_count >= len(expected_headers_lower):
                print(f"Found headers in Excel row {i} (0-based index) for {os.path.basename(filepath)}")
                return i

    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except ImportError:
         print("Error: 'openpyxl' library not found. Please install it: pip install openpyxl")
         return None
    except Exception as e:
        print(f"Error reading Excel file {filepath} during header search: {e}")
        traceback.print_exc()
        return None

    print(f"Error: Could not find the header row containing {expected_headers} in the first {MAX_HEADER_CHECK_ROWS} rows of {os.path.basename(filepath)}.")
    return None

def load_and_clean_data(filepath, header_row_index, key_columns):
    """Loads data from Excel, cleans it, and creates a unique identifier."""
    print(f"Attempting to load: {os.path.basename(filepath)} starting from header row {header_row_index}")
    df = None

    try:
        # Читаем все как строки изначально, чтобы избежать проблем с типами Excel
        df = pd.read_excel(filepath, header=header_row_index, engine='openpyxl', dtype=str)
        print(f"Successfully loaded Excel (as string): {os.path.basename(filepath)}")
    except ImportError:
         print("Error: 'openpyxl' library not found. Please install it: pip install openpyxl")
         return None
    except Exception as e:
        print(f"An unexpected error occurred while loading {os.path.basename(filepath)}: {e}")
        traceback.print_exc()
        return None

    # --- Data Cleaning ---
    print(f"Cleaning data for {os.path.basename(filepath)}...")

    # 1. Проверяем и переименовываем столбцы
    actual_columns_base = {str(col).split('.')[0].strip().lower(): col for col in df.columns}
    expected_lower_map = {col.lower(): col for col in key_columns + ['Наименование']}
    missing_cols = []
    column_mapping = {}

    for expected_lower, expected_orig in expected_lower_map.items():
        if expected_lower in actual_columns_base:
            column_mapping[actual_columns_base[expected_lower]] = expected_orig
        elif expected_orig in key_columns:
             missing_cols.append(expected_orig)

    if missing_cols:
        print(f"Error: Missing required columns (case-insensitive) in {os.path.basename(filepath)}: {', '.join(missing_cols)}")
        print(f"Available columns: {', '.join(map(str, df.columns))}")
        return None

    df.rename(columns=column_mapping, inplace=True)
    print(f"Columns after rename: {list(df.columns)}")

    # 2. Выбираем релевантные столбцы
    columns_to_keep = [col for col in key_columns + ['Наименование'] if col in df.columns]
    if not columns_to_keep:
        print(f"Error: No relevant columns found after renaming in {os.path.basename(filepath)}")
        return None
    df_cleaned = df[columns_to_keep].copy()

    # 3. Заполняем 'Наименование'
    if 'Наименование' in df_cleaned.columns:
        # Уже прочитано как строка, просто чистим и заполняем
        df_cleaned['Наименование'] = df_cleaned['Наименование'].str.strip().str.strip('"')
        df_cleaned['Наименование'] = df_cleaned['Наименование'].replace(['nan', 'none', '', '<NA>'], pd.NA)
        df_cleaned['Наименование'] = df_cleaned['Наименование'].ffill()
        df_cleaned['Наименование'] = df_cleaned['Наименование'].fillna('N/A')

    # 4. Удаляем строки, где ВСЕ ключевые столбцы пусты или NA
    # Сначала заменим пустые строки и 'nan' на pd.NA во всех ключевых колонках
    for col in key_columns:
         if col in df_cleaned.columns:
              df_cleaned[col] = df_cleaned[col].str.strip().replace(['', 'nan', 'none'], pd.NA)
    df_cleaned.dropna(subset=key_columns, how='all', inplace=True)

    if df_cleaned.empty:
        print(f"Warning: DataFrame became empty after dropping rows with all key columns NA for {os.path.basename(filepath)}.")
        return pd.DataFrame(columns=columns_to_keep + ['Unique_ID'])

    # 5. Обрабатываем и фильтруем 'Модель'
    if 'Модель' in key_columns:
        print(f"Processing 'Модель' column for {os.path.basename(filepath)}...")
        # 'Модель' уже строка из-за dtype=str при чтении
        df_cleaned['Модель'] = df_cleaned['Модель'].str.strip()
        df_cleaned.dropna(subset=['Модель'], inplace=True) # Удаляем строки, где Модель стала NA после strip

        if df_cleaned.empty:
             print(f"Warning: DataFrame empty after dropping rows with NA 'Модель' for {os.path.basename(filepath)}.")
             return pd.DataFrame(columns=columns_to_keep + ['Unique_ID'])

        # --- ИЗМЕНЕННЫЙ ФИЛЬТР МОДЕЛИ ---
        # Оставляем строки, где 'Модель' состоит из цифр, возможно с .0 на конце
        # Исключаем строки, которые точно НЕ модели (например, 'ххх', если такие есть)
        # Можно добавить сюда известные не-модели: | (df_cleaned['Модель'].str.lower() == 'ххх')
        is_valid_model = df_cleaned['Модель'].str.match(r'^\d+(\.0)?$', na=False)

        print(f"  Rows before model filtering: {len(df_cleaned)}")
        rows_to_remove = len(df_cleaned[~is_valid_model])
        if rows_to_remove > 0:
             print(f"  Filtering out {rows_to_remove} rows with non-numeric-like 'Модель'. Examples:")
             print(df_cleaned.loc[~is_valid_model, 'Модель'].head().to_string(index=False)) # Убрал индекс для чистоты

        df_cleaned = df_cleaned[is_valid_model].copy() # Используем .copy() для избежания ошибки ниже
        # --- КОНЕЦ ИЗМЕНЕННОГО ФИЛЬТРА ---

        if df_cleaned.empty:
             print(f"Warning: DataFrame became empty after filtering 'Модель' for {os.path.basename(filepath)}.")
             return pd.DataFrame(columns=columns_to_keep + ['Unique_ID'])
        else:
            # Убираем .0 если он есть, уже после фильтрации
            df_cleaned['Модель'] = df_cleaned['Модель'].str.replace(r'\.0$', '', regex=True)
            print(f"  Rows after model filtering: {len(df_cleaned)}")


    # 6. Стандартизируем КЛЮЧЕВЫЕ столбцы (Модель уже обработана)
    for col in key_columns:
        if col in df_cleaned.columns:
            # Преобразуем в строку (на всякий случай, если тип изменился) и чистим
            df_cleaned[col] = df_cleaned[col].fillna('N/A').astype(str).str.strip().str.upper()
            # Убираем .0 (может появиться в других числовых колонках, прочитанных как строки)
            df_cleaned[col] = df_cleaned[col].str.replace(r'\.0$', '', regex=True)
            if col == 'Размер':
                df_cleaned[col] = df_cleaned[col].str.replace('В', 'B', regex=False)
                df_cleaned[col] = df_cleaned[col].str.replace('С', 'C', regex=False)
            # Финальная замена пустых/NA на 'N/A'
            df_cleaned[col] = df_cleaned[col].replace(['NAN', 'NONE', '', 'NA', '<NA>'], 'N/A')
        else:
             print(f"Warning: Column '{col}' not found during standardization in {os.path.basename(filepath)}.")

    # 7. Создаем уникальный ID
    existing_key_columns = [col for col in key_columns if col in df_cleaned.columns]
    if len(existing_key_columns) != len(key_columns):
         print(f"Error: Not all key columns ({key_columns}) exist after cleaning for {os.path.basename(filepath)}. Aborting ID creation.")
         return None

    sorted_keys = sorted(existing_key_columns)
    try:
        # Все колонки должны быть строками после шага 6
        df_cleaned['Unique_ID'] = df_cleaned[sorted_keys].apply(lambda row: '_'.join(row.astype(str).values), axis=1)
    except Exception as e:
        print(f"Error creating Unique_ID for {os.path.basename(filepath)}: {e}")
        print("Data sample causing issues (first 5 rows of keys):")
        print(df_cleaned[sorted_keys].head())
        traceback.print_exc()
        return None

    # 8. Удаляем дубликаты по ID
    if 'Unique_ID' not in df_cleaned.columns:
        print(f"Error: Unique_ID column creation failed for {os.path.basename(filepath)}. Cannot drop duplicates.")
        return None
    df_cleaned.drop_duplicates(subset=['Unique_ID'], keep='first', inplace=True)

    print(f"Finished cleaning {os.path.basename(filepath)}. Found {len(df_cleaned)} unique product variants.")
    return df_cleaned


# --- Остальные функции (compare_files, __main__) остаются без изменений ---
def compare_files(file1_path, file2_path, output_path, key_columns):
    """Compares two files based on key columns and writes differences to Excel."""

    print("\nStarting comparison process...")
    print(f"File 1: {os.path.basename(file1_path)}")
    print(f"File 2: {os.path.basename(file2_path)}")
    print(f"Output: {os.path.basename(output_path)}")
    print(f"Key Columns: {', '.join(key_columns)}")
    print("-" * 30)

    # --- Find Header Row ---
    header_search_cols = key_columns[:min(3, len(key_columns))]
    header_row1 = find_excel_header_row(file1_path, header_search_cols)
    header_row2 = find_excel_header_row(file2_path, header_search_cols)

    if header_row1 is None or header_row2 is None:
        print("Could not find headers in one or both files. Exiting.")
        return

    # --- Load and Clean Data ---
    df1 = load_and_clean_data(file1_path, header_row1, key_columns)
    df2 = load_and_clean_data(file2_path, header_row2, key_columns)

    # Проверяем, что оба DataFrame существуют после очистки
    if df1 is None or df2 is None:
        print("Failed to load or clean one or both files. Comparison aborted.")
        return
    # Добавим проверку на наличие колонки Unique_ID
    if 'Unique_ID' not in df1.columns or 'Unique_ID' not in df2.columns:
         print("Error: 'Unique_ID' column missing after cleaning one or both files. Comparison aborted.")
         print(f"Columns in df1: {list(df1.columns) if df1 is not None else 'None'}")
         print(f"Columns in df2: {list(df2.columns) if df2 is not None else 'None'}")
         return

    # --- Perform Comparison ---
    print("\nComparing datasets...")

    set1_ids = set(df1['Unique_ID'])
    set2_ids = set(df2['Unique_ID'])

    only_in_file1_ids = set1_ids - set2_ids
    only_in_file2_ids = set2_ids - set1_ids

    # --- Prepare Output ---
    # Добавим проверку на пустые датафреймы перед фильтрацией по ID
    diff1 = pd.DataFrame()
    diff2 = pd.DataFrame()
    if not df1.empty and only_in_file1_ids:
        diff1 = df1[df1['Unique_ID'].isin(only_in_file1_ids)].copy()
    if not df2.empty and only_in_file2_ids:
        diff2 = df2[df2['Unique_ID'].isin(only_in_file2_ids)].copy()


    file1_basename = os.path.basename(file1_path)
    file2_basename = os.path.basename(file2_path)

    if not diff1.empty:
        diff1['Источник'] = f"Только в {file1_basename}"
    if not diff2.empty:
        diff2['Источник'] = f"Только в {file2_basename}"

    all_diffs = pd.concat([diff1, diff2], ignore_index=True)

    output_columns_order = ['Источник']
    # Проверяем наличие колонок в all_diffs перед добавлением
    if 'Наименование' in all_diffs.columns:
         output_columns_order.append('Наименование')
    # Добавляем только те ключевые колонки, что реально есть в all_diffs
    output_columns_order.extend([col for col in key_columns if col in all_diffs.columns])

    # Финальный список колонок (только существующие)
    final_output_columns = [col for col in output_columns_order if col in all_diffs.columns]

    if all_diffs.empty:
        print("\n--- No differences found based on the specified key columns. ---")
        # Создаем пустой файл с заголовками, если различий нет
        try:
            # Создаем пустой DataFrame с ожидаемыми колонками для записи заголовков
            empty_df = pd.DataFrame(columns=final_output_columns if final_output_columns else ['Сообщение'])
            if not final_output_columns: # Если даже колонки 'Источник' нет
                 empty_df['Сообщение'] = ["Различий не найдено, колонки не определены."]

            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                 empty_df.to_excel(writer, sheet_name='Differences', index=False)
            print(f"Empty comparison result file created (or overwritten) at: {output_path}")
        except Exception as e:
            print(f"Could not create empty output file: {e}")
            traceback.print_exc()

    else:
        print(f"\nFound {len(only_in_file1_ids)} items only in {file1_basename}.")
        print(f"Found {len(only_in_file2_ids)} items only in {file2_basename}.")

        # Убедимся, что финальный DataFrame не пустой и содержит колонки
        if not final_output_columns:
             print("Error: Could not determine output columns for the differences DataFrame.")
             return

        final_output_df = all_diffs[final_output_columns].copy()

        # --- Write to Excel ---
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                final_output_df.to_excel(writer, sheet_name='Differences', index=False)
            print(f"\nComparison complete. Differences saved to: {output_path}")
        except ImportError:
             print("\nError: 'openpyxl' library not found. Cannot write Excel file.")
             print("Please install it: pip install openpyxl")
        except PermissionError:
             print(f"\nError: Permission denied when trying to write to {output_path}.")
             print("Please make sure the file is not open in another program and you have write permissions.")
        except Exception as e:
            print(f"\nError writing output file {output_path}: {e}")
            traceback.print_exc()

# --- Main Execution ---
if __name__ == "__main__":
    print("Excel File Comparison Script")
    print(f"Looks for '{FILE1_NAME}' and '{FILE2_NAME}' in the script's directory.")
    print(f"Saves differences to '{OUTPUT_NAME}' in the same directory.")
    print("-" * 30)

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
         script_dir = os.getcwd()
         print(f"Warning: Could not determine script directory automatically. Using current working directory: {script_dir}")

    file1_path = os.path.join(script_dir, FILE1_NAME)
    file2_path = os.path.join(script_dir, FILE2_NAME)
    output_path = os.path.join(script_dir, OUTPUT_NAME)

    if not os.path.isfile(file1_path):
        print(f"Error: Input file not found: {FILE1_NAME}")
        print(f"Expected location: {file1_path}")
        sys.exit(1)
    if not os.path.isfile(file2_path):
         print(f"Error: Input file not found: {FILE2_NAME}")
         print(f"Expected location: {file2_path}")
         sys.exit(1)

    compare_files(file1_path, file2_path, output_path, KEY_COLUMNS_FOR_COMPARISON)

    print("\nScript finished.")