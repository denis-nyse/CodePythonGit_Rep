import pandas as pd
import numpy as np
import os
import re

def clean_col_names(df):
    """Cleans column names by removing newlines, extra spaces, and standardizing."""
    new_columns = {}
    for col in df.columns:
        # Replace newline with space, strip leading/trailing spaces
        cleaned_col = col.replace('\n', ' ').strip()
        # Standardize common names (adjust based on exact names)
        # Using regex for more robust matching, ignoring case
        if re.match(r'Цена в руб.*', cleaned_col, re.IGNORECASE):
            cleaned_col = 'Цена'
        elif re.match(r'Наименование.*', cleaned_col, re.IGNORECASE):
            cleaned_col = 'Наименование'
        elif re.match(r'Количество.*складе', cleaned_col, re.IGNORECASE):
            cleaned_col = 'Количество'
        # Add more standardization if needed
        elif re.match(r'^№', cleaned_col, re.IGNORECASE):
             cleaned_col = '№'
        elif cleaned_col.lower() == 'модель':
             cleaned_col = 'Модель'
        elif cleaned_col.lower() == 'состав':
             cleaned_col = 'Состав'
        elif cleaned_col.lower() == 'цвет':
             cleaned_col = 'Цвет'
        elif cleaned_col.lower() == 'размер':
             cleaned_col = 'Размер'
        elif cleaned_col.lower() == 'штрихкод':
             cleaned_col = 'Штрихкод'

        # Remove potential extra spaces between words
        cleaned_col = re.sub(r'\s+', ' ', cleaned_col).strip()
        new_columns[col] = cleaned_col
    df.rename(columns=new_columns, inplace=True)
    # Filter out potential fully empty columns that might result from bad parsing
    df = df.loc[:, df.columns.notna()]
    # print(f"Cleaned columns: {df.columns.tolist()}") # Debugging line
    return df

def process_file(filepath):
    """Reads and preprocesses the Excel or CSV file."""
    try:
        # Try reading as Excel first (.xlsx or .xls)
        try:
            df = pd.read_excel(filepath, dtype={'Штрихкод': str, 'Модель': str})
            print(f"Successfully read {filepath} as Excel.")
        except Exception as e:
            # If Excel read fails, try reading as CSV (often saved from Excel)
            print(f"Reading as Excel failed for {filepath}: {e}. Trying as CSV...")
            # Detect encoding if possible, otherwise try common ones
            encodings_to_try = ['utf-8', 'cp1251', 'latin1']
            read_success = False
            last_error = None
            for enc in encodings_to_try:
                try:
                    # Determine separator by sniffing first few lines if possible, default to ','
                    # This is a basic guess, might need refinement for complex CSVs
                    with open(filepath, 'r', encoding=enc) as f_test:
                        line1 = f_test.readline()
                        line2 = f_test.readline()
                    sep = ',' if ',' in line1 else ';' # Simple guess
                    if sep not in line1 and '\t' in line1: sep = '\t'

                    df = pd.read_csv(filepath, sep=sep, dtype={'Штрихкод': str, 'Модель': str}, encoding=enc, low_memory=False)
                    print(f"Successfully read {filepath} as CSV with encoding {enc} and separator '{sep}'")
                    read_success = True
                    break # Stop trying encodings if successful
                except UnicodeDecodeError:
                    print(f"CSV read with encoding {enc} failed for {filepath} (UnicodeDecodeError). Trying next...")
                    last_error = f"UnicodeDecodeError with encoding {enc}"
                except Exception as csv_e:
                     # Catch other potential CSV errors
                     print(f"Generic CSV read error with encoding {enc} for {filepath}: {csv_e}")
                     last_error = csv_e
            if not read_success:
                 raise ValueError(f"Could not read {filepath} as CSV with any tried encoding. Last error: {last_error}")

        df = clean_col_names(df)

        # --- Data Propagation ---
        # Identify columns that define the product group vs. the variant
        # Crucial: Ensure these column names EXACTLY match the output of clean_col_names
        group_cols = ['Модель', 'Наименование', 'Состав'] # These might be filled down
        variant_group_cols = ['Цвет', 'Цена'] # Color/Price define subgroups within Model
        variant_detail_cols = ['Размер', 'Штрихкод', 'Количество'] # These vary per row

        # Check if necessary columns exist after cleaning
        required_cols = ['Модель', 'Цвет', 'Размер', 'Штрихкод', 'Количество']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
             # Print available columns for debugging
             print(f"Columns found after cleaning in {filepath}: {df.columns.tolist()}")
             raise ValueError(f"File {filepath} is missing required columns after cleaning: {missing_cols}.")

        # Forward fill the main product information
        for col in group_cols:
            if col in df.columns:
                df[col] = df[col].ffill()

        # Forward fill Color and Price *within each Model group*
        # This ensures color applies to all its sizes but doesn't leak to the next model
        if 'Цвет' in df.columns:
             df['Цвет'] = df.groupby('Модель', group_keys=False, sort=False)['Цвет'].ffill()
        if 'Цена' in df.columns:
             df['Цена'] = df.groupby('Модель', group_keys=False, sort=False)['Цена'].ffill()
             # Clean Price: remove currency, spaces, convert comma to dot
             df['Цена'] = df['Цена'].astype(str).str.replace('руб\.', '', regex=True).str.replace('РБ', '', regex=False).str.replace('с НДС', '', regex=False).str.replace(',', '.', regex=False).str.strip()
             df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce')


        # Ensure 'Штрихкод' is string and clean, drop rows without barcode or model
        df['Штрихкод'] = df['Штрихкод'].astype(str).str.strip().str.replace('.0$', '', regex=True) # Remove trailing .0 if read as float
        df.dropna(subset=['Штрихкод', 'Модель'], inplace=True)
        df = df[df['Штрихкод'].str.lower().ne('nan') & df['Штрихкод'].ne('')]
        df = df[df['Модель'].notna() & df['Модель'].ne('') & df['Модель'].str.lower().ne('nan')]

        # Convert 'Количество' to numeric, coercing errors to NaN
        # Handle potential comma decimal separator
        if 'Количество' in df.columns:
            if df['Количество'].dtype == 'object':
                 df['Количество'] = df['Количество'].astype(str).str.replace(',', '.', regex=False)
            df['Количество'] = pd.to_numeric(df['Количество'], errors='coerce')
        else:
             raise ValueError(f"Column 'Количество' not found in {filepath} after cleaning.")


        # Select necessary columns and handle potential missing data for key fields
        cols_to_keep = ['Штрихкод', 'Модель', 'Цвет', 'Размер', 'Количество']
        # Add 'Цена' if needed for comparison later, otherwise skip
        # if 'Цена' in df.columns: cols_to_keep.append('Цена')

        df = df[cols_to_keep].copy()

        # Fill NaN in descriptive columns AFTER propagation
        df['Цвет'].fillna('N/A', inplace=True)
        df['Размер'].fillna('N/A', inplace=True)
        # Model NaN should be handled by dropna earlier, but just in case
        df['Модель'].fillna('N/A', inplace=True)

        # Fill NaN Quantities with 0 *before* dropping duplicates
        df['Количество'] = df['Количество'].fillna(0).astype(int)


        # Drop duplicates based on barcode, keeping the first occurrence
        # (important if barcode accidentally appears twice in a source file)
        df.drop_duplicates(subset=['Штрихкод'], keep='first', inplace=True)

        print(f"Finished processing {filepath}. Found {len(df)} unique valid rows.")
        # print(df.head()) # Debugging line
        return df

    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None
    except ValueError as ve:
        print(f"ValueError processing file {filepath}: {ve}")
        # Attempt to print columns if df was partially created
        if 'df' in locals() and isinstance(df, pd.DataFrame):
             print(f"Columns at time of error: {df.columns.tolist()}")
        return None
    except Exception as e:
        import traceback
        print(f"General Error processing file {filepath}: {e}")
        print(traceback.format_exc()) # Print full traceback for unexpected errors
        # Attempt to print columns if df was partially created
        if 'df' in locals() and isinstance(df, pd.DataFrame):
             print(f"Columns at time of error: {df.columns.tolist()}")
        return None


def compare_stock_files(file1_path, file2_path, output_path):
    """Compares stock data from two files based on Barcode."""

    print(f"Processing {file1_path}...")
    df1 = process_file(file1_path)
    if df1 is None:
        print(f"Failed to process {file1_path}. Aborting comparison.")
        return

    print(f"Processing {file2_path}...")
    df2 = process_file(file2_path)
    if df2 is None:
        print(f"Failed to process {file2_path}. Aborting comparison.")
        return

    print("Files processed successfully. Merging data...")
    # Perform an outer merge to keep all barcodes from both files
    merged_df = pd.merge(
        df1,
        df2,
        on='Штрихкод',
        how='outer',
        suffixes=('_file1', '_file2')
    )
    print(f"Merge complete. Found {len(merged_df)} unique barcodes across files.")

    # Fill NaN quantities with 0 AFTER merge for items missing in one file
    merged_df['Количество_file1'] = merged_df['Количество_file1'].fillna(0).astype(int)
    merged_df['Количество_file2'] = merged_df['Количество_file2'].fillna(0).astype(int)

    # Consolidate descriptive columns (Model, Color, Size)
    # Use data from file2 (newer) if available, otherwise fallback to file1
    merged_df['Модель'] = merged_df['Модель_file2'].combine_first(merged_df['Модель_file1'])
    merged_df['Цвет'] = merged_df['Цвет_file2'].combine_first(merged_df['Цвет_file1'])
    merged_df['Размер'] = merged_df['Размер_file2'].combine_first(merged_df['Размер_file1'])

    # Fill any remaining NaNs in consolidated columns (should be rare)
    merged_df['Модель'].fillna('N/A', inplace=True)
    merged_df['Цвет'].fillna('N/A', inplace=True)
    merged_df['Размер'].fillna('N/A', inplace=True)

    # --- Identify differences ---
    print("Identifying differences...")

    # Condition 1: Quantity Changed (and item exists in both files)
    # Use the consolidated columns to check for existence
    cond_changed = (merged_df['Количество_file1'] != merged_df['Количество_file2']) & \
                   (merged_df['Модель_file1'].notna()) & \
                   (merged_df['Модель_file2'].notna())

    # Condition 2: Removed (In file1, not in file2 based on Штрихкод)
    cond_removed = (merged_df['Модель_file1'].notna()) & \
                   (merged_df['Модель_file2'].isna())

    # Condition 3: Added (Not in file1, in file2 based on Штрихкод)
    cond_added = (merged_df['Модель_file1'].isna()) & \
                 (merged_df['Модель_file2'].notna())

    # Assign status based on conditions
    merged_df['Статус'] = np.select(
        [cond_changed, cond_removed, cond_added],
        ['Изменилось Кол-во', 'Удалено из file2', 'Добавлено в file2'],
        default='Без изменений' # Apply if quantities are equal and item exists in both
    )

    # Filter out rows with no changes
    diff_df = merged_df[merged_df['Статус'] != 'Без изменений'].copy()
    print(f"Found {len(diff_df)} differences.")

    # --- Prepare final output ---
    diff_df_final = diff_df[[
        'Штрихкод',
        'Модель',
        'Цвет',      # Now includes the consolidated color
        'Размер',
        'Количество_file1',
        'Количество_file2',
        'Статус'
    ]].rename(columns={
        'Количество_file1': 'Кол-во File1',
        'Количество_file2': 'Кол-во File2'
    }).sort_values(by=['Модель', 'Цвет', 'Размер', 'Штрихкод']) # Sort for readability

    # --- Save to Excel ---
    try:
        print(f"Saving differences to {output_path}...")
        diff_df_final.to_excel(output_path, index=False, engine='openpyxl')
        print(f"Differences successfully saved to {output_path}")
    except Exception as e:
        print(f"Error saving differences to Excel file {output_path}: {e}")

# --- Configuration ---
# Use raw strings (r'...') for paths on Windows to avoid backslash issues
# Or use forward slashes '/' which work on most systems
file1 = 'file1.xlsx' # Adjust extension if needed (.csv, .xls)
file2 = 'file2.xlsx' # Adjust extension if needed (.csv, .xls)
output_file = 'stock_differences_with_color.xlsx'

# --- Run Comparison ---
if not os.path.exists(file1):
    print(f"Error: Cannot find input file: {file1}")
elif not os.path.exists(file2):
    print(f"Error: Cannot find input file: {file2}")
else:
    compare_stock_files(file1, file2, output_file)