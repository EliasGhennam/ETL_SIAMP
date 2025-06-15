# etl/io_utils.py
import pandas as pd

def read_excel_file(path, usecols=None):
    df = pd.read_excel(path, engine="openpyxl", usecols=usecols)
    df.columns = df.columns.str.strip().str.upper()
    return df

def save_csv(df, output_path):
    df.to_csv(output_path, index=False, encoding="utf-8")
