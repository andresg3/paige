import pandas as pd
import numpy as np
from databasedriver import DatabaseDriver


def extract(source_file):
    return pd.read_csv(source_file, usecols=[0, 5, 6, 7, 8, 9])


def cleanup(df):
    glucose_cols = ['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=glucose_cols, how="any")
    df = df.drop_duplicates(subset="patient_id")

    for col in glucose_cols:
        df = df.drop(df[(df[col] > 1000) | (df[col] < 10)].index)
    return df


def xform(df):
    df['readings_avg'] = df[['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']].mean(axis=1)

    levels = []
    for val in df['readings_avg']:
        if val < 140:
            levels.append('Normal')
        elif 140 <= val < 200:
            levels.append('Prediabetes')
        elif val >= 200:
            levels.append('Diabetes')

    df['levels'] = levels
    return df


def main():
    source_file = "2020-10-28_patient_data.csv"

    db = DatabaseDriver()
    db.setup()

    df = extract(source_file)
    clean_df = cleanup(df)
    final_df = xform(clean_df)

    db.load_df(final_df)


if __name__ == '__main__':
    main()
