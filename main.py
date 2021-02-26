import pandas as pd
import numpy as np
from databasedriver import DatabaseDriver


def read_dataframe(source_file):
    """
    Read csv file and load it into dataframe
    """
    df = pd.read_csv(source_file, usecols=[0, 5, 6, 7, 8, 9])
    return df


def clean_dataframe(df):
    """
    Drop nan, inf and duplicates values from dataframe.
    Any Reading of less than 10 and more than 1000 is considered
    anomaly and it is also dropped
    """
    glucose_cols = ['glucose_mg/dl_t1', 'glucose_mg/dl_t2', 'glucose_mg/dl_t3']
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=glucose_cols, how="any")
    df = df.drop_duplicates(subset="patient_id")

    for col in glucose_cols:
        df = df.drop(df[(df[col] > 1000) | (df[col] < 10)].index)
    return df


def prepare_dataframe(df):
    """
    Prepare the dataframe for insertion into the database
    """
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
    csv_file = "2020-10-28_patient_data.csv"

    # Connect to the database and create ddl
    db = DatabaseDriver()
    db.setup()

    # Read dataframe
    df = read_dataframe(csv_file)
    # Clean Dataframe
    clean_df = clean_dataframe(df)
    # Prepare dataframe
    final_df = prepare_dataframe(clean_df)

    # Load prepared dataframe into database
    db.load_dataframe(final_df)


if __name__ == '__main__':
    main()