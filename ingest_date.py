import argparse
from time import time
import os

import pandas as pd
from sqlalchemy import create_engine


def convert_to_datetime(df, datetime_columns):
    if datetime_columns is not None:
        df[datetime_columns] = df[datetime_columns].apply(pd.to_datetime)
    return df


def ingest_data_to_postgres(csv_name, params):
    engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}')

    df_iter = pd.read_csv(f'ingestion_files/{csv_name}', iterator=True, chunksize=100000)

    df = next(df_iter)
    df = convert_to_datetime(df, params.datetime_columns)

    csv_name = csv_name.split(".")[0]

    df.head(n=0).to_sql(name=csv_name, con=engine, if_exists='replace')

    df.to_sql(name=csv_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df = next(df_iter)
            df = convert_to_datetime(df, params.datetime_columns)

            df.to_sql(name=csv_name, con=engine, if_exists='append')

            t_end = time()

            print(f"Inserted chunk in {t_end - t_start:.3f} seconds")

        except StopIteration:
            print("Finished ingesting data")
            break
        except pd.errors.EmptyDataError:
            print("No more data available")
            break


def main(params):

    for csv in os.scandir('ingestion_files'):
        ingest_data_to_postgres(csv.name, params)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--datetime_columns', required=False, help='columns that need to be converted to datetime')

    args = parser.parse_args()

    datetime_columns = args.datetime_columns.split(',') if args.datetime_columns else None

    args.datetime_columns = datetime_columns

    main(args)