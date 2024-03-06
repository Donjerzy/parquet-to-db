import pyarrow.parquet as pp
import pandas as pd
from sqlalchemy import create_engine


def main(args: dict):

    user = args['user']
    password = args['password']
    host = args['host']
    port = args['port']
    database = args['database']
    file_name = args['file_name']
    directory = './parquet/'
    table_exists = args['table_exists']
    table_name = args['table_name']
    schema = args['schema']
    db_connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(db_connection_str)
    df = None

    if file_name.split('.')[-1] != 'parquet':
        print(f'Invalid File Format: {file_name.split(".")[-1]}')
        quit()

    file_path = directory + file_name

    if not table_exists:
        # create table
        df = pd.read_parquet(file_path)
        df.head(n=0).to_sql(con=engine,name=table_name, schema=schema, if_exists='fail', index=False)
        df = None

    parquet_file = pp.ParquetFile(file_path)

    completed = 1
    unsuccessful = []

    for batch in parquet_file.iter_batches(batch_size=100000):
        batch_df = batch.to_pandas()
        try:
            batch_df.to_sql(con=engine,name=table_name, schema=schema, if_exists='append', index=False)
        except:
            print(f'Could not insert batch {completed} to table')
            unsuccessful.append(completed)
            continue
        print(f'Batch {completed} done')
        completed += 1

    print('Insertion Complete')
    print(f'Successful batches count = {completed}. The unsuccessful batches were {unsuccessful}')


if __name__ == '__main__':
    main(
        {
            'user': '',
            'password': '',
            'host': '',
            'port': 0,
            'database': '',
            'file_name': '',
            'table_exists': False,
            'table_name': '',
            'schema': ''
        }
    )
