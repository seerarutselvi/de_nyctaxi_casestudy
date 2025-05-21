from minio import Minio
import pandas as pd
import os 
from io import BytesIO
import pg8000
from dotenv import load_dotenv

load_dotenv()

class DataPipeline:
    def __init__(self):
        # MinIO Configuration
        self.minio_client = Minio(
            endpoint=os.environ["endpoint"],
            access_key=os.environ["access_key"],
            secret_key=os.environ["secret_key"],
            secure=False
        )
        self.bucket_name = "taxi-bucket"
        self.object_name_green = "taxi-data/green_tripdata_2024-06.parquet"
        self.object_name_yellow = "taxi-data/yellow_tripdata_2024-06.parquet"

        # PostgreSQL Configuration
        self.db_config = {
            "host": os.environ["host"],
            "port": os.environ["port"],
            "user": os.environ["user"],
            "password": os.environ["password"],
            "database": os.environ["database"]
        }
        self.table_name = "lpep_trip_data"

    def main(self):
        print("Starting Data Pipeline...")

        # Step 1: Read CSV file from MinIO
        print("reading CSV from MinIO...")
        response_green = self.minio_client.get_object(self.bucket_name, self.object_name_green)
        response_yellow = self.minio_client.get_object(self.bucket_name, self.object_name_yellow)
        csv_data_green = BytesIO(response_green.read())
        csv_data_yellow = BytesIO(response_yellow.read())
        df_green = pd.read_parquet(csv_data_green)
        df_green = df_green.rename(columns={'lpep_dropoff_datetime':'tpep_dropoff_datetime',
        'lpep_pickup_datetime':'tpep_pickup_datetime'})
        # print(df_green.columns)
        df_green["trip_color"] = "green"
        df_green = df_green.drop("trip_type", axis=1)
        df_green.rename(columns = {'ehail_fee': 'fee'}, inplace=True)
        # limited rows because of memory issue
        df_yellow = pd.read_parquet(csv_data_yellow)
        df_yellow["trip_color"] = "yellow"
        df_yellow.rename(columns = {'Airport_fee': 'fee'}, inplace=True)
        final_df = pd.concat([df_yellow, df_green], ignore_index=True)
        print(final_df.columns)
        print(f"Loaded {len(df_green)} rows and {df_green.shape} from green table MinIO.")
        print(f"Loaded {len(df_yellow)} rows and {df_yellow.shape} from yellow table MinIO.")
        print(f"Loaded {len(final_df)} rows and {final_df.shape} from final df.")

        # Step 2: Connect to PostgreSQL using pg8000
        print("Connecting to PostgreSQL with pg8000...")
        conn = pg8000.connect(**self.db_config)
        cursor = conn.cursor()

        # Step 3: Create table
        print(f"Creating table '{self.table_name}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {self.table_name};")
        column_defs = ', '.join([f'"{col}" TEXT' for col in final_df.columns])
        create_table_sql = f'CREATE TABLE {self.table_name} ({column_defs});'
        cursor.execute(create_table_sql)

        # Step 4: Insert data
        print(f"Inserting data into '{self.table_name}'...")
        columns = ', '.join([f'"{col}"' for col in final_df.columns])
        placeholders = ', '.join(['%s'] * len(final_df.columns))

        insert_sql = f'INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})'
        for _, row in final_df.iterrows():
            values = tuple(row.astype(str))
            cursor.execute(insert_sql, values)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Data successfully inserted into table '{self.table_name}'.")
