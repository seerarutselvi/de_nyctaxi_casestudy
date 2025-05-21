import pandas as pd

class TLCTransformer:
    def __init__(self, raw_path: str):
        self.raw_path = raw_path

    def transform(self):
        df = pd.read_csv(self.raw_path)

        # Minimal column normalization
        df = df.rename(columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id"
        })

        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
        df["payment_type"] = df["payment_type"].astype(str)

        # Create dim_payment_type
        payment_map = {
            "1": "Credit Card", "2": "Cash", "3": "No Charge",
            "4": "Dispute", "5": "Unknown", "6": "Voided Trip"
        }
        df["payment_desc"] = df["payment_type"].map(payment_map)
        df["payment_type_id"] = df["payment_type"].astype(int)

        return df
