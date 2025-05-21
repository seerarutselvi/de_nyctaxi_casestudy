import pg8000
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class TaxiDataAnalyzer:
    def __init__(self, db_params):
        """
        Initialize the database connection.
        db_params: dict with keys - host, database, user, password, port
        """
        self.conn = pg8000.connect(**db_params)

    def fetch_data(self, table_name):
        """
        Fetch pickup and dropoff location data from the taxi trip table.
        """
        query = f"""
        SELECT CAST(CAST("PULocationID" AS FLOAT) AS INTEGER),CAST(CAST("DOLocationID" AS FLOAT) AS INTEGER)
        FROM {table_name}
        """
        
        df = pd.read_sql(query, self.conn)
        return df

    def fetch_zone_lookup(self, lookup_table):
        """
        Fetch the zone lookup data.
        """
        query = f"""
        SELECT CAST(CAST("LocationID" AS FLOAT) AS INTEGER), "Zone", "Borough"
        FROM {lookup_table}
        """
        return pd.read_sql(query, self.conn)

    def compute_zone_frequencies(self, trip_data):
        """
        Compute pickup, dropoff, and total counts.
        """
        pickup_counts = trip_data['PULocationID'].value_counts().rename_axis('ZoneID').reset_index(name='Pickup Count')
        dropoff_counts = trip_data['DOLocationID'].value_counts().rename_axis('ZoneID').reset_index(name='Dropoff Count')
        
        zone_counts = pd.merge(pickup_counts, dropoff_counts, on='ZoneID', how='outer').fillna(0)
        zone_counts['Total Count'] = zone_counts['Pickup Count'] + zone_counts['Dropoff Count']
        return zone_counts.sort_values(by='Total Count', ascending=False)

    def enrich_with_zone_info(self, zone_counts, zone_lookup):
        """
        Merge the zone frequency counts with the lookup table for readability.
        """
        return zone_counts.merge(zone_lookup, left_on='ZoneID', right_on='LocationID', how='left')

    def run_analysis(self, trip_table, lookup_table, top_n=10):
        """
        Run full analysis pipeline and return top N zones.
        """
        trip_data = self.fetch_data(trip_table)
        zone_lookup = self.fetch_zone_lookup(lookup_table)
        zone_counts = self.compute_zone_frequencies(trip_data)
        enriched_data = self.enrich_with_zone_info(zone_counts, zone_lookup)
        return enriched_data.head(top_n)

    def plot_zone_activity(self, enriched_data, kind='total', top_n=10):
        """
        Plot the top N zones by pickup, dropoff, or total counts.

        Parameters:
        - enriched_data: DataFrame from run_analysis
        - kind: 'pickup', 'dropoff', or 'total'
        - top_n: number of top zones to display
        """
        if kind == 'pickup':
            data = enriched_data.sort_values(by='Pickup Count', ascending=False).head(top_n)
            y_col = 'Pickup Count'
        elif kind == 'dropoff':
            data = enriched_data.sort_values(by='Dropoff Count', ascending=False).head(top_n)
            y_col = 'Dropoff Count'
        else:
            data = enriched_data.sort_values(by='Total Count', ascending=False).head(top_n)
            y_col = 'Total Count'

        plt.figure(figsize=(12, 6))
        sns.barplot(data=data, x='Zone', y=y_col, palette='viridis')
        plt.title(f'Top {top_n} Zones by {y_col}')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()


    def compute_payment_distribution(self, table_name):
        """
        Compute the percentage of trips paid by cash vs card.
        """
        query = f"""
        SELECT "payment_type", COUNT(*) as trip_count
        FROM {table_name}
        GROUP BY "payment_type"
        """
        df = pd.read_sql(query, self.conn)
        # Filter only card and cash
        filtered = df[df['payment_type'].isin(['1.0', '2.0'])].copy()
        total = filtered['trip_count'].sum()
        
        if total == 0:
            return {"Credit Card": 0.0, "Cash": 0.0}
        
        percentages = {
            "Credit Card": float(round(filtered.loc[filtered['payment_type'] == '1.0', 'trip_count'].values[0] / total * 100, 2))
            if "1.0" in filtered['payment_type'].values else 0.0,
            
            "Cash": float(round(filtered.loc[filtered['payment_type'] == '2.0', 'trip_count'].values[0] / total * 100, 2))
            if "2.0" in filtered['payment_type'].values else 0.0,
        }

        return percentages

    def compute_average_fare(self, table_name):
        """
        Compute the average fare amount per trip.
        """
        query = f"""
        SELECT AVG(CAST(fare_amount AS FLOAT))  as avg_fare
        FROM {table_name}
        WHERE CAST(fare_amount AS FLOAT) > 0
        ;
        """
        df = pd.read_sql(query, self.conn)
        return round(df['avg_fare'].iloc[0], 2)


    def close_connection(self):
        self.conn.close()
