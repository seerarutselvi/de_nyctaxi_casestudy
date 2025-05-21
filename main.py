import warnings
import os
from dotenv import load_dotenv

from taxi_analysis import TaxiDataAnalyzer
warnings.filterwarnings("ignore")

load_dotenv()
db_params = {
            "host": os.environ["host"],
            "port": os.environ["port"],
            "user": os.environ["user"],
            "password": os.environ["password"],
            "database": os.environ["database"]
        }

#Zones with the highest pickup and dropoff frequency

analyzer = TaxiDataAnalyzer(db_params)
top_zones = analyzer.run_analysis('lpep_trip_data', 'location_lookup', top_n=10)
print("top_zones")
print(top_zones)

# Plotting the data highest pickup and dropoff frequency
analyzer.plot_zone_activity(top_zones, kind='pickup', top_n=10)
analyzer.plot_zone_activity(top_zones, kind='dropoff', top_n=10)
analyzer.plot_zone_activity(top_zones, kind='total', top_n=10)

#Percentage of trips are paid with cash vs. card

analyzer = TaxiDataAnalyzer(db_params)
payment_stats = analyzer.compute_payment_distribution('lpep_trip_data')
print("Payment Type Percentages:", payment_stats)

#The average fare amount per trip

average_fare = analyzer.compute_average_fare('lpep_trip_data')
print("Average Fare Amount per Trip: $", average_fare)

analyzer.close_connection()
