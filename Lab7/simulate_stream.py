import pandas as pd
import time
import os

# Load full dataset
df = pd.read_csv("car_prices.csv")

# Create streaming folder
stream_folder = "stream_data"
if not os.path.exists(stream_folder):
    os.makedirs(stream_folder)

# Write rows one-by-one to simulate real-time stream
for i, row in df.iterrows():
    temp_df = pd.DataFrame([row])
    temp_df.to_csv(f"{stream_folder}/car_{i}.csv", index=False)
    print(f"Streamed row {i}")
    time.sleep(1)  # Simulate delay
