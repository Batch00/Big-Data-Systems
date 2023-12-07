import os
import pandas as pd
import matplotlib.pyplot as plt
import json

partition_numbers = [0, 1, 2, 3]

def find_latest_data(partition_numbers):
    latest_data = {}

    for month in ['January', 'February', 'March']:
        latest_avg = 0
        for partition in partition_numbers:
            file_path = f'/files/partition-{partition}.json'
            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    partition_data = json.load(file)
                    if month in partition_data:
                        latest_year = max(partition_data[month], key=int)
                        avg_temp = partition_data[month][latest_year]['avg']
                        if avg_temp > latest_avg:
                            latest_avg = avg_temp
                            latest_data[f"{month}-{latest_year}"] = avg_temp
    
    return latest_data

latest_month_data = find_latest_data(partition_numbers)
#print(latest_month_data)

month_series = pd.Series(latest_month_data)

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
ax.set_xlabel('Month')
plt.tight_layout()
plt.savefig("/files/month.svg")
