"""
    example.py
    ~~~~~~~~~~

    Loads QUANT Clean data from all devices, converts it into a format suitable
    for analysis and demonstrates basic use cases.
"""

import seaborn as sns
import matplotlib.pyplot as plt

from load_data import load_data

quant_folder = "/home/stuart/Documents/quant_data/clean/"
df = load_data(quant_folder, companies=["Aeroqual", "AQMesh"],
               start="2020-01-01", end="2020-04-28")
print(df)

aqmesh_march = df.query("(manufacturer == 'AQMesh') & (timestamp > '2020-03-17') & (timestamp < '2020-03-20')")
plt.figure(figsize=(15,10))
sns.lineplot(data=aqmesh_march, x='timestamp', y='NO2', hue='device');
plt.show()

print(df.set_index('device').isnull()['NO2'].groupby('device').mean())
