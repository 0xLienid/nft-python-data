import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def millions(x, pos):
    'The two args are the value and tick position'
    return '%1.1fM' % (x * 1e-6)

df = pd.read_csv('niftygateway_weekly_volume.csv', header=0)
df = df[df['timestamp'] >= '2021-01-01']
print(df)

formatter = FuncFormatter(millions)
fig, ax = plt.subplots()
plt.bar(df['timestamp'], df["tx_amount"])
plt.ticklabel_format(style='plain', axis='y')
plt.gcf().autofmt_xdate()
ax.yaxis.set_major_formatter(formatter)
plt.xlabel("Date")
plt.ylabel("$ Volume")
plt.title("NiftyGateway Weekly NFT Volume")
plt.show()