import requests
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
from joblib import Parallel, delayed
from ciso8601 import parse_datetime

def main():
    tracker = FtxNFTVolumeTracker()

    current_time = dt.datetime.utcnow()
    date_range = pd.date_range(start="2021-10-01",end=current_time).to_pydatetime().tolist()

    def get_daily_volume(time):
        trades = tracker.get_day_trades(time)
        prices = tracker.get_day_prices(time, 'SOL/USD')
        vol = tracker.calc_aggregate_volume(trades, prices)

        return {
            'Date': time,
            'USD Volume': vol
        }

    rows = Parallel(n_jobs=30)(delayed(get_daily_volume)(day) for day in date_range)

    daily_volume = pd.DataFrame(rows)
    weekly_volume = tracker.calc_weekly_volume(daily_volume)
    print(weekly_volume)

    plt.figure(1)

    fig, ax = plt.subplots()
    ax.bar(daily_volume['Date'], weekly_volume["USD Volume"])
    plt.xlabel("Date")
    plt.ylabel("$ Volume")
    plt.title("FTX Daily NFT Volume")

    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig("daily_volume.png")

class FtxNFTVolumeTracker():
    def __init__(self):
        self.url = 'https://ftx.us/api/nft/all_trades'
        self.price_url = 'https://ftx.us/api/markets'

    def get_day_trades(self, day):
        start_time = day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + dt.timedelta(days=1)

        ids = set()
        all_trades = []

        while True:
            r = requests.get(self.url, params={
                'start_time': start_time.timestamp(),
                'end_time': end_time.timestamp()
            })

            data = r.json()
            trades = data['result']
            deduped_trades = [t for t in trades if t['id'] not in ids]

            if len(deduped_trades) == 0:
                break

            # print(f'Adding {len(trades)} trades with end time {end_time}')

            all_trades.extend(deduped_trades)
            ids |= {r['id'] for r in deduped_trades}
            end_time = min(parse_datetime(t['time']) for t in trades)

        return all_trades

    def get_day_prices(self, day, ticker):
        start_time = day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + dt.timedelta(days=1)

        ids = set()
        all_prices = []

        full_url = self.price_url + '/' + ticker + '/candles'

        while True:
            r = requests.get(full_url, params={
                'resolution': 60,
                'start_time': start_time.timestamp(),
                'end_time': end_time.timestamp()
            })

            data = r.json()
            prices = data['result']
            deduped_prices = [t for t in prices if t['startTime'] not in ids]

            if len(deduped_prices) == 0:
                break

            # print(f'Adding {len(prices)} prices with end time {end_time}')

            all_prices.extend(deduped_prices)
            ids |= {r['startTime'] for r in deduped_prices}
            end_time = min(parse_datetime(t['startTime']) for t in prices)

        return all_prices

    def calc_aggregate_volume(self, trades, prices):
        try:
            df = pd.DataFrame(trades)

            df_prices = pd.DataFrame(prices)
            df_prices = df_prices.set_index('startTime')

            df['time'] = df['time'].apply(lambda t: parse_datetime(t).replace(second=0, microsecond=0).isoformat())
            df.loc[df['quoteCurrency'] == 'USD', 'usdPrice'] = df['price']

            def set_usd_price(r):
                if r['usdPrice'] != r['usdPrice']:
                    pass

                r['usdPrice'] = r['price'] * df_prices.loc[r['time'], 'close']

                return r

            df = df.apply(set_usd_price, axis=1)

            vol = df['usdPrice'].sum()

            return vol

        except:
            return

    def rename_column_dates(self, Date):
        index_day_of_week = Date.strftime("%A")
        offset_from_monday = 0
        if index_day_of_week == 'Tuesday':
            offset_from_monday = 1
        elif index_day_of_week == 'Wednesday':
            offset_from_monday = 2
        elif index_day_of_week == 'Thursday':
            offset_from_monday = 3
        elif index_day_of_week =='Friday':
            offset_from_monday = 4
        elif index_day_of_week == 'Saturday':
            offset_from_monday = 5
        elif index_day_of_week == 'Sunday':
            offset_from_monday = 6

        new_date = Date - pd.Timedelta(days = offset_from_monday)
        return (new_date.strftime('%Y/%m/%d') + '%' + str(offset_from_monday))

    def calc_weekly_volume(self, daily_volume):
        print(daily_volume)
        daily_volume['Date'] = daily_volume['Date'].apply(lambda t: self.rename_column_dates(t))
        weekly_df = daily_volume.groupby(daily_volume.columns.str.split('%').str[0], axis=1).sum()

        return weekly_df

if __name__ == "__main__":
    main()