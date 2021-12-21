import requests
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from joblib import Parallel, delayed
from ciso8601 import parse_datetime
import time
from matplotlib.ticker import FuncFormatter

def millions(x, pos):
    'The two args are the value and tick position'
    return '%1.1fM' % (x * 1e-6)

def get_week_vol(start_date, end_date):
    tracker = NiftyGatewayVolumeTracker()
    page_nums = tracker.get_page_numbers("day", start_date = start_date, end_date = end_date)
    if page_nums != "request failed":
        rows = [tracker.get_trades(i) for i in range(1, page_nums)]
        trades = pd.DataFrame()
        for row in rows:
            trades = pd.concat([trades, pd.DataFrame(row)])

        trades["tx_amount"] = trades["saleAmountInCents"] / 100

        weekly_volume = tracker.calc_weekly_volume(trades)
        weekly_volume.sort_values('timestamp')
        return weekly_volume

    return weekly_volume


def get_all_time_vol():
    tracker = NiftyGatewayVolumeTracker()
    page_nums = tracker.get_page_numbers("all")

    if page_nums != "request failed":
        rows = [tracker.get_trades(i) for i in range(1, page_nums)]
        trades = pd.DataFrame()
        for row in rows:
            trades = pd.concat([trades, pd.DataFrame(row)])

        trades["tx_amount"] = trades["saleAmountInCents"] / 100

        weekly_volume = tracker.calc_weekly_volume(trades)
        weekly_volume.sort_values('timestamp')
        return weekly_volume

    return

class NiftyGatewayVolumeTracker():
    def __init__(self):
        self.url = "https://api.niftygateway.com/market/all-data/?page=%7B%22current%22:{page},%22size%22:100%7D&filters=%7B%22exclude_types%22:[%22withdrawals%22,%22single_nifty_offers%22,%22global_nifty_offers%22,%22attribute_nifty_offers%22,%22primary_market_bids%22,%22gifts%22,%22deposits%22,%22listings%22]%7D"
        self.timeUrl = "https://api.niftygateway.com/market/all-data/?page=%7B%22current%22:{page},%22size%22:100%7D&filters=%7B%22exclude_types%22:[%22withdrawals%22,%22single_nifty_offers%22,%22global_nifty_offers%22,%22attribute_nifty_offers%22,%22primary_market_bids%22,%22gifts%22,%22deposits%22,%22listings%22],%22date_start%22:%22{start_date}+00:00:00%22,%22date_end%22:%22{end_date}+23:59:59%22%7D"

    def get_page_numbers(self, version, start_date = "", end_date = ""):
        r
        if version == "all":
            r = requests.get(self.url.format(page = 1))
        else:
            r = requests.get(self.timeUrl.format(page = 1, start_date = start_date, end_date = end_date))
        try:
            data = r.json()
            return data['data']['meta']['page'].total_pages
        except:
            return "request failed"

    def get_day_trades(self, i, start_date, end_date):
        trades = []

        for j in range(100):
            print(j)
            r = requests.get(self.url.format(page = i, start_date = start_date, end_date = end_date))
            try:
                data = r.json()
                if 'limit' in data['message']:
                    time.sleep(5)
                    continue
                else:
                    try:
                        trades = data['data']['results']
                        trades = [{'timestamp': t['Timestamp'], 'type': t['Type'], 'id': t['id'], 'birthingPurchaseAmountInCents': t['BirthingPurchaseAmountInCents'], 'saleAmountInCents': t['SaleAmountInCents']} for t in trades if t['BirthingPurchaseAmountInCents'] == 0]
                        break
                    except:
                        trades = data
                        trades = [{'timestamp': t['Timestamp'], 'type': t['Type'], 'id': t['id'], 'birthingPurchaseAmountInCents': t['BirthingPurchaseAmountInCents'], 'saleAmountInCents': t['SaleAmountInCents']} for t in trades if t['BirthingPurchaseAmountInCents'] == 0]
                        break
            except:
                continue

        return trades

    def get_trades(self, i):
        trades = []

        for j in range(100):
            print(j)
            r = requests.get(self.url.format(page = i))
            try:
                data = r.json()
                if 'limit' in data['message']:
                    time.sleep(5)
                    continue
                else:
                    try:
                        trades = data['data']['results']
                        trades = [{'timestamp': t['Timestamp'], 'type': t['Type'], 'id': t['id'], 'birthingPurchaseAmountInCents': t['BirthingPurchaseAmountInCents'], 'saleAmountInCents': t['SaleAmountInCents']} for t in trades if t['BirthingPurchaseAmountInCents'] == 0]
                        break
                    except:
                        trades = data
                        trades = [{'timestamp': t['Timestamp'], 'type': t['Type'], 'id': t['id'], 'birthingPurchaseAmountInCents': t['BirthingPurchaseAmountInCents'], 'saleAmountInCents': t['SaleAmountInCents']} for t in trades if t['BirthingPurchaseAmountInCents'] == 0]
                        break
            except:
                continue

        return trades

    def rename_column_dates(self, Date):
        index_date = pd.to_datetime(Date, format='%Y/%m/%d', 
                                exact = False)

        index_day_of_week = index_date.strftime("%A")
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

        new_date = index_date - pd.Timedelta(days = offset_from_monday)
        return (new_date.strftime('%Y/%m/%d') + '%' + str(offset_from_monday))

    def calc_weekly_volume(self, daily_volume):
        daily_volume.sort_values('timestamp')
        daily_volume['timestamp'] = daily_volume['timestamp'].apply(lambda t: self.rename_column_dates(t))
        daily_volume = daily_volume[['timestamp', 'tx_amount']]
        daily_volume['timestamp'] = daily_volume['timestamp'].apply(lambda t: t.split('%')[0])
        weekly_df = daily_volume.groupby('timestamp').sum()

        return weekly_df
