# %%
import sys
sys.path.append('..')
sys.path.append('../tickers')

import pandas as pd
import requests
import dbconfig
from hiroku import hiroku
import datetime,time
from apscheduler.schedulers.background import BackgroundScheduler


os.environ['TZ'] = 'UTC'

KST = datetime.timezone(datetime.timedelta(hours=9))
UTC = datetime.timezone.utc

token = dbconfig.TIINGO_TOKEN

class sp500_daily(hiroku):
    def __init__(self):
        hiroku.__init__()
        self.get_tickers()
        self.columns = 'ticker, date, open, high, low, close, volume,\
            adjopen, adjhigh, adjlow, adjclose, adjvolume, divcash, split'
        self.sched = BackgroundScheduler()
        self.shced.add_job(self.db_scheduled, 'cron', hour ='00', id = 'mkdata_sp500_daily')

    def get_tickers(self):
        tickers = pd.read_csv('../tickers/sp500_tickers.csv')
        self.tickers = tickers.Symbol
    
    def get_tiingo(self, ticker):
        start_date = hiroku.get_latest_date('market_sp500_daily',ticker)[1]
        last_date = datetime.date.today()
        headers = {'Content-Type': 'application/json'}
        params = {'startDate': start_date, 'endDate': last_date, 'token': token}
        
        requestResponse = requests.get(f"https://api.tiingo.com/tiingo/daily/{ticker}/prices",
                                       headers=headers, params=params)
        results = requestResponse.json()
        return results
    
    def db_scheduled(self):
        hiroku.connection_check()
        print('Starts')
        failed_list = []
        
        for ticker in self.tickers:
            temp = self.get_tiingo(ticker)
            insert_res = hiroku.insert('market_sp500_daily', temp, self.columns)
            if insert_res == 0:
                failed_list.append(ticker)
        print(f'DB insert completed at {datetime.datetime.now(KST)} as KST')
        if len(failed_list != 0):
            print(f'Failed tickers: {failed_list}')

if __name__=='__main__':
    instance = sp500_daily()
    instance.sched.start()
    while True:
        time.sleep(1)