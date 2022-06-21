#%%
import pandas as pd
import datetime
import numpy as np
import sys
from tqdm import tqdm

# %%
tickers = pd.read_csv('../Korea_data/krx_tickers_merged.csv')
tickers['corp_code'] = tickers.corp_code.apply(
    lambda x: '0'*(8-len(str(x)))+str(int(x)))
tickers['ticker'] = tickers.ticker.apply(
    lambda x: '0'*(6-len(str(x)))+str(int(x)))

nav_a = pd.read_csv('../Korea_data/krx_fin/annual_fin.csv')
nav_q = pd.read_csv('../Korea_data/krx_fin/quarter_fin.csv')

price = pd.read_csv('../Korea_data/krx_market/krx_marketdata_monthly.csv')
# %%
