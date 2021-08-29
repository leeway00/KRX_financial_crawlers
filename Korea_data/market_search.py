#%%
import pandas as pd
import sys
sys.path.append('..')


tickers = pd.read_csv('krx_tickers.csv')
# %%
def find_value(col, args):
    return tickers[tickers[col].isin([args])]
# %%
find_value('name','코윈테크')
# %%
