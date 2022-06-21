# %%
from Korea_data.tradingview import check
import datetime
import pandas as pd
import os
import sys
sys.path.append('..')

# %%
# %%
tview = pd.read_csv("../tview0825.csv")
tview.drop(tview.columns[0], axis=1, inplace=True)

cols = tview.columns
cols = [i.split('.')[1] if len(i) > 10 else i for i in cols]

# %%
tview.columns = cols

# %%
tview.sort_values(['All|1D', 'All|1W', 'All|1M'],
                  ascending=False, inplace=True)
# %%
tview.head(10)


# %%

# %%
check(2083)
# %%

# %%
tview.timestamp = pd.to_datetime(tview.timestamp).apply(lambda x: x.date())
# %%
tview.timestamp = datetime.date(year=2021, month=8, day=25)
# %%
tview.to_csv('tview0825.csv')
# %%
today = datetime.date.today()
# %%
today.month
# %%
data = [f"Recommend.Other", f"Recommend.All", f"Recommend.MA", f"RSI", f"RSI[1]", f"Stoch.K", f"Stoch.D", f"Stoch.K[1]", f"Stoch.D[1]", f"CCI20", f"CCI20[1]",
                                 f"ADX", f"ADX+DI", f"ADX-DI", f"ADX+DI[1]", f"ADX-DI[1]", f"AO", f"AO[1]", f"AO[2]",
                                 f"Mom", f"Mom[1]", f"MACD.macd", f"MACD.signal", f"Rec.Stoch.RSI", f"Stoch.RSI.K",
                                 f"Rec.WR", f"W.R", f"Rec.BBPower", f"BBPower", f"Rec.UO", f"UO", f"EMA10", f"close",
                                 f"SMA10", f"EMA20", f"SMA20", f"EMA30", f"SMA30", f"EMA50", f"SMA50", f"EMA100", f"SMA100", f"EMA200", f"SMA200",
                                 f"Rec.Ichimoku", f"Ichimoku.BLine", f"Rec.VWMA", f"VWMA", f"Rec.HullMA9", f"HullMA9",
                                 f"Pivot.M.Classic.S3", f"Pivot.M.Classic.S2", f"Pivot.M.Classic.S1", f"Pivot.M.Classic.Middle",
                                 f"Pivot.M.Classic.R1", f"Pivot.M.Classic.R2", f"Pivot.M.Classic.R3", f"Pivot.M.Fibonacci.S3",
                                 f"Pivot.M.Fibonacci.S2", f"Pivot.M.Fibonacci.S1", f"Pivot.M.Fibonacci.Middle", f"Pivot.M.Fibonacci.R1",
                                 f"Pivot.M.Fibonacci.R2", f"Pivot.M.Fibonacci.R3", f"Pivot.M.Camarilla.S3", f"Pivot.M.Camarilla.S2",
                                 f"Pivot.M.Camarilla.S1", f"Pivot.M.Camarilla.Middle", f"Pivot.M.Camarilla.R1", f"Pivot.M.Camarilla.R2",
                                 f"Pivot.M.Camarilla.R3", f"Pivot.M.Woodie.S3", f"Pivot.M.Woodie.S2", f"Pivot.M.Woodie.S1",
                                 f"Pivot.M.Woodie.Middle", f"Pivot.M.Woodie.R1", f"Pivot.M.Woodie.R2", f"Pivot.M.Woodie.R3", f"Pivot.M.Demark.S1",
                                 f"Pivot.M.Demark.Middle", f"Pivot.M.Demark.R1"]

# %%
for i in data:
    print(i)
# %%
