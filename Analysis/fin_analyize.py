#%%
import pandas as pd
import numpy as np
import sys

import os
os.chdir('..')

recent = pd.read_csv('./Korea_data/krx_fin/quarter_fin.csv')
# %%
temp = recent[recent.date == '2021.06'].iloc[:,:-3].dropna().reset_index(drop = True)
# %%
temp
# %%
temp.sort_values(by = '영업이익률', ascending = False, kind = 'stable', inplace = True)
temp.sort_values(by = ['PBR(배)', 'PER(배)'], kind = 'stable', inplace = True)
#%%
temp.replace(to_replace = '-', value = '0', inplace = True)

t_cols = temp.columns.to_list()[4:]
for i in t_cols:
    temp[i] = pd.to_numeric(temp[i])

# %%
temp2 = temp[(temp['PBR(배)'] > 0.5) & (temp['PBR(배)'] < 1)].copy()
# %%
temp2.sort_values(by = '영업이익률', ascending = False, kind = 'stable', inplace = True)
# %%
temp['score'] = temp['영업이익률'] + temp['ROE(지배주주'] - temp['PER(배)'] - temp['PBR(배)']*10 
temp2 = temp[temp['ROE(지배주주)']>0].copy()
temp2.sort_values(by = 'score', ascending = False, inplace = True)
# %%
temp3 = temp2[['ticker','name','date','영업이익률','순이익률','ROE(지배주주)','부채비율','PER(배)','PBR(배)','score']].copy()
# %%

# %%
price = pd.read_csv('./Korea_data/krx_market/krx_marketdata.csv')
# %%
price
# %%
