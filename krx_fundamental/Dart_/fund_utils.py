#%%
import pandas as pd
import numpy as np
import datetime

class FundUtils:
    @staticmethod
    def find_value(data, col, args):
        return data[data[col].isin([args])]

    @staticmethod
    def report_date(num):
        return datetime.datetime.strptime(num[:8], "%Y%m%d")

    @staticmethod
    def remove(data, col='fs_div', *args):
        if data[col].unique()==['CFS', 'OFS']:
            return data[data[col] == 'CFS']
        else:
            return data

    @staticmethod
    def sort_ifrs(data):
        return data[~data['account_id'].isin(['-표준계정코드 미사용-'])]
    
    @staticmethod
    def str_format(data, col, length):
        return data[col].apply(lambda x: '0'*(length-len(str(x)))+str(int(x)))



def check_response(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            print('Error')
            return None
    return wrapper