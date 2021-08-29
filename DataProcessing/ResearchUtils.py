import pandas as pd
import numpy as np
import OpenDartReader

class FindTickers:
    """
    # keywords
        data = any data that have ticker and name of firms
    # methods:
        self.tickers -> pd.Series : 
            Series of tickers that have a name as index
        self.names -> np.array : 
            array of names, type = str
        self.namefind('keyword') -> pd.Series :  
            Series of tickers(with index of name) whose name or ticker contain 'keyword'
    # Example
        tickers = FindTickers(total) #make a class object once
        tickers.search('삼성') #find whenever you need
        =>  삼성화재            810
            삼성제약           1360
            삼성전자           5930
            삼성SDI          6400
            삼성공조           6660
            삼성전기           9150
            삼성중공업         10140
            삼성증권          16360
            삼성에스디에스       18260
            삼성엔지니어링       28050
            삼성물산          28260
            삼성카드          29780
            삼성생명          32830
            삼성출판사         68290
            삼성바이오로직스     207940
            삼성스팩2호       291230
            삼성머스트스팩3호    309930
            dtype: int64
    """
    def __init__(self, data):
        if type(data) == pd.Series:
            self.tickers = data
            self.names = data.index.to_numpy().astype('str')
        elif 'ticker' in str(data.columns):
            self.tickers = pd.Series(data = data.ticker.unique(), index = data.name.unique())
            self.names = self.tickers.index.to_numpy().astype('str')
        else:
            self.tickers = pd.Series(data)


    def search(self, keyword:'str or int'):
        if type(keyword) == str:
            nominees = self.names[np.char.find(self.names, keyword) != -1]
        else:
            nominees = (self.tickers == keyword)
        if len(nominees) == 0: 
            return 'Nothing returned'
        return self.tickers[nominees]



if __name__ == '__main__':
    pass