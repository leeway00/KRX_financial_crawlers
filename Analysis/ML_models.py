# %%
from abc import abstractstaticmethod
import pandas as pd
import numpy as np

from tabulate import tabulate
from sklearn.model_selection import train_test_split as tts
from sklearn.model_selection import TimeSeriesSplit as tss

from sklearn.linear_model import *

import matplotlib.pyplot as plt

# %%


class Baseline():

    def __init__(self, data_x, data_y):
        from sklearn import linear_model

        self.x = data_x
        self.y = data_y
        pass

    def train_test_split(self, data_x, data_y, test=0.2):

        def split(data):
            train, test = tts(data, shuffle=False, test_size=test)
            # train, var = tts(temp_train, shuffle=False, test_size=val)
            return train,  test
        self.train_x, self.test_x = split(data_x)
        self.train_y, self.test_y = split(data_y)
        pass

    def validation_split(self, n_splits, max_train_size=0, test_size=0):
        self.train_iteration = n_splits
        self.tscv = tss(n_splits=n_splits)
        self.indices = [i for i in self.tscv.split(self.x)]
        # self.indices = [i[0] for i in self.indice], [ i[1] for i in self.indice]

    def sample_generate(self, indices):
        train_index, test_index = indices
        x_train, x_test = self.x.iloc[train_index], self.x.iloc[test_index]
        y_train, y_test = self.y.iloc[train_index], self.y.iloc[test_index]
        return x_train, x_test, y_train, y_test

    def train(self, model):

        self.params = []
        self.pred = []
        self.scores = []
        data_length = []

        for indice in self.indices:
            x_train, x_val, y_train, y_val = self.sample_generate(indice)
            data_length.append((len(y_train), len(y_val)))
            self.temp = y_train
            model.fit(X=x_train, y=y_train)
            self.params.append(model.get_params())
            self.pred.append(model.predict(x_val))
            self.scores.append(model.score(x_val, y_val))

        res = pd.DataFrame()
        res['train_iteration'] = [i for i in range(1, self.train_iteration+1)]
        res['scores'] = self.scores
        res['train_data_count'] = [i[0] for i in data_length]
        res['val_data_count'] = [i[1] for i in data_length]

        self.train_result = res
        print(res.to_markdown())
        return model

    @abstractstaticmethod
    def test_model(data):
        pass

    def plot_output(self, iteration = -1):
        if iteration == -1:
            iteration = self.train_iteration
        params = self.params[iteration]
        model = self.model.copy()
        model.params(params)
        plt.figure()
        

        pass


class linear_classifiers(Baseline):
    def __init__(self, data_x, data_y):
        super().__init__(data_x, data_y)

    def preprocess_y(self, data):
        return data.apply(lambda x: 1 if x >= 0 else 0)

    def set_model(self, kind='logit', cv=5):

        if kind == 'logit':
            model = LogisticRegressionCV(cv=cv)
        elif kind == 'ridge':
            model = RidgeClassifierCV(cv=cv)
        elif kind == 'sgd':
            model = SGDClassifier()
        self.model = model

    def ensemble(self, ):
        pass

    def train(self):
        self.y = self.preprocess_y(self.y)
        self.model = super().train(self.model)

    def test_model(self, data_x, data_y):
        
        self.test_label = self.preprocess_y(data = data_y)
        
        self.test_results = self.model.predict(data_x)
        self.test_scores = self.model.score(data_x, self.test_label)
        return self.test_scores


class linear_regressors(Baseline):
    def __init__(self, data_x, data_y):
        super().__init__(data_x, data_y)

    def set_model(self, kind='linear', cv=5):
        if kind == 'linear':
            model = LinearRegression()
        elif kind == 'Ridge':
            model = RidgeCV(cv=cv)
        elif kind == 'SGD':
            model = SGDRegressor()
        elif kind == 'ElasticNet':
            model = ElasticNetCV(cv=cv)
        elif kind == 'LARs':
            model = LarsCV(cv=cv)
        elif kind == 'LassoLARs':
            model = LassoLarsCV(cv=cv)
        else:
            raise Exception('Selected kind of model is unavailable')
        self.model = model

    def train(self):
        self.model = super().train(self.model)

    def test_model(self, data_x, data_y):
        self.test_results = self.model.predict(data_x)
        self.test_scores = self.model.score(data_x, data_y)
        return self.test_scores


# %%
if __name__ == "__main__":
    import dbconfig

    conn = dbconfig.hiroku()
    table = 'market_sp500_daily'
    query = f'select * from {table} where ticker= \'AAPL\''
    # query = f'select * from {table} where date = \'2021-08-02\''
    res = pd.read_sql_query(query, conn.connection)
    tickers = res.ticker

    # %%
    res['daily'] = np.nan
    for i in tickers:
        temp = res[res.ticker == i]
        res['daily'][temp.index] = temp.close.pct_change()

    res = res.dropna()
    # %%
    res['classic_pivot'] = (res.high+res.low+res.close)/3
    res['high_low'] = (res.high-res.low)/res.open
    res['close_open'] = (res.close- res.open)/res.open
    
    # %%
    res_test = res.iloc[int(len(res)*0.9):]
    res_train = res.iloc[:int(len(res)*0.9)]

    # %%
    temp = linear_classifiers(res_train.drop(
        ['ticker', 'date', 'daily'], axis=1), res_train.daily)
    temp.validation_split(5)
    temp.set_model(kind='logit')
    temp.train()

    # %%
    temp.test_model(data_x = res_test.drop(['ticker', 'date', 'daily'], axis=1), 
                    data_y = res_test.daily)

    # %%
