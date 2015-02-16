import pandas as pd

from orca.combiner.regression import (
        OLSCombiner,
        QuantRegCombiner,
        RidgeCombiner,
        LassoCombiner,
        )

class MyOLSCombiner(OLSCombiner):

    def normalize(self):
        self.orginal_data = self.data.copy()
        Ys = []
        for date, df in self.data.groupby('date'):
            Y = df.returns.copy()
            pos = Y >= Y.quantile(0.75)
            Y[pos] = 1
            Y[~pos] = -1
            Ys.append(Y)
        self.data['returns'] = pd.concat(Ys)
        Y = self.data.returns.copy()
        pos = self.data[self.data.returns == 1]
        neg = self.data[self.data.returns == -1]
        self.data = pd.concat([pos, pos, pos, neg])

    def fit(self, X, Y):
        results = sm.OLS(Y, X).fit()
        self.info('Regression summary:\n{}'.format(results.summary()))
        X = self.original_data.iloc[:, 2:-1]
        return pd.Series(results.predict(X), index=X.index).unstack()


groups = {
        'wdfund': {
            'combiner': OLSCombiner(20),
            'alphas': {
                'wdfund1': {'path': 'wdfund1.msgpack'},
                'wdfund2': {'path': 'wdfund2.msgpack'},
                'wdfund3': {'path': 'wdfund3.msgpack'},
                'wdfund4': {'path': 'wdfund4.msgpack'},
                'jyfund1': {'path': 'jyfund1.msgpack'},
                'jyfund2': {'path': 'jyfund2.msgpack'},
                'jyfund3': {'path': 'jyfund3.msgpack'},
                'jyfund4': {'path': 'jyfund4.msgpack'},
                'jyfund5': {'path': 'jyfund5.msgpack'},
                },
            },
        }

group_combiner = {
        'combiner': OLSCombiner(20),
        'groups': None,
        'output': 'wdfund.msgpack',
        'filetype': 'msgpack',
        }
