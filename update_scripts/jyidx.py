"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

import numpy as np
import pandas as pd

from base import UpdaterBase


class JYIdxUpdater(UpdaterBase):
    """The updater class for collections 'jyidx'."""

    def __init__(self, timeout=30*60):
        super(JYIdxUpdater, self).__init__(timeout)

    def pre_update(self):
        self.collection = self.db.jydata
        self.dates = self.db.dates.distinct('date')

    def pro_update(self):
        return

        self.logger.debug('Ensuring index dtype_1_date_1_sid_1 on collection {}', self.collection.name)
        self.collection.ensure_index([('dtype', 1), ('date', 1), ('sid', 1)], background = True)

    @staticmethod
    def norm(df1, df0=None, col=None):
        if df0 is None:
            df1[col] = [0 if np.isnan(i) else i for i in df1[col]]
        else:
            df1[col] = [i if np.isnan(j) else j for i, j in zip(df0[col], df1[col])]

    def update(self, date):
        date = self.dates[self.dates.index(date)-1]
        cursor = self.db.jydata.find({'date': date}, {'_id': 0, 'date': 0})
        self.data = pd.DataFrame(list(cursor))
        if len(self.data) == 0:
            return
        self.update_Q()

    def update_Q(self):
        data0 = self.data.query("dtype == 'Q0'")
        data0.index = data0.sid
        data1 = self.data.query("dtype == 'Q-1'")
        data1.index = data1.sid
        data1 = data1.reindex(index=data0.index)


if __name__ == '__main__':
    jyidx = JYIdxUpdater()
    jyidx.run()
