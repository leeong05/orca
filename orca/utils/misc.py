"""
.. moduleauthor:: Li, Wang <wangziqi@foreseefund.com>
"""

from orca.mongo.industry import IndustryFetcher
INDUSTRY = IndustryFetcher()

def industry2name(pdobj):
    pdobj = pdobj.copy()
    ind_name = INDUSTRY.fetch_info()
    pdobj.index = [ind_name[ind] for ind in pdobj.index]
    return pdobj

def name2industry(pdobj):
    pdobj = pdobj.copy()
    name_ind = {v: k for k, v in INDUSTRY.fetch_info().iteritems()}
    pdobj.index = [name_ind[name] for name in pdobj.index]
    return pdobj
