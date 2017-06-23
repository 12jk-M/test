# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 10:20:04 2017

@author: PRui
"""

from config.xmlpath import XML
from WindPy import *
import numpy as np
from database.Mongo import ResultDBs
import threading
import datetime as dt

class runJob(object):
    '''
        rt_latest : 最新成交价
        rt_time : 时间
        rt_pct_chg : 涨跌幅
        rt_chg : 涨跌
        rt_date : 日期
        rt_swing : 振幅
        rt_pre_close : 昨日收盘价
        rt_high : 最高
        rt_open : 今开
        rt_low : 最低 
        rt_vol : 成交量
        '''
    def __init__(self):
        self.xml =XML()
        self.rlock =threading.RLock()
       
    def futures5minite(self):
        w.start()
        index = ['open','high','low','close','volume','amt','chg','pct_chg','oi']
        w.wsq(self.klinepair.keys(), ','.join(index),func=self.WSQCallback)




if __name__ =='__main__':
    runjob=runJob()
    if runjob.xml.getrunFlag()=='1':
        runjob.futures5minite()
