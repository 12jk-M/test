# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 15:36:08 2017
 * 多线程获取
 * 存储数据
@author: candy
"""


from WindPy import *
import traceback
import numpy as np
import time
from help.helper import listsplit
from database.Mongo import ResultDBs
import datetime as dt

w.start()


class threadKlinefx(threading.Thread):
    '''
    继承了多线程类
    '''
    def __init__(self,currencpairLst,rlock):
       threading.Thread.__init__(self)
       self.currencpairLst =currencpairLst
       self.rlock = rlock
       
       

    def  run(self):
        '''
        针对单个货币对的操作
        带锁的执行（存入）返回数据
        
        
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

        '''
        self.result=[]
        index = ['rt_chg','rt_latest','rt_date','rt_time','rt_high','rt_low','rt_pre_close','rt_open','rt_swing','rt_pct_chg']
        for currencypair in self.currencpairLst:
            time.sleep(0.1)
            check = 0
            while (check<3):
                try:               
                    Gettime = dt.datetime.now()  
            #        rq = w.wsq(key, "rt_chg,rt_latest,rt_date,rt_time,rt_high,rt_low,rt_pre_close,rt_open,rt_swing,rt_pct_chg")
                    rq = w.wsq(currencypair, ','.join(index))
                    Data =np.array(rq.Data).flatten()
                    #print currencypair,':',Data.sum()
                    if Data.sum()!=0:
                        reDict =dict(zip(index,Data))        
                        reDict['CurrencyPair'] = currencypair
                        reDict['Time'] = str(rq.Times[0])
                        reDict['ErrorCode'] =rq.ErrorCode
                        reDict['Gettime'] = Gettime                      
                        
                        wh={}
                        wh['CurrencyPair'] =  reDict['CurrencyPair']
                        wh['Time'] = reDict['Time']
                        
                        self.result.append((wh,reDict))
                        #print 'get successful' 
                        #Set.add(currencypair)
                        check=3                        
                    else:
                        check+=1
                        print 'get null data'
                        print 'Gettime: ',Gettime ,currencypair,':',Data.sum()
                        time.sleep(1)                        
                        
                except:
                    check+=1
                    print traceback.print_exc()
                    time.sleep(1)
                
#重写数据库模块
                
        while self.rlock.acquire() == False:

            print 'waiting'
            time.sleep(0.1)
            self.rlock.release()
    
        if self.rlock.acquire():
            try:    
                    database = 'fes'
                    indexlist=["CurrencyPair","Time"]                    
                    sql = ResultDBs('windfx',database,indexlist)
                    sql.save(self.result)
                    self.rlock.release()     
                    #print self.result
            except Exception,e:
                print Exception,':',e
            self.rlock.release()

 
  

            

    
class threadKlineExcute(object):
        
    """
    1、循环生成并执行线程
    2、这里线程是用的全局锁，虽然这里只有一个excute，但也无妨，这里是双锁
    """
    def __init__(self, klinepair,rlock):
        self.klinepair = klinepair
        self.rlock = rlock
        
    def run(self,threadnum=10):
        """
        threadnum:执行的线程数量
        threadnum=0,表示调用最多线程数
        """
        
        if threadnum<=0:
            threadnum = len(self.klinepair)
        #requestdata = []
        threadsplit = listsplit(self.klinepair.keys(),threadnum)##均等分割用于多线程调用
        ths = []        
        for job in threadsplit:
            ths.append(threadKlinefx(currencpairLst=job,rlock=self.rlock))##执行多进程
        return ths