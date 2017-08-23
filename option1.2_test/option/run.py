# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 16:36:54 2016

@author: lywen
"""
import traceback
import threading
from config.config import setrate
from price.forwards import forwards##普通外汇远期
from price.collaoptions import CollaOptions##领式期权

from price.swapoption import SwapOptions## 区间式货币掉期or货币互换or封顶式期权

from price.participateforwards import participateforwards##参与式远期

from price.knockoptions import knockoptions##敲出式期权


from price.targetforwards import TargetRedemptionForwards##目标可赎回式远期

from database.database import postgersql
from config.postgres  import table_write,table_write_tmp

if '__main__'==__name__:
    #fd = participateforwards(0.1)
    #for items in [participateforwards,CollaOptions,knockoptions,forwards,SwapOptions,TargetRedemptionForwards]:
        #items()
        #TargetRedemptionForwards()
    setrate()##获取历史汇率最早时间全局变量
    
    #post = postgersql()
    #post.deltetable(table_write_tmp)##清空数据库
    #post.close()
    thds = []
    items = [participateforwards,CollaOptions,knockoptions,forwards,SwapOptions,TargetRedemptionForwards]
    Lock = threading.RLock()
    #SwapOptions(Lock)
    
    try:
        for item in items:
            thds.append(threading.Thread(target=item,args=(Lock,)))        
        for thd in thds:
            thd.start()        
        for thd in thds:
            thd.join()
    except:
        traceback.print_exc()

    post = postgersql()
    post.deltetable(table_write,'date<current_date')##清空数据库
    sql = """insert into %s select * from %s where date<current_date"""%(table_write,table_write_tmp)
    post.view(sql)
    post.close()
  
    
    #try:
        #for items in [participateforwards,CollaOptions,knockoptions,forwards,SwapOptions,TargetRedemptionForwards]:
            #items()
    #except:
        #traceback.print_exc()
        
        