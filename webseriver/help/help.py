# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 15:19:44 2016
help file
@author: lywen
"""
import datetime as dt
import numpy as np

def getNow(format='%Y-%m-%d %H:%M:%S'):
    now = dt.datetime.now()
    try:
       return now.strftime(format)
    except:
        return None
       
       
       
def strTodate(s,formats='%Y-%m-%d'):
    try:
        return dt.datetime.strptime(s,formats)
    except:
        return None
       
def dateTostr(s,formats='%Y-%m-%d'):
    try:
        return dt.datetime.strftime(s,formats)
    except:
        return None
        
def strTostr(s,format1='%Y-%m-%d',format2='%Y-%m-%d %H:%M:%S'):
    try:
        return dt.datetime.strptime(s,format1).strftime(format2)
    except:
        return None
        
def timedelta(s,day,format='%Y-%m-%d'):
   try:
      return (dt.datetime.strptime(s,format)+dt.timedelta(day)).strftime(format)
   except:
       return None
        
        

     
def dictTostr(D):
    """
    example:
    D=[{'a':1,'b':3},{'c':1,'d':3}]
    
    return:
       ['a =1 and b=3','c =1 and b=3']
    """
    tmp=[]
    for item in D.items():
            s = u"""%s="""%item[0]
            if type(item[1]) is  str or type(item[1]) is  unicode:
                s+=u"""'%s'"""%item[1]
            else:
                s+=u"""%s"""%item[1]
            tmp.append(s)
             
    return  ' and '.join(tmp)
    
def getcurrency(code):
    """
    根据货币代码回去对应的拆借利率名称
    """        
    if code=='CNY' or code=='CNH':
        return "Shibor人民币"
        
        
    elif code=='USD':
        return "Libor美元"
        
    elif code=='GBP':
        return "Libor英镑"
        
    elif code=='JPY':
        return "Libor日元"
        
    elif code=='EUR':
        
        return "Euribor欧元"
    
        
    elif code=='AUD':
        return '澳大利亚元'
    
    elif code=='CAD':
        return '加拿大元'
        
        
def getlocalflag(_type,currency,paircurrency):
    """
    _type:交易类型，购汇、结汇、互换
    currency:卖出货币  
    paircurrency:汇率对
    获取损益的符号
    """
    if _type=='1':
        return 1##购汇
        
    elif _type=='2':
        return -1##结汇
        
    elif _type=='3':
        if currency==paircurrency[:3]:
            return -1
        else:
            return 1
    else:
        return 0
        
    