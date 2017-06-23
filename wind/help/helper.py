# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 17:16:23 2016
帮助模块
@author: lywen
"""

def listsplit(Lst,splitnum):
    """
    对列表进行均等分割
    splitnum:分割份数
    """
    spacelist = []
    length = len(Lst)
    if splitnum<=1 or length< splitnum:
        return [Lst]
    else:
        
        num  = length / splitnum 
        
        
        for i in range(splitnum):
            spacelist.append(Lst[i*num:(i+1)*num])
        
        if length % splitnum>0:
             spacelist[-1].extend(Lst[(i+1)*num:])
        return spacelist
        

