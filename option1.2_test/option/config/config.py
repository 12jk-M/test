# -*- coding: utf-8 -*-
"""
Created on Tue Nov 22 14:41:15 2016

@author: PRui
"""
"""
全局汇率变量，{‘汇率对’：‘历史汇率最早时间’}
"""
class globalrate:
    rate = None
        
def setrate():
    from database.database import mongodb## mongo connect
    from help.help import getNow,strTostr ##get current datetime
    key={'code':True}
    Time = strTostr(getNow('%Y-%m-%d'))
    initial ={'Time':Time}
    reduces = """function(doc,prev){
                if (prev.Time>doc.Time){
                prev.Time = doc.Time
                }}"""
    condition={'type':'5'}   
    db = mongodb()         
    rate = db.group('kline',key,condition,initial,reduces)
    rate = map(lambda x:{x['code']:x['Time'].split()[0]},rate)
    globalrate.rate = reduce(lambda x,y:dict(x,**y),rate)
    db.close()
    
def getrate():
    return globalrate.rate