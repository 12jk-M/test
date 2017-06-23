# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 13:50:30 2017

@author: candy
"""


#import  xml.dom.minidom as xmlpath
from lxml import objectify

##****


##*****




class XML(object):
    def __init__(self):
        xml = objectify.parse(open('config/config.xml'))
        self.root = xml.getroot()
        
    def getrunFlag(self):
        flag = self.root.find('runmodel')
        if flag is not None:
            return flag.text
        else:
            return '0'
        
    def getlogprint(self):
        """
        获取打印属性，默认状态为False
        """
        log = self.root.find('log')
        if log is not None:
            
            logprint = log.find('print')
            return bool(logprint)
        else:
            return False
            
    def getdatabaseconfig(self):
        """
        获取数据库配置文件
        """
        tmp = self.root.find('database')
        db = tmp.find('db')
        user = tmp.find('user')
        password = tmp.find('password')
        host = tmp.find('host')
        port = tmp.find('port')
        return str(db),str(user),str(password),str(host),int(port)
            
    
    def getkline(self):
        
        """
        1、获取K线图汇率对列表
        2、一共七十个货币对，但有如下27各获取不到，如下
            USDIDR.FX : 美元印尼盾
            NZDAUD.FX : 新西兰元澳元
            RUBCNY.FX : 俄罗斯卢布人民币
            XAGEUR.FX : 白银欧元
            XAGUSD.FX : 白银美元
            NZDCHF.FX : 新西兰元瑞郎
            USDRUB.FX : 美元俄罗斯卢布
            XAGGBP.FX : 白银英镑
            KRWCNY.FX : 韩元人民币
            CNHXAG.FX : 离岸人民币白银
            XAUGBP.FX : 黄金英镑
            XAUUSD.FX : 黄金美元
            NZDHKD.FX : 新西兰元港元
            TWDCNY.FX : 新台币人民币
            THBCNY.FX : 泰铢人民币
            USDTRY.FX : 美元土耳其里拉
            NZDEUR.FX : 新西兰元欧元
            NZDCAD.FX : 新西兰元加元
            CNHXAU.FX : 离岸人民币黄金
            XAUEUR.FX : 黄金欧元
            USDBRL.FX : 美元巴西雷亚尔
            USDINR.FX : 美元印度卢比
            CNHCNY.FX : 离岸人民币人民币
            XAUAUD.FX : 黄金澳元
            USDMXN.FX : 美元墨西哥比索
            NZDJPY.FX : 新西兰元日元
        """
        
        urllist = self.root.find('urllist')
        code =None
        codedict={}
        if urllist is not None:
            klinecode = urllist.find('klinecode')
            if klinecode is not None:
                code = [inter.tag for inter in klinecode.getiterator()][1:]
                codedict = reduce(lambda x,y:dict(x,**y),[{inter.tag:inter.text} for inter in klinecode.getiterator()][1:])
        return (code,codedict)