# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 15:31:22 2016
日志管理模块
@author: lywen
"""

from help.helper import getNow


class logs(object):
    """
    日志输出模块
    """
    def __init__(self,types,classname,funname,message,infoFalg=False):
        """
        format='[%s]:%Y-%m-%d %H:%M:%S %s %s %s'
        输出格式实例：
        ［error］:2016-07-12 17:11:38 logs error  报错明细
        """
        
        self.classname      = classname##类的名字
        self.funname        = funname## 函数名字
        self.message        = message##报错信息
        self.time           = getNow()
        self.types          = types ##日志类型
        self.errormessage   = None
        self.infomessage    = None
        self.warningmessage = None
        self.infoFalg       = infoFalg
        self.printf()
        
    def debug(self):
        """
        调试错误信息
        """
        pass
    
    def error(self):
        """
        程序报错信息
        """
        self.errormessage = '[%s]:%s %s %s %s %s\n'%('error',self.types,self.time,self.classname,self.funname,self.message)
    
    def warning(self):
        """
        警告信息
        """
        self.warningmessage = '[%s]:%s %s %s %s %s\n'%('warning',self.types,self.time,self.classname,self.funname,self.message)
    
    def info(self):
        """
        常规信息
        """
        self.infomessage = '[%s]:%s %s %s %s %s\n'%('info',self.types,self.time,self.classname,self.funname,self.message)
    
    def printf(self):
        """
        打印信息
        """
        if self.infoFalg:
            self.info()
            print self.infomessage
        else:
            self.error()
            print self.errormessage
    
