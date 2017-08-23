# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 15:11:13 2016
web api
@author: lywen
1表示数据重复，2表示少字段，3表示json格式不对，4表示汇率找不到之类的
"""

import web
#import traceback
#web.config.debug = False
urls = ('/model', 'model')
from model.result import request 
import json

class model:
    def POST(self):
          try:
            data = web.data()
            return request(data)    
            
          except:
            #error=traceback.format_exc()
            error=u"数据不规范，请重新填写!"
            return json.dumps({"errCode":4, "errMsg":error,'data':None})
         
        
    def GET(self):
        return None

    
        

if __name__ == "__main__":
   app = web.application(urls, globals()) 
   app.run()
    #data = '''{"param":1,"data":["Z000"]}'''
    #print request(data) 
            
