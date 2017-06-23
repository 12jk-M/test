# -*- coding: utf-8 -*-
"""
Created on Wed Mar 01 13:44:21 2017

@author: candy
"""
import logging
from config.xmlpath import XML
import threading
from main.thread import threadKlineExcute
import time



class runJob(object):
    
    def __init__(self):
        self.rlock =threading.RLock()
        self.job=[]
        self.xml =XML()
        self.klinepair = self.xml.getkline()[1]
        
    
    def addKlineFx(self,threadnum):
        kl =threadKlineExcute(klinepair=self.klinepair,rlock=self.rlock)
        self.job.extend (kl.run(threadnum))  
        for j in self.job:
            j.start()

#    def start(self):
#        for j in self.job:
#            if j.rlock.acquire()==True:
#                j.start()
#                j.rlock.release()

def  run(arg):
    """arg ={'addKlineFx':20}"""
    runjob=runJob()
    if runjob.xml.getrunFlag()=='1':

        for item in arg.iteritems():
            if item[0]=='addKlineFx':
                runjob.addKlineFx(item[1])
        

        


#**test**#
#t=runJob()
#Set =set()
#runjob =runJob()
#runjob.addKlineFx(4)
#runjob.start()
#****#




if __name__ =='__main__':
    arg ={'addKlineFx':10}    
    while True:
        print 'start'
        time.sleep(40)
        run(arg)# 这只是启动线程，而不算其跑了多久

        


