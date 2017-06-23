# -*- coding: utf-8 -*-
"""
Created on Sat Sep 24 09:16:27 2016

@author: PRui
"""

with open('txt1.txt') as f1,open('out1.txt') as f2:
    for line in f1:
        if 'IP' in line:
            print '****'
            f2.write(line)