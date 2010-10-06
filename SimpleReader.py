# -*- coding: utf-8 -*-
from interfaces import Reader

class SimpleReader(Reader):
    def __init__(self, filename):
        self.filename = filename
        
    def getKeyValue(self):
        f = open(self.filename)
        i=0
        for line in f:
            yield i, line
            i+=1
        f.close()
