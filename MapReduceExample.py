# -*- coding: utf-8 -*-
import string
from interfaces import Mapper_Template, Reducer_Template

class MapExample(Mapper_Template):
    def __init__(self):
        print "Mapper Constructor Called"

    def __call__(self, key,value):
        value = map(lambda x:x in string.letters and x or ' ', value)
        value = ''.join(value)
        for word in value.split():
            yield word.lower(), 1
            #for i in xrange(10):
                #yield "%s%d" % (word.lower(), i),1
            

class ReduceExample(Reducer_Template):
    def __call__(self, key,values):
        yield key,sum(values)




class Init:
    def __init__(self):
        self.filename = "machiavelli.txt"
        self.mapper = MapExample
        self.reducer = ReduceExample
        self.num_reducer = 0;
