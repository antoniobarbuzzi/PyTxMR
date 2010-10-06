# -*- coding: utf-8 -*-
import bisect
import heapq
import spickle
import itertools
#from blist import sortedlist


class SpilldKVList():
    def __init__(self, fileheader="./pickled", maxlen=10000000):
        self.container = []
        self.diskdumps = []
        self.index=0
        self.maxlen=maxlen
        self.fileheader = fileheader
        
    def put(self, key, value):
        self.container.append((key,value))
        #bisect.insort(self.container, (key, value)) # funziona, ma è meglio ordinare anziché tenere lista ordinata :D
        #self.container.add((key, value)) # for sorted list
#        heapq.heappush(self.container, (key,value)) # not works, at least you should modify something
        if(len(self.container)>self.maxlen):
            self.flush()

    def flush(self):
        filename = '%s-%06d'%(self.fileheader, self.index)
        self.index+=1
        self.diskdumps.append(filename)
        self.container.sort()
        with open(filename, 'w') as f:
            spickle.s_dump(self.container, f)
        del(self.container)
        self.container=[]
    
    def sorted_iterator(self):
        self.container.sort()
        fileslist = [open(filename) for filename in self.diskdumps]
        sequences = [spickle.s_load(file) for file in fileslist]
        sequences.append(iter(self.container))
        return heapq.merge(*sequences)
        map(lambda x:x.close(), fileslist)


if __name__=='__main__':
    import random
    ul =SpilldKVList()
    
    for i in xrange(1000000):
        k,v = (random.randint(1,10), random.randint(1,1000))
        ul.put(k,v)
    #ul.flush()
    
    for i in ul.sorted_iterator():
        pass
        
    #for k, g in itertools.groupby(ul.sorted_iterator(), lambda x:x[0]):
        #print "%s]  --> " %k,
        #for ignore, v in g:
            #print v,
        #print
