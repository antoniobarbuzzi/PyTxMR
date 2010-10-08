# -*- coding: utf-8 -*-
from twisted.spread.util import FilePager
from twisted.spread.flavors import Referenceable
from twisted.internet.defer import Deferred

# see http://code.activestate.com/recipes/457670/ to understand it


### Server Side
import pickle
import random
import netstring

class KeyValueStore(object):
    def __init__(self):
        self.__job_list = set()
        self.__numreducer_x_job = {}
        self.__container={}
        self.__debug_invalid_list = {} #TODO Remove debug_invalid_list (it does not scale), used only for checking correct behaviour of class users.
        
    def bookJob(self, jobid, numreducer):
        assert jobid not in self.__job_list
        self.__job_list.add(jobid)
        self.__numreducer_x_job[jobid]=numreducer
        self.__container[jobid]={} # per map
    
    def destroyJob(self, jobid):
        assert jobid in self.__job_list
        self.__job_list.remove(jobid)
        del self.__container[jobid]
        del self.__debug_invalid_list[jobid]
        
    def getPut(self, jobid, mapid): # in order to avoid two lookups for each put, each map uses a new defined put
        assert jobid in self.__job_list
        self.__container[jobid][mapid]=[[] for _ in range(self.__numreducer_x_job[jobid])]
        l = self.__container[jobid][mapid]
        assert(len(l)==self.__numreducer_x_job[jobid])
        
        def put(numreducer, key, value):
            assert(not self.__debug_invalid_list.has_key(jobid) or mapid not in self.__debug_invalid_list[jobid])
            #print '>>>>>>>>>>>>', numreducer, self.__numreducer_x_job[jobid]
            assert(numreducer < self.__numreducer_x_job[jobid])
            l[numreducer].append((key, value))
        return put
    
     
    def getSortedIterator(self, jobid, mapid, partition_number): # once you get an iterator, you cannot use put anymore
        assert jobid in self.__job_list
        self.__debug_invalid_list.setdefault(jobid, []).append(mapid)
        
        self.__container[jobid][mapid][partition_number].sort() # TODO Sort using just key, not values
        l = self.__container[jobid][mapid][partition_number]
        
        ## TODO  Add Combiner, using itertools.groupby
        #for k, g in itertools.groupby(iter(l)), lambda x:x[0]):
        #   pass
        
        for kv in l:
            yield kv



class ToNetstringFile:
    def __init__(self, it):
        self.it=it
        self._tmp = None
    
    def read(self, size):
        tmp=[]
        totlen = 0
        if self._tmp:
            a, self._tmp = self._tmp, None
            return a
        for i in self.it:
            s = netstring.encode(pickle.dumps(i, pickle.HIGHEST_PROTOCOL))
            l = len(s)
            if totlen==0 and l>size:
                raise Exception()
            elif l+totlen>size:
                self._tmp = s
                return ''.join(tmp)
            else:
                tmp.append(s)
                totlen+=l
        return ''.join(tmp)
     
    def close(self):# never called
        pass


class KVReferenceable(Referenceable):
    def __init__(self, kvstore):
        self.kvstore = kvstore
        
    def remote_getResult(self, collector, jobid, mapid, partition_number):
        kvfile = ToNetstringFile(self.kvstore.getSortedIterator(jobid, mapid, partition_number))
        pager = FilePager(collector, kvfile)
        #return pager.deferred #TODO: deferred useless, if filesender finish, you should still wait for the data transfer to the client



#### Client ###

class SimplePageCollector(Referenceable):
    def __init__(self, deferred):
        self.decoder = netstring.Decoder()
        self.totalnum = 0
        self.deferred = deferred

    def remote_gotPage(self, page):
        print "Deserializzo"
        decoder = self.decoder
        for value in decoder.feed(page):
            print ">", pickle.loads(value)
            self.totalnum+=1

    def remote_endedPaging(self):
        print 'Got all pages', self.totalnum
        self.deferred.callback(self.totalnum)

class ResultGetter:
    def __init__(self, dt):
        self.dataThingy = dt

    def getRemoteResult(self, jobid, mapid, partition_number):
        d = Deferred()
        collector = SimplePageCollector(d)
        d.addCallbacks(self.ok, self.nok)
        return self.dataThingy.callRemote("getResult", collector, jobid, mapid, partition_number)
    
    def ok(self, *args):
        print 'got all results ok'*10, args
        return None
        #print int1, int2, repr(shortString), len(hugeString)

    def nok(self, f):
        print 'data not ok', "==>" * 100, f
        return f




def __main__():
    JOBID='Job001'
    MAPID='Map001'
    NUMPARTITION=5
    PORT=8192
    import random
    if sys.argv[1] == 'server':
        from twisted.spread.flavors import Root
        from twisted.spread.pb import PBServerFactory 

        store = KeyValueStore()
        store.bookJob(JOBID, NUMPARTITION)
        put = store.getPut(JOBID, MAPID)

        def fillKV(n):
            for _ in xrange(n):
                k, v = random.randint(0, 100), random.randint(0, 5)
                red = random.randint(0, NUMPARTITION-1)
                put(red, k,v)
        print "Filling KV"
        fillKV(1000)
        print "Filled"
        class SimpleRoot(Root):
            def rootObject(self, broker):
                return KVReferenceable(store)
        
        reactor.listenTCP(PORT, PBServerFactory(SimpleRoot()))
    elif sys.argv[1] == 'client':
        from twisted.spread import pb
        def getIt(x):
            r = ResultGetter(x)
            return r.getRemoteResult(JOBID, MAPID, random.randint(0,NUMPARTITION-1) )
        cf = pb.PBClientFactory()
        reactor.connectTCP("localhost", PORT, cf)
        cf.getRootObject().addCallback(getIt)
    else:
        raise sys.exit("usage: %s (server|client)" % sys.argv[0])
    reactor.run()
    


if __name__ == '__main__':
    import sys
    from twisted.internet import reactor
    from twisted.python import log    
    log.startLogging(sys.stdout)

    __main__()

