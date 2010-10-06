# -*- coding: utf-8 -*-
#import spilled

from zope.interface import implements
from twisted.internet import reactor, interfaces
from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import ServerFactory
try:
    import cPickle as pickle
except ImportError:
    import pickle

class KeyValueStore:
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
        self.__debug_invalid_list.setdefault(jobid, []).append(mapid)
        
        self.__container[jobid][mapid][partition_number].sort() # TODO Sort using just key, not values
        l = self.__container[jobid][mapid][partition_number]
        
        ## TODO  Add Combiner, using itertools.groupby
        #for k, g in itertools.groupby(iter(l)), lambda x:x[0]):
        #   pass
        
        for kv in l:
            yield kv
 

class KeyValueSender: ## per (jobid, partition_number)
    implements(interfaces.IPushProducer)
    def __init__(self, proto, iterator):
        self.__proto = proto
        self.__paused = False
        self.__it = iterator

        
    def begin(self):
        self.resumeProducing()
        
    def pauseProducing(self):
        self.__paused = True
        print('pausing connection from %s' % (self.__proto.transport.getPeer()))
    
    def resumeProducing(self):
        self.__paused = False
        it = self.__it
        for kv in it:
            a = pickle.dumps(kv)
            self.__proto.transport.write(a)
            self.__proto.transport.write('\n\n')
            if self.__paused:
                print('PAUSE')
                break

        if not self.__paused:
            self.__proto.transport.unregisterProducer()
            self.__proto.transport.loseConnection()

    def stopProducing(self):
        print("STOP PRODUCING") #FIXME MMM; NN SONO sicuro che vada fatto così, controlla sintassi
        self.__proto.transport.loseConnection()
        pass


class KeyValueProtocol(LineReceiver):
    def connectionMade(self):
        print('connection made from %s' % (self.transport.getPeer()))
        self.transport.write('JobID?\r\n')

    def lineReceived(self, line):
        jobid, mapid, partition_number = line.split('@')
        partition_number = int(partition_number)
        
        kvstore = self.factory.kvstore
        iterator = kvstore.getSortedIterator(jobid, mapid, partition_number)
        
        self.producer = KeyValueSender(self, iterator)
        self.transport.registerProducer(self.producer, True)
        self.setRawMode()
        self.producer.begin()
        #self.producer.resumeProducing()

    def connectionLost(self, reason):
        print('connection lost from %s [%s]' % (self.transport.getPeer(), reason.getErrorMessage()))


class KeyValueFactory(ServerFactory):
    protocol = KeyValueProtocol
    def __init__(self, kvstore):
        self.kvstore = kvstore

def createKVServer(port):
    kvstore = KeyValueStore()
    factory = KeyValueFactory(kvstore)
    reactor.listenTCP(port, factory)
    print('KeyValueServerlistening on port %d...' % port)
    #reactor.run() 
    return kvstore
    
    

def __main__():
    import random
    JOBID='Job_01'
    MAPID='MAP_001'
    NUMPARTITION=5
    PORT=4096
    
    store = createKVServer(PORT)
    store.bookJob(JOBID, NUMPARTITION)
    put = store.getPut(JOBID, MAPID)
    
    for _ in xrange(100):
        k, v = random.randint(0, 100), random.randint(0, 5)
        red = random.randint(0, NUMPARTITION-1)
        put(red, k,v)
    for partition in xrange(NUMPARTITION):
        print "*"*5, "PARTITION %d" % partition, "*"*5
        for k, v in store.getSortedIterator(JOBID, MAPID, partition):
            print '\t', k,v


if __name__ == '__main__':
    __main__()
    reactor.run()
    