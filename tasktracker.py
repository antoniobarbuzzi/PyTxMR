# -*- coding: utf-8 -*-
"Server.py: Provides a calculation service across the network"
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import threads #change to work with ampoule

#from partitionspiller import PartitionSpiller

import time
import random
PORT = 8992
from SimpleReader import SimpleReader
import newpartitionspiller
import time

import kvstuff

class Worker(pb.Root):
    def __init__(self, id):
        self.id = id
        self.wid = None
        self.job_data={}
        self.__hdr = "%s >>> " %self.id
        print self.__hdr, "Init"
        #self.node = node.Execute()
        #self.spiller={}
        #self.map_store = newpartitionspiller.createKVServer(PORT+1) #FIXME add something to close Server with program finish
        self.map_store = kvstuff.MapKVstore()
        self.red_store = kvstuff.RedKVstore()
        #self.reduce_store = reducestorer()
        
        
        #store.bookJob(JOBID, NUMPARTITION)
        #put = store.getPut(JOBID, MAPID)
        
    #### INITIAL PHASE ###
    
    def connectToJT(self, host, port, num=0):
        print self.__hdr, 'Connecting to JT ...'
        factory = pb.PBClientFactory()
        reactor.connectTCP(host, port, factory)
        factory.getRootObject().addCallbacks(self.registerToJT, self.retryConnection, errbackArgs=(host, port, num))

    #TODO replace using ReconnectingPBClientFactory or something similar
    def retryConnection(self, reason, host, port, num):
        num=(num+1)%10
        t = min(60, 2**num)
        print self.__hdr, "Connection to JT failed [%s]" % reason.getErrorMessage()
        print self.__hdr, "Retrying in %d s..." % t
        reactor.callLater(t, self.connectToJT, host, port, num)
    
    def registerToJT(self, remote, *args, **kw):
        print self.__hdr, "CONNECTED"
        print self.__hdr, "Registering to JT"
        remote.callRemote('register', self).addCallbacks(self.registeredToJT, self.failure)
    
    def registeredToJT(self, wid, *args, **kw):
        self.wid = wid
        print self.__hdr, "Successfully registered to JT with wid = %s" % wid
        self.__hdr = "%s#%s] >>> " %(self.id, wid)

    def failure(self, reason, *args, **kw):
        print self.__hdr, "FAILURE", reason
        
    ## JOBS ##
    def remote_initJob(self, jobid, numreducer):
        assert(jobid not in self.job_data)
        self.job_data[jobid]=numreducer
        self.map_store.bookJob(jobid, numreducer)#FIXME????
        self.red_store.bookJob(jobid)#FIXME????
    
    def remote_destroyJob(self, jobid):
        assert(jobid in self.job_data)
        del self.job_data[jobid]
        self.map_store.destroyJob(jobid)
    
    # Shuffle

        #redkv = RedKVstore()
        #redkv.bookJob(JOBID)
        #put = redkv.getPut(JOBID, 0)

        #def getIt(x):
            #r = ResultGetter(x)
            #return r.getRemoteResult(JOBID, MAPID, random.randint(0,NUMPARTITION-1) , put)
        #cf = pb.PBClientFactory()
        #reactor.connectTCP("localhost", PORT, cf)
        #def printKV(BHO, redkv):#FIXME: BHO
            #print "=~<>~"*10
            #print BHO
            #for k, it in redkv.getIterator(JOBID, 0):
                #print k
                #for kv in it:
                    #print "\t", kv
            #print "=~<>~"*10
        #cf.getRootObject().addCallback(getIt).addCallback(printKV, redkv)

    #Shuffle Client
    def remote_shuffle(self, jobid, mapid, numpartition, host, port):
        ''' JT asks a reducer to contact a mapper and to start copying data from the mapper
        '''
        #TEST Remove printKV
        def printKV(BHO, redkv, jobid, reduceid):
            import array
            print "=~<>~"*10
            print BHO
            i=0
            for k, it in redkv.getIterator(jobid, reduceid):
                num=0
                for kv in it:
                    num+=1
                print k,num
                
                if i==10:
                    break
                i+=1
                
                #for kv in it:
                    #print "\t", kv
            print "=~<>~"*10
        
        print self.__hdr, "RPC: shuffling data from %s:%d (%s@%s@%d)" % (host, port, jobid, mapid, numpartition)
        d = self.connectToWorker(host, port)
        putter = self.red_store.getPut(jobid, 0)#FIXME ReducerID
        print "FIXME"*10, "ReduceID"
        d.addCallback(self.getKVShuffler).addCallback(self.getKVs, jobid, mapid, numpartition, putter).addCallback(printKV, self.red_store, jobid, 0)#FIXME ReducerID
        return d

    def connectToWorker(self, host, port): #TODO: merge connectToJT e connectToWorker
        print self.__hdr, 'Connecting to Worker (%s:%s) ...' %(host, port)
        factory = pb.PBClientFactory()
        reactor.connectTCP(host, port, factory)
        return factory.getRootObject()
    
    def getKVs(self, remote, jobid, mapid, numpartition, putter):
        #print "REMOTE=%s - type = %s" %(remote, type(remote))
        #print dir(remote)
        peer = remote.broker.transport.getPeer()
        print self.__hdr, "CONNECTED to %s:%d" % (peer.host, peer.port)
        r = kvstuff.ResultGetter(remote) #FIXME: ResultGetter maybe is useless, or not?
        return r.getRemoteResult(jobid, mapid, numpartition, putter)
        
    def getKVShuffler(self, remote):
        return remote.callRemote("getKVShuffler")
        #.addCallbacks(self.registeredToJT, self.failure)

    # Shuffler Server Side
    
    def remote_getKVShuffler(self):
        '''
        '''
        return kvstuff.KVReferenceable(self.map_store)
    
        
    
    ### EXECUTOR ###
    def remote_executeMap(self, jobid, mapid):
        #TODO add a queue somewhere (rdq)
        assert(jobid in self.job_data)
        print self.__hdr, "RPC: execute MAP (%s:%s)" %(jobid, mapid)
        numreducer = self.job_data[jobid]
        d = self.add_map(jobid, mapid, 'MapReduceExample', 'MapExample', numreducer, SimpleReader("./warandpeace.txt"))
        return d
    
    def remote_executeReduce(self, jobid, reduceid, partition_number):
        assert(jobid in self.job_data)
        print self.__hdr, "RPC: execute REDUCE (%s@%s@%d)" % (jobid, reduceid, partition_number)
        #d = self.add_map(jobid, mapid, 'MapReduceExample', 'MapExample', numreducer, SimpleReader("./warandpeace.txt"))
        pass
    
    def execute_map(self, mapid, mapfunction, numreducer, put_function, reader):
        #print "*"*10, "<MAP>" + "*" * 10
        #time.sleep(10)
        #print "*"*10, "</MAP>" + "*" * 10
        #return "return_execute_map"
        #put = store.getPut(JOBID, MAPID)
        print "*"*10, "<MAP id=%s>" % mapid,  "*" * 10
        assert(callable(mapfunction))
        for in_k, in_v in reader.getKeyValue():
            for out_k,out_v in mapfunction(in_k, in_v):
                #self.spiller.put( (hash(out_k)%numreducer), out_k, out_v)
                put_function((hash(out_k)%numreducer), out_k, out_v)
        print "*"*10, "</MAP id=%s>" % mapid,  "*" * 10
    
    def execute_reduce(self, lista):
        for in_k, in_v in lista.iteritems():
            for out_k, out_v in reduce_funz(in_k, in_v):
                #store results
                print out_k, out_v
                
    def add_map(self, job_id, mapid, mapfilename, mapclass, numreducer, reader):
        assert(job_id in self.job_data)
        mapfilename = mapfilename.strip('.py')
        mapper = __import__(mapfilename, None, None, [''])
        mapfunz = mapper.__dict__[mapclass]()
        
        #readerfilename = readerfilename.strip('.py')
        #reader = __import__(readerclass, None, None, [''])
        #reader = reader.__dict__['mapclass']
        put_function = self.map_store.getPut(job_id, mapid)
        map_args = (mapid, mapfunz, numreducer, put_function, reader)

        print "Launch thread to execute map:", map_args
        d = threads.deferToThread(self.execute_map, *map_args)
        ###TODO Add callback to flush self.map_store for map
        return d
        
        
        #if False:
            #self.spiller[job_id].flush()
            #for reducer in xrange(numreducer):
                #print "="*10, "reducer %d" % reducer, "="*10
                #for k,v in self.spiller[job_id].sorted_iterator(reducer):
                    #print k,v
                    
        #if False:
            #import itertools
            #for reducer in xrange(numreducer):
                #print "="*10, "reducer %d" % reducer, "="*10
                #for k, g in itertools.groupby(self.spiller[job_id].sorted_iterator(reducer), lambda x:x[0]):
                    #print "%s  ==> " %k,
                    #for ignore, v in g:
                        #print v,
                    #print

    

if __name__ == '__main__':
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)
    w1 = Worker('Worker1')
    #w2 = Worker('fake-id2')
    w1.connectToJT('localhost', 9000)
    #w2.connectToJT('localhost', 9000)
    print "LISTENING"
    reactor.listenTCP(PORT, pb.PBServerFactory(w1))
    #reactor.listenTCP(PORT+1, pb.PBServerFactory(w2))

    reactor.run()

#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#
#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#~-~#

#"Client.py: Uses the calculation service across the network"
