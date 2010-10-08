# -*- coding: utf-8 -*-
from twisted.spread import pb
from twisted.internet import reactor
from twisted.internet.defer import DeferredList, inlineCallbacks
import time
import Queue as queue
import itertools

PORT = 8992
RESTART_DELAY=10
from SimpleReader import SimpleReader

class Job(object):
    CREATED = 0
    RESOURCE_RESERVER = 1
    RUNNING = 2
    def __init__(self, job_id, timestamp, mapfilename, mapclass, numreducer, reader, reducerfilename, reducerclass):
        self.job_id = job_id
        self.timestamp = timestamp
        self.mapfilename = mapfilename
        self.mapcalss = mapclass
        self.numreducer = numreducer
        self.reader = reader
        self.reducerfilename = reducerfilename
        self.reducerclass = reducerclass
        # dynamic
        self.status = Job.CREATED
        
    def getMapNum(self):
        pass
    def getReduceNum(self):
        pass
    
    def getMap(self):
        pass
    
    def getReduce(self):
        pass
        
class JobManager(object):
    def __init__(self):
        self.job_id = 0
        self.job_list = queue.Queue()
    
    def getJobID(self, mapfilename, mapclass, numreducer, reader, reducerfilename, reducerclass):
        self.job_id+=1
        job = Job(job_id, timestamp, mapfilename, mapclass, numreducer, reader, reducerfilename, reducerclass)
        self.job_list.put(job)

class JobTracker(pb.Root):
    def __init__(self):
        print "JobTracker started"
        self.workers=[]
        self.worker_id=0
        self.jobs = []
        self.job_id = 0
    
    def finish(self, _):
        print "Stopping reactor"
        reactor.stop()
        print "Exit successfully"
     
    def remote_register(self, worker):
        self.worker_id += 1
        worker.wid = self.worker_id
        self.workers.append(worker)
        worker.notifyOnDisconnect(self.worker_disconnected)
        ip4 = worker.broker.transport.getPeer()
        print "Worker wid=%s registered - (%s:%d)" %(worker.wid, ip4.host, ip4.port)
        return worker.wid
    
    def worker_disconnected(self, worker, *args):
        assert(worker in self.workers)
        print "Worker #%s disconnected:" % worker.wid
        self.workers.remove(worker)

    def success(self, result, *methodArgs, **methodKeywords):
        host, port = methodArgs[0], methodArgs[1]
        print "success from host %s:%s ==> %s" %(host, port, result)

    def failure(self, *args):
        print "Failure", args
            
    def getNewJobID(self, **kwargs):
        self.job_id+=1
        #self.job_list.append((self.job_id, kwargs))
        return self.job_id

    
    @inlineCallbacks
    def stepOneV2(self):
        #try:
            print "TestMapReduce"
            jobid = "Job%03d" % self.getNewJobID() #FIXME ke cagata di metodo
            print "Booking resource for JOB '%s' in %d workers" % (jobid, len(self.workers))
            deferreds = []
            for worker in self.workers:
                d = worker.callRemote('initJob', jobid, len(self.workers))
                deferreds.append(d)
            
            dl = DeferredList(deferreds, consumeErrors=True)#.addCallbacks(self.mapDone, self.failure, errbackArgs=("fallito"), callbackArgs=(jobid))
            results = yield dl
            print "Resource booked - Running Mappers"
            #self.jobFailure = self.failure
            deferreds = []
            mapid_template='Map%03d'
            nummap = 0
            for worker in self.workers:
                d = worker.callRemote('executeMap', jobid, mapid_template % nummap)
                deferreds.append(d)
                nummap+=1
                # FIXME Add callback 4 shuffle
                
                d = worker.callRemote('executeMap', jobid, mapid_template % nummap)
                deferreds.append(d)
                nummap+=1
                        
            dl = DeferredList(deferreds, consumeErrors=True)#.addCallbacks(self.mapDone, self.failure, errbackArgs=("fallito"), callbackArgs=(jobid))
            results = yield dl
            
            print "Maps finished", jobid
            for content in results:
                print "CONTENT:", content
            
            print "Shuffling DATA"
            numreducer = 0
            nummap = 0
            deferreds = []
            for reducer, mapper in itertools.product(self.workers, self.workers):
                mapper_ip = mapper.broker.transport.getPeer().host
                mapper_port = 8992
                d = worker.callRemote('shuffle', jobid, mapid_template % numreducer, numreducer, mapper_ip, mapper_port) #jobid, mapid, partition_number, remoteHost, remotePort
                deferreds.append(d)
                numreducer+=1
                nummap+=1
            
            dl = DeferredList(deferreds, consumeErrors=True)#.addCallbacks(self.mapDone, self.failure, errbackArgs=("fallito"), callbackArgs=(jobid))
            results = yield dl
            
            print "Starting Reducer"
            
            
        #except:
        #     print "ERRORE"

    
    #def stepOne(self):
        #print "TestMapReduce"
        #jobid = self.getNewJobID()
        #print "Creating JOB", jobid
        #self.jobFailure = self.failure
        #deferreds = []
        #for worker in self.workers:
            #d = worker.callRemote('executeMap', jobid)
            #deferreds.append(d)

            #d = worker.callRemote('executeMap', jobid)
            #deferreds.append(d)
                    
        #dl = DeferredList(deferreds, consumeErrors=True)#.addCallbacks(self.mapDone, self.failure, errbackArgs=("fallito"), callbackArgs=(jobid))
        #dl.addCallback(self.mapsFinished, jobid) #FIXME errorback
        #dl.addErrback(self.failure)
        
    def mapsFinished(self, results, jobid): #worker, jobid, *args
        print "Maps for job %s finished" % jobid
        for isSuccess, content in results:
            print "Successful? %s" % isSuccess
            print type(content), content


    def done(self, *args, **kwargs):
        print "DONE", args, kwargs
            
            

import sys
from twisted.python import log
log.startLogging(sys.stdout)

jt = JobTracker()
reactor.listenTCP(9000, pb.PBServerFactory(jt))
reactor.callLater(10, jt.stepOneV2)
reactor.run()



