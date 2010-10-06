# -*- coding: utf-8 -*-
import spilled

import random
from zope.interface import implements
from twisted.internet import reactor, interfaces
from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver 


class PartitionSpiller:
    def __init__(self, id, numpartitions, maxlen=1000000):
        self.partmaxlen=int(maxlen/numpartitions)
        self.partitionsplits=[spilled.SpilldKVList(fileheader="./cache/map%s-partition%s"% (id, partnumber), maxlen=self.partmaxlen) for partnumber in xrange(numpartitions)]
    
    def put(self, partition, key, value):
        self.partitionsplits[partition].put(key,value)
    
    def sorted_iterator(self, partition):
        return self.partitionsplits[partition].sorted_iterator()
    
    def flush(self):
        partitionsplits = self.partitionsplits
        for spilledlist in partitionsplits:
            spilledlist.flush()


class KeyValueSender:
    implements(interfaces.IPushProducer)
    def __init__(self, proto, partition_spiller, partition_number):
        self._proto = proto
        self._paused = False
        self._partition_spiller = partition_spiller
        self._partition_number = partition_number
        
        self.it = partition_spiller.sorted_iterator(partition_number)
        
    def pauseProducing(self):
        self._paused = True
        print('pausing connection from %s' % (self._proto.transport.getPeer()))
    
    def resumeProducing(self):
        self._paused = False
        it = self.it
        for kv in it:
            a = pickle.dumps(elt_to_pickle)
            self._proto.transport.write(a)
            self._proto.transport.write('\n\n')
            if self._paused:
                print('PAUSE')
                break

    def stopProducing(self):
        pass


class KeyValueServer(LineReceiver):
    def connectionMade(self):
        print('connection made from %s' % (self.transport.getPeer()))
        self.transport.write('JobID:\r\n')

    def lineReceived(self, line):
        self.producer = KeyValueProducer(self)
        self.transport.registerProducer(self.producer, True)
        self.setRawMode()
        self.producer.resumeProducing()

    def connectionLost(self, reason):
        print('connection lost from %s' % (self.transport.getPeer()))


def listen(port):
    factory = Factory()
    factory.protocol = KeyValueServer
    reactor.listenTCP(port, factory)
    print('listening on 1234...')
    reactor.run() 