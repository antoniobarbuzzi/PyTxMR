# -*- coding: utf-8 -*-

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hadoopfs import ThriftHadoopFileSystem
from hadoopfs.ttypes import *


class TextInputFormat(object):
    def __init__(self, uri, server_name, server_port):
        self.uri = uri
        self.server_name = server_name
        self.server_port = server_port
   
    
    def connect(self):
        try:
            self.transport = TSocket.TSocket(self.server_name, self.server_port)
            self.transport = TTransport.TBufferedTransport(self.transport)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

            # Create a client to use the protocol encoder
            self.client = ThriftHadoopFileSystem.Client(self.protocol)
            self.transport.open()

            # tell the HadoopThrift server to die after 60 minutes of inactivity
            self.client.setInactivityTimeoutPeriod(60*60)
            return True

        except Thrift.TException, tx:
            print "ERROR in connecting to ", self.server_name, ":", self.server_port
            print '%s' % (tx.message)
            return False
        
        
    
    def getSplits(self):
        self.path = Pathname();
        self.path.pathname = uri;
        self.filesize = self.client.stat(path).length
        blockLocations = self.client.getFileBlockLocations(self.path, 0, self.filesize)
        for item in blockLocations:
            self.printLocations(item)



if __name__ == '__main__':
    tif = TextInputFormat("/user/antonio/prova.cap", "localhost", "8020")
    tif.connect()
    
        