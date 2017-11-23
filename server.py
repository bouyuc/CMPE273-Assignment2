'''
################################## server.py #############################
# Lab1 gRPC RocksDB Server
################################## server.py #############################
'''
#getUpdateSince
#

import time
import grpc
import datastore_pb2
import datastore_pb2_grpc
import uuid
import rocksdb
import ast
import threading

from concurrent import futures

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
newInput = False
slaveReplicationThreadPort = 1337
opts = rocksdb.Options()
opts.create_if_missing = True
opts.table_factory = rocksdb.BlockBasedTableFactory(
    index_type="binary_search",
    filter_policy=rocksdb.BloomFilterPolicy(0),
    block_cache=rocksdb.LRUCache(2*(1024**3)),
    block_cache_compressed=rocksdb.LRUCache(500*(1024**2)))
db = rocksdb.DB("lab2.db", rocksdb.Options(create_if_missing=True))
dbLog = rocksdb.DB("lab2Log.db", opts)


class MyDatastoreServicer(datastore_pb2.DatastoreServicer):
    def __init__(self):
        print("starting server")
        self.db = db
        self.dbLog = dbLog

    def encode(input):
        return input.encode("utf-8")

    def replicator(someFunction):
        def wrapper(self, request, context):
            global newInput
            newInput = True
            logKey = str(time.time()).encode("utf-8")
            logValue1 = str(someFunction.__name__)
            logValue2 = str(request.data)
            logValue = (logValue1 + "," + logValue2).encode("utf-8")

            dbLog.put(logKey, logValue)

            return someFunction(self, request, context)

        return wrapper

    def get(self, request, context): #Used to serve slave requests
        global newInput
        if (newInput == False):
            value = ""
        else:
            it = self.dbLog.iteritems()
            itV = self.dbLog.itervalues()
            value=""
            print("Get")
            print(request.data)
            if(request.data == "pa55w0rd"):
                it.seek_to_first()
            else:
                it.seek(request.data.encode("utf-8"))
                it.__next__()
            for i in list(it):
                value = value + i[0].decode("utf-8") + "!" + i[1].decode("utf-8") + "!"
            print(value)

            newInput = False
        return datastore_pb2.Request(data=value)

    @replicator
    def put(self, request, context):
        print("put")
        it = self.dbLog.iterkeys()
        it.seek_to_last()
        key = uuid.uuid4().hex
        key = key.encode("utf-8")
        testString = request.data
        testString = testString.encode("utf-8")
        logKey = str(time.time()).encode("utf-8")
        logValue = str(["put", request.data]).encode("utf-8")
        self.db.put(key, testString)
        print(logKey)
        print(request)
        print(newInput)

        return datastore_pb2.Response(data=key)

    @replicator
    def delete(self, request, context):
        # logKey = str(time.time()).encode("utf-8")
        # logValue = str(["delete", request.data]).encode("utf-8")
        # self.dbLog.put(logKey, logValue)
        print("deleting " + str(request.data))
        self.db.delete(request.data.encode("utf-8"))

        return datastore_pb2.Response(data=request.data)

class slaveReplicationThread(threading.Thread):
    def __init__(self, IP, Port):
        threading.Thread.__init__(self)
        self.IP = IP
        self.Port = Port

    def run(IP):
        '''
        Run the GRPC server
        '''
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        datastore_pb2_grpc.add_DatastoreServicer_to_server(MyDatastoreServicer(), server)
        server.add_insecure_port('%s:%d' % ("localhost", slaveReplicationThreadPort))
        server.start()
        try:
            while True:
                print("Server started at...%d" % slaveReplicationThreadPort)
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)

def run(host, port):
    '''
    Run the GRPC server
    '''
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    datastore_pb2_grpc.add_DatastoreServicer_to_server(MyDatastoreServicer(), server)
    server.add_insecure_port('%s:%d' % (host, port))
    server.start()

    try:
        while True:
            print("Server started at...%d" % port)
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    slaveThread = slaveReplicationThread("localhost", slaveReplicationThreadPort)
    slaveThread.start()
    run('0.0.0.0', 3000)
