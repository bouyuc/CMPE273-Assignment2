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

from concurrent import futures

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class MyDatastoreServicer(datastore_pb2.DatastoreServicer):
    def __init__(self):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.table_factory = rocksdb.BlockBasedTableFactory(
            index_type="binary_search",
            filter_policy=rocksdb.BloomFilterPolicy(0),
            block_cache=rocksdb.LRUCache(2*(1024**3)),
            block_cache_compressed=rocksdb.LRUCache(500*(1024**2)))

        self.db = rocksdb.DB("lab2.db", rocksdb.Options(create_if_missing=True))
        self.dbLog = rocksdb.DB("lab2Log.db", opts)
        self.dbLog.put("0".encode("utf-8"), str(['init','init']).encode("utf-8"))

    def put(self, request, context):
        print("put")
        it = self.dbLog.iterkeys()
        it.seek_to_last()
        latestSequenceNumber = int(list(it)[0].decode("utf-8"))
        print(latestSequenceNumber)

        key = uuid.uuid4().hex
        key = key.encode("utf-8")
        testString = request.data
        testString = testString.encode("utf-8")
        #logKey = str(latestSequenceNumber+1).encode("utf-8")
        logKey = str(time.time()).encode("utf-8")
        logValue = str(["put", request.data]).encode("utf-8")
        latestSequenceNumber = int(latestSequenceNumber)+1

        self.db.put(key, testString)
        self.dbLog.put(logKey, logValue)
        print(logKey)
        print(request)
        #print(int(list(it)[0].decode("utf-8")))

        return datastore_pb2.Response(data=key)

    def get(self, request, context):
        it = self.dbLog.itervalues()
        it.seek_to_first()
        for i in list(it):
            print(i.decode("utf-8"))
        print("get")
        print(request)
        value =  self.db.get(request.data.encode("utf-8"))

        return datastore_pb2.Request(data=value)

    def delete(self, request, context):
        print("deleting " + str(request.data))
        self.dbLog.delete(request.data.encode("utf-8"))

        return datastore_pb2.Response(data=request.data)

def run(host, port):
    '''
    Run the GRPC server
    '''
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
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
    run('0.0.0.0', 3000)
