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
        self.db = rocksdb.DB("lab2.db", rocksdb.Options(create_if_missing=True))
        self.dbLog = rocksdb.DB("lab2Log.db", rocksdb.Options(create_if_missing=True))
        it = db.iterkeys()

    def put(self, request, context):
        print("put")
        it.seek_to_last()
        latestSequenceNumber = int(list(it)[0].decode("utf-8"))

        key = uuid.uuid4().hex
        key = key.encode("utf-8")
        testString = request.data
        testString = testString.encode("utf-8")
        logKey = str(latestSequenceNumber+1).encode("utf-8")
        logValue = str(["put", request.data]).encode("utf-8")

        self.db.put(key, testString)
        self.dbLog.put(logKey, logValue)
        print(request)
        print(int(list(it)[0].decode("utf-8")))

        return datastore_pb2.Response(data=key)

    def get(self, request, context):
        print("get")
        print(request)
        value =  self.db.get(request.data.encode("utf-8"))

        return datastore_pb2.Request(data=value)

    def delete(self, request, context):
        print("delete")
        self.db.delete(request.data)

        return datastore_pb2.Response(data=value)

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
