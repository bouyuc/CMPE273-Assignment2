import grpc
import datastore_pb2
import argparse
import readline
import rocksdb
import time

PORT = 3000
pullInterval = 10
opts = rocksdb.Options()
opts.create_if_missing = True
opts.table_factory = rocksdb.BlockBasedTableFactory(
    index_type="binary_search",
    filter_policy=rocksdb.BloomFilterPolicy(0),
    block_cache=rocksdb.LRUCache(2*(1024**3)),
    block_cache_compressed=rocksdb.LRUCache(500*(1024**2)))
db = rocksdb.DB("lab2Slave.db", rocksdb.Options(create_if_missing=True))
dbLog = rocksdb.DB("lab2LogSlave.db", opts)


class DatastoreClient():

    def __init__(self, host='0.0.0.0', port=PORT):
        self.channel = grpc.insecure_channel('%s:%d' % (host, port))
        self.stub = datastore_pb2.DatastoreStub(self.channel)
        self.db = db
        self.dbLog = dbLog

    def get(self):
        itLog = dbLog.iteritems()
        itLog.seek_to_last()
        lastItem = list(itLog)
        # print(lastItem[0][0].decode("utf-8"))
        if (len(lastItem)!= 0):
            print("last item = " + str(lastItem))
            key = lastItem[0][0].decode("utf-8")
        else:
            print("empty")
            key = "pa55w0rd"

        return self.stub.get(datastore_pb2.Request(data=key))

    def updateDB(input):
        print ("test")

    def put(self, key, value):
        print("woahdoialhwdlajwgdoailfhaf")
        print(key)
        self.db.put(key.encode("utf-8"), value.encode("utf-8"))
        logKey = key.encode("utf-8")
        logValue = []
        logValue.append("put")
        logValue.append(value)
        logValue = str(logValue).encode("utf-8")
        self.dbLog.put(logKey, logValue)
        print(self.dbLog.put(logKey, logValue))
        print("Finished put!")
    def delete(self, key):
        it = db.iteritems()
        it.seek_to_first()
        print(list(it))
        it.seek_to_first()
        print("Deleting: " + str(key))
        print("vvvvvvvvvvvvvvv")
        print(self.db.get(key.encode("utf-8")))
        print("^^^^^^^^^^^^^^^^")
        self.db.delete(key.encode("utf-8"))
        print("end delete")
        # return self.stub.delete(datastore_pb2.Request(data=key))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host", help="display a square of a given number")
    args = parser.parse_args()
    print("Client is connecting to Server at {}:{}...".format(args.host, PORT))
    client = DatastoreClient(host=args.host)
    temp =""

    try:
        while True:
            update = client.get()
            print(update.data.split("!"))
            temp = update.data.split("!")
            for i in range(1, len(temp), 2):
                print(temp[i].split(","))
                if(temp[i].split(",")[0]=="put"):
                    client.put(temp[i-1], temp[i].split(",")[1])
                    print("key: " + str(temp[i-1]))
                    print("value: " + str(temp[i].split(",")[1]))
                if(temp[i].split(",")[0]=="delete"):
                    client.delete(temp[i-1])
                    # print(temp[i-1])
            time.sleep(pullInterval)
    except KeyboardInterrupt:
            print("exiting")

# [(b'1511289937.1061482', b'put,abc'), (b'1511289940.15034', b'delete,def'),
# (b'1511289948.9530685', b'put,abbbbb'), (b'1511289954.7693572', b'delete,abbbbb'),
# (b'abc', b'123'), (b'put2', b'456'), (b'put3', b'789'), (b'put4', b'4124'),
# (b'put5', b'1233123'), (b'put6', b'7812341249'), (b'put7', b'123123')]

if __name__ == "__main__":
    main()
