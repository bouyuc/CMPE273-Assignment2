'''
################################## client.py #############################
# 
################################## client.py #############################
'''
import grpc
import datastore_pb2
import argparse
import readline

PORT = 3000

class DatastoreClient():
    
    def __init__(self, host='0.0.0.0', port=PORT):
        self.channel = grpc.insecure_channel('%s:%d' % (host, port))
        self.stub = datastore_pb2.DatastoreStub(self.channel)

    def put(self, value):
        return self.stub.put(datastore_pb2.Request(data=value))

    def get(self, key):
        return self.stub.get(datastore_pb2.Request(data=key))

    def delete(self, key):
        return self.stub.delete(datastore_pb2.Request(data=key))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("host", help="display a square of a given number")
    args = parser.parse_args()
    print("Client is connecting to Server at {}:{}...".format(args.host, PORT))
    client = DatastoreClient(host=args.host)
    
    try:
        while True:
            text = input("Insert: ")
            print("Inserting: " + text)
            resp = client.put(text) #resp is server response
            key = resp.data
            print("## PUT Response: key = " + key)
    except KeyboardInterrupt:
            print("exiting")

    


    ##print("## GET Request: key = " + key) 
    ##resp = client.get(key)
    #print("## GET Response: value = " + resp.data) 
    #resp = client.delete("4")
    #print(resp.data + "Deleted")


if __name__ == "__main__":
    main()

