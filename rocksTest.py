import rocksdb
import time
import ast

db = rocksdb.DB("lab2Log.db", rocksdb.Options(create_if_missing=True))

it = db.iteritems()
itV = db.itervalues()
it.seek_to_first()
itV.seek_to_first()

imAList = ["put", "test"]

def addData():
    db.put("3".encode("utf-8"), "abc".encode("utf-8"))
    db.put("1".encode("utf-8"), "abc".encode("utf-8"))
    db.put("2".encode("utf-8"), "abc".encode("utf-8"))

def printAll():
    it.seek_to_first()
    for i in list(it):
        print(i)

def deleteAll():
    it.seek_to_first()
    for i in list(it):
        print("deleting " + str(i))
        db.delete(i[0])

#print(int(list(it)[0].decode("utf-8"))+1)
printAll()
#deleteAll()
