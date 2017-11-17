import rocksdb
import time
import ast

db = rocksdb.DB("lab2.db", rocksdb.Options(create_if_missing=True))

it = db.iterkeys()
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
    itV.seek_to_first()
    for i in list(it):
        print(i.decode("utf-8"))
    for i in list(itV): 
        print(i.decode("utf-8"))

def deleteAll():
    it.seek_to_first()
    for i in list(it):
        print("deleting " + i.decode("utf-8")) 
        db.delete(i)

addData()
it.seek_to_last()
print(int(list(it)[0].decode("utf-8"))+1)
