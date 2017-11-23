import rocksdb
import time
import ast

db = rocksdb.DB("lab2.db", rocksdb.Options(create_if_missing=True))
dbLog = rocksdb.DB("lab2Log.db", rocksdb.Options(create_if_missing=True))
db2 = rocksdb.DB("lab2Slave.db", rocksdb.Options(create_if_missing=True))
db2Log = rocksdb.DB("lab2LogSlave.db", rocksdb.Options(create_if_missing=True))

it = db.iteritems()
it2 = db2.iteritems()
it2Log = db2Log.iteritems()
itV = db.itervalues()
it.seek_to_first()
itV.seek_to_first()

def encode(input):
    return input.encode("utf-8")

def addData():
    # db.put("3".encode("utf-8"), "abc".encode("utf-8"))
    # db.put("1".encode("utf-8"), "abc".encode("utf-8"))
    # db.put("2".encode("utf-8"), "abc".encode("utf-8"))
    db.put("abc".encode("utf-8"), encode("123"))
    db.put(encode("put2"), encode("456"))
    db.put(encode("put3"), encode("789"))
    db.put(encode("put4"), encode("4124"))
    db.put(encode("put5"), encode("1233123"))
    db.put(encode("put6"), encode("7812341249"))
    db.put(encode("put7"), encode("123123"))


def printAll():
    imAList = []

    it2Log.seek_to_first()
    # it.seek("1510981933.187634".encode("utf-8"))
    # for i in list(it):
    #     print(i[0].decode("utf-8"))
    #     print(i[1].decode("utf-8").split(","))
    #     imAList.append(i[0].decode("utf-8"))
    #     imAList.append(i[1].decode("utf-8"))

    print("printing master")
    it.seek_to_first()
    for i in list(it):
        print(i)

    print("printing slave database")
    it2.seek_to_first()
    for i in list(it2):
        print(i)

    # print("printing slave log")
    # it2Log.seek_to_first()
    # for i in list(it2Log):
    #     print(i)
    #
    # it.seek_to_first()
    # print(len(list(it)))
    # print(imAList)

def deleteAll():
    it.seek_to_first()
    for i in list(it):
        print("deleting " + str(i))
        db.delete(i[0])

def largestSeq():
    # print(db.get_live_files_metadata()[0].get('largest_seqno'))
    it.seek_to_first()
    for i in db.get_live_files_metadata():
        print(i)
    # print(db.get_live_files_metadata())

def printLast():
    it.seek_to_last()
    print(list(it))
    # it.seek_to_first()
    # for i in list(it):
    #     print (i)
    # it.seek_to_first()
    # print(it.seek('put5'.encode("utf-8")))
    # it.__next__()
    # print(list(it))
    # print(it)

    # print(list(it)[0][0].decode("utf-8"))

#print(int(list(it)[0].decode("utf-8"))+1)
printAll()
# printLast()
# deleteAll()
# addData()
# printAll()
# largestSeq()
