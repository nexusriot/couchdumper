#!/usr/bin/env python
#-*- coding: utf-8 -*-

import httplib
import json, os, os.path
from optparse import OptionParser

def oDoc(obj):
    return json.dumps(json.loads(obj.read()))

def fileHandler(f):
    def handle(*args, **kwargs):
        file, folder, dbname, json = f(*args, **kwargs)
        folder = os.path.join(folder, dbname)
        place = os.path.join(folder, file)
        try:
            try:
                os.mkdir(folder)
            except OSError:
                pass
            destination = open(place, 'wb+')
            destination.write(json)
            destination.close
        except IOError:
            return None
    return handle

class CouchDumper:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect(self):
        return httplib.HTTPConnection(self.host, self.port)

    def get(self, u):
        connection = self.connect()
        header = {"Accept": "application/json"}
        connection.request("GET", u, None, header)
        return connection.getresponse()

    def listDocuments(self, dbName):
        lt = self.get(''.join(['/', dbName, '/', '_all_docs']))
        return oDoc(lt)

    def ids(self, dbName):
        d= json.loads(self.listDocuments(dbName=dbName))
        try:
            return [x[u"id"] for x in d[u"rows"] if len(d[u"rows"]) != 0]
        except: print "Not awaiable or empty database!"


    def getDoc(self, dbName, doc):
        return self.get(''.join(['/', dbName, '/', doc,]))

    def dump(self, dbName, dir):
        if self.ids(dbName) == None:
            return None
        for id in self.ids(dbName):
            print (u"processing id: %s" % (id))
            self.dumpstring(dbName, id, dir)
        print ("Completed!")

    @fileHandler
    def dumpstring(self, dbName, id, dir):
        if not dir: dir = os.path.dirname(__file__)
        return (id, dir, dbName, oDoc(self.getDoc(dbName, id)))


if __name__== '__main__':

    usage = "usage: couchdumper.py --database database_name [--server hostname] [--port port] [--path path-to-dump]"
    parser = OptionParser(usage=usage, version=0.1)
    parser.add_option("-d", "--database", dest="database", help = "database name")
    parser.add_option("-s", "--server", dest = "server", default = "localhost",help = "server name")
    parser.add_option("-p", "--port", dest = "port", default =5984, help = "port number")
    parser.add_option("--path", dest = "path", default = '', help = "dump path")
    (options, args) = parser.parse_args()

    if not options.database:
        print "Database name required!"
        print usage
    else:
        couch = CouchDumper(options.server, options.port)
        couch.dump(options.database, options.path)
