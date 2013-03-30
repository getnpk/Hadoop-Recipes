import os
import sys
import glob
import fnmatch
import shutil

try:
        from hive import ThriftHive
        from hive.ttypes import HiveServerException
        from thrift import Thrift
        from thrift.transport import TSocket
        from thrift.transport import TTransport
        from thrift.protocol import TBinaryProtocol
except ImportError,e:
        raise

pattern='*.gz'

def create_table(tablename,case,client):
        if case == 1:
                hql="create table if not exists %s (sgid string, msisdn string, starttime string, connecttime string, endtime string, callduration string, outdialerid string, callfromcli bigint) row format delimited fields terminated by ',' escaped by '\\\\' lines terminated by '\\n'" %tablename
                try:
                        print hql
                        client.execute(hql)
                except Exception,e:
                        print("exceptin %s" %e)
        else:
                os.system("hive -e \"create table if not exists %s (msisdn string,starttime string, callduration string, appname string, call_from_cli string, server_id string) row format delimited fields terminated by ',' escaped by '\\' lines terminated by '\n'\"" %tablename)


def open_transport(host="localhost",port=10000):
    try:
        transport = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(transport)
        transport.open()
        return transport
    except Thrift.TException, tx:
        print tx
        raise Thrift.TException(tx)


def close_transport(transport):
    try:
        transport.close()
    except Thrift.TException, tx:
        raise


def get_client(transport):
    try:
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = ThriftHive.Client(protocol)
        return client
    except Thrift.TException, tx:
        raise

def createdir(d):
        if not os.path.exists(d):
                os.makedirs(d)

def unzipall(rootpath):
        pattern='*.rar'
        for root,dir,files in os.walk(rootpath):
                for filename in fnmatch.filter(files,pattern):
                        print "Deflating file %s" %(os.path.join(root,filename))
                        os.system("unzip %s" %(os.path.join(root,filename)))


def search_files(rootpath, client):
    
    for root,dir,files in os.walk(rootpath):
        for filename in fnmatch.filter(files,pattern):
            print "%s" %(os.path.join(root,filename))
            fullpath=os.path.join(root,filename)
            #circle_name=fullpath.split('/')[6].lower()
            #appname=fullpath.split('/')[7].lower().split('.')[0]

def readfirstline(filepath):
        try:
                f = open(filepath)
                fline=f.readline()
        except Exception,e:
                print e
        return len(fline.split(','))

def main():
        transport=open_transport()
        client=get_client(transport)
        #unzipall(os.getcwd())
        #createdir("discardfiles")
        #search_files(os.getcwd(),client)

        if transport:
            close_transport(transport)



if __name__ == '__main__':
        main()

