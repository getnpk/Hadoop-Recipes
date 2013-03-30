#!/usr/bin/env python
#<nitin2.kumar@one97.net>


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

def create_table(table,client):
        hql="create table if not exists %s_old like %s"%(table, table)
        try:
                print hql
                client.execute(hql)
                print('created table')
        except Exception,e:
                print("exceptin %s" %e)


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

def getMonyear(year, month):
        mon={'01':'jan','02':'feb','03':'mar','04':'apr','05':'may','06':'jun','07':'jul','08':'aug','09':'sep','10':'oct','11':'nov','12':'dec'}
        return mon[month]+year

def createdir(d):
        if not os.path.exists(d):
                os.makedirs(d)

def search_files(rootpath, client):

    for root,dir,files in os.walk(rootpath):
        for filename in fnmatch.filter(files,pattern):
            print "%s" %(os.path.join(root,filename))
            fullpath=os.path.join(root,filename)
            #/backup/data/sm/mts/sm_matrix_report/sm_matrix_report_20110712_mts.csv.gz
            table_name=fullpath.split('/')[5]+'_old'
            print table_name
            os.system("gunzip %s"%fullpath)
            csvfile=fullpath[:fullpath.rfind('.')]
            size = os.path.getsize(csvfile)
            thedate=fullpath.split('/')[6].split('_')[3]
            year=thedate[:4]
            mon=thedate[4:6]
            day=thedate[6:8]
            calldate=year + '-' + mon + '-' + day
            monyear=getMonyear(year,mon)
            print monyear
            #print thedate
            print calldate
            if size <> 0:
                    os.system("hive -e \"load data local inpath '%s' into table %s partition (operator='mts', monyear='%s',calldate='%s')\""%(csvfile,table_name,monyear,calldate))
            else:
                pass

def main():
    rootpath=os.getcwd()
    transport=open_transport()
    client=get_client(transport)
    search_files(rootpath,client)


if __name__ == '__main__':
    main()
