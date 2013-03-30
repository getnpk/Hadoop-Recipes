#!/usr/bin/env python

"""
Modified arpu hadooploader for loading data from arpulogs, on a circle basis.
Nothing for e1util
"""

import logging.handlers
import fcntl
import random
import ConfigParser
from optparse import OptionParser
import datetime
import time
import re
import sys,os

import MySQLdb


sys.path.append('/home/hadoop/hive/build/dist/lib/py')

__author__ = "nitin p kumar"
__version__ = "0.4"
__maintainer__ = "nitin p kumar"
__email__ = "nitin2.kumar@one97.net"
__status__ = "Production"

dbase="etl_config40"
source_table="source_data_tracker"
suffix="merge"

# scriptdir
scriptdir = sys.path[0]
logdir = scriptdir + '/log'
#create if not exists
if not os.path.isdir(logdir):
    os.mkdir(logdir)
piddir = scriptdir + '/pid'    
if not os.path.isdir(piddir):
    os.mkdir(piddir)

############ rotating logger #################
def rotating_logger(logfile_name,log_max_size=1048576,log_max_files=10):
    #maximum size of log file 1 MB by default , max number of log files 10
    logfile=logdir + '/'+logfile_name
    print "logfile : %s" %logfile
    # Set up a specific mylogger with desired output level
    mylog = logging.getLogger('mylog')
    mylog.setLevel(logging.DEBUG)
    # Add the log message handler to the logger
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=log_max_size, backupCount=log_max_files)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S' )
    handler.setFormatter(formatter)
    mylog.addHandler(handler)
    return mylog

####################### read config and args #################
def parseopts():
    try:            
        usage = "usage: %prog options filename tablename"
        parser = OptionParser(usage)
        parser.add_option("-c", "--config",action="store", type="string",help="read details from config",dest="config")
        (options, args) = parser.parse_args()
        if len(args) == 4 and options.config:
            bu=args[0].strip()
            operator=args[1].strip()
            basepath=args[2].strip()
            logtype=args[3].strip()
            config_file=options.config
            if not os.path.isfile(config_file):
                msg=config_file+" does not exists"
                print msg
                raise
            return (bu,operator,basepath,config_file,logtype)
        else:
            parser.print_help()
            sys.exit()

    except Exception, e:
        print ('exception : '+str(e))
        sys.exit(0)


########## thrift ############

def open_transport(host="localhost",port=10000):
    """
    open transport to HiveServer
    """
    try:
        transport = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(transport)
        transport.open()
        return transport
    except Thrift.TException, tx:
##        mylog.error(tx)
        print tx
        raise Thrift.TException(tx)

def close_transport(transport):
    """
    close transport
    """
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

################### mysql db #############################
def mysql_connect(mysql_host="localhost",mysql_port=3306,mysql_un="root",mysql_pw="",mysql_sock="/var/lib/mysql/mysql.sock"):    
    try:
        db = MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_sock)
        return db        
    except Exception,e:
        raise

def mysql_close(db):
    try:
        db.close()
    except:
        pass

def fun_execute_query(db,dbase,query):
    """
    execute query 
    """
    db.select_db(dbase)
    cursor = db.cursor()
    cursor.execute(query)
    resultset = cursor.fetchall()
    cursor.close()
    db.commit()
    return(resultset)


################ get the logtype ##################
def get_logtype(logtype):
    return logtype.split('_')[0]

################ get the circle ###################
def get_circle(logtype):
    return logtype.split('_')[1]

################  Get File For Loading Purpose #################
def get_files_to_load(bu,operator,logtype="default"):
   
    db=mysql_connect()
    if bu=="arpu":
        fields="bocs_id,server_id,product,log_type,cf_name"
        fields1="sdt.bocs_id as bocs_id,mb.server_id,product,log_type,cf_name"
        query="select %s from %s sdt, master_bocs mb where bu='%s' and operator='%s' and log_type like '%s_%%' and merge_flag=1 and hadoop_load=0 and log_type != 'data_reconciliation' and mb.bocs_id=sdt.bocs_id" \
            %(fields1,source_table,bu,operator,logtype)
    else:
        mylog.debug('not arpu!')
        pass
    print query
    mylog.debug("get_files_to_load query : %s " %query)
    resultset=fun_execute_query(db,dbase,query)    
    mysql_close(db)
    return (resultset,fields)   #   return resultset and fields string
    

#################### load files ############################
def load(client,filename,table_name,table_struct,partition="",create_table="true"):
    try:
        if create_table.lower()=="true":                
            ##DDL Create table if not exists        
            hql='create table if not exists %s %s ' %(table_name,table_struct)
            msg = 'execute hql : %s' %(hql)
            print msg
            mylog.debug(msg)
            client.execute(hql)
            
        #   form hql load data inpath
        hql='load data local inpath "%s" into table %s ' %(filename,table_name)
        
        if partition:   #   check if there are partition columns    partition={'circle': '11', 'monyear': 'jan2011'}
            partition_str=",".join(['%s="%s"' %(k,v) for k,v in partition.items()])
            hql='%s partition(%s)' %(hql,partition_str) #   form hql with partition column

        msg = 'execute hql : %s' %(hql)
        print msg
        mylog.debug(msg)
        client.execute(hql)
        mylog.info('%s successfully loaded into %s' %(filename,table_name))
    except Thrift.TException, tx:
        raise
    except Exception, e:
        raise

############## status update ###################
def update_sdt(bocs_id,product,zip_name):
    try:
        db=mysql_connect()
        query="update source_data_tracker set hadoop_load=1 where bocs_id='%s' and product = '%s' and cf_name='%s'" %(bocs_id,product,zip_name)        
        print query
        mylog.debug(query)
        fun_execute_query(db,dbase,query)
        mysql_close(db)
    except Exception, e:
        raise
    finally:
        mysql_close(db)

# load it into a temp table and get distinct fields.
def get_field_values(client,filename,table_struct,field,circle):
    try:
        temp_tablen="%s_%s_%s_%s" %("temp",logtype,os.getpid(),random.randint(1000,9999)) #  form table name for temporary table
        partition={"operator":operator,"circle":circle}
        load(client,filename,temp_tablen,table_struct,partition)  #   Load filename to temp table
        hql='select distinct %s from %s' %(field,temp_tablen)
        msg = 'execute hql : %s' %(hql)
        print msg
        mylog.debug(msg)
        client.execute(hql)
        field_li=client.fetchAll()    #  list of strings ['2011-02-07', '2011-02-08']
        try :
            if 'NULL' in field_li :
                field_li.remove('NULL')
            if '0000-00-00' in field_li :
                field_li.remove('0000-00-00')
            if '' in field_li :
                field_li.remove('')
        except Exception,e:
            pass
        finally :
            return field_li ##        return(['2011-03-03', '2011-02-11','2011-02-12', '2010-02-08', '2010-03-07'])    #   test
    except Thrift.TException, tx:
        raise
    finally :
        try:
            hql='drop table %s' %(temp_tablen)  #   drop temp table
            client.execute(hql)
        except Thrift.TException, tx:
            raise
        


if __name__ == "__main__":
    try:
        (bu,operator,basepath,config_file,logtype)=parseopts()    
        logfile_name='%s-%s-%s-%s.log' %(bu,operator,logtype,os.path.basename(sys.argv[0]))
        mylog=rotating_logger(logfile_name)
        mylog.debug('Starting loader')
        mylog.debug('bu : '+bu)
        mylog.debug('operator : '+operator)
        mylog.debug('logtype : '+ logtype)
        mylog.debug('config_file : '+config_file)
        pid_file = '%s/%s_%s.pid' %(piddir,bu,operator)
        print "pid_file : %s" %pid_file
        mylog.debug("pid_file : %s" %pid_file)
        try:
            fp = open(pid_file, 'w')
            fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)  #   Take lock on file          
            try:
                from hive import ThriftHive
                from hive.ttypes import HiveServerException
                from thrift import Thrift
                from thrift.transport import TSocket
                from thrift.transport import TTransport
                from thrift.protocol import TBinaryProtocol
            except ImportError,e:
                raise

            try:
                config = ConfigParser.RawConfigParser()
                config.read(config_file)
                tab_struct='table_struct_'+logtype
                column_in='col_in_'+logtype
                hs_host=config.get('hive_server','hostname')
                hs_port=config.getint('hive_server','port')
                table_struct=config.get('table_details',tab_struct)
                col_sel=config.get('table_details',column_in)
                mylog.debug('hs_host : '+hs_host)
                mylog.debug('hs_port : '+str(hs_port))
                mylog.debug('table_struct : '+table_struct)
                mylog.debug('col_sel : '+col_sel)
            except Exception, e:
                raise
            
            try:
                transport=open_transport()
                client=get_client(transport)

                (file_set,fields)=get_files_to_load(bu,operator,logtype)
                field_li=fields.split(",")
                mylog.debug("field li : %s" %field_li)
                mylog.debug("file_set : %s" %str(file_set))
                
                if file_set:
                    for i in file_set:
                        try:                                
                            file_dict=dict(zip(field_li,i)) #   for every tuple in resultset form a dictinary (field name, field value)
                            # file to load
                            file_to_load="/".join([basepath,bu,operator,file_dict["product"],file_dict["log_type"],suffix,\
                                                   ".".join((file_dict["cf_name"][:file_dict["cf_name"].find(".")],"csv"))])                            
                            if bu=="arpu":
                                #check from here
                                logtype=get_logtype(file_dict["log_type"])
                                stag_tbl="stag_arpu_%s" %(logtype)
                                circle=get_circle(file_dict["log_type"])
                                partition={"operator":operator,"circle":circle} # do partition based on operator and circle
                                load(client,file_to_load,stag_tbl,table_struct,partition) # to the stag table.
                                fl="call_date"
                                call_date_li=get_field_values(client,file_to_load,table_struct,fl,circle)  #   load file to temp table and get distinct fields values
                                mylog.debug("data for call dates %s is present in file %s " %(str(call_date_li),file_to_load))
                                if call_date_li:
                                    for i in call_date_li:    #   Assuming calldate i is in format "yyyy-mm-dd"
                                        datep=re.compile(r'^(?P<year>\d{4})-(?P<mon>\d{2})-(?P<date>\d{2})$')
                                        datem=datep.match(i)
                                        if datem is not None:
                                            if i <> "0000-00-00":
                                                year=i[:4]
                                                m=i[5:7]
                                                dict1={'01':'jan','02':'feb','03':'mar','04':'apr','05':'may','06':'jun','07':'jul','08':'aug','09':'sep','10':'oct','11':'nov','12':'dec'}
                                                mon=dict1[m]
                                                monyear="%s%s"%(mon,year)
                                                numofdays=300
                                                if (datetime.datetime.now()-datetime.datetime(int(datem.group('year')),int(datem.group('mon')),int(datem.group('date')))).days < numofdays:
                                                    
                                                    hql="INSERT OVERWRITE TABLE arpu_%s PARTITION(operator='%s',circle='%s',monyear='%s', calldate='%s') SELECT %s FROM %s where \
                                                            circle='%s' and call_date='%s' group by %s " %(logtype,operator,circle,monyear,i,col_sel,stag_tbl,circle,i,col_sel)
                                                    print hql
                                                    mylog.info("executing hql : %s" %(hql)) 
                                                    try:                                        
                                                        client.execute(hql)
                                                        mylog.info('hql succeded')
                                                    except Thrift.TException, tx:
                                                        raise
                                                else:
                                                    #call date is older than threshhold date difference, ignore it
                                                    mylog.info("ignoring data for call date %s in file %s" %(i,file_to_load))
                                            else:
                                                    #Invalid call date
                                                    mylog.info("ignoring data for call date %s in file %s" %(i,file_to_load))
                                        else:
                                            raise Exception,("Invalid date %s" %i)
                                            
                                    update_sdt(file_dict["bocs_id"],file_dict["product"],file_dict["cf_name"])
                                    
                            else :
                                mylog.info("No %s is available in file %s " %(fl,file_to_load))
                                
                        except Exception, e:
                            msg='exception : %s' %str(e)
                            print (msg)
                            mylog.error(msg)
                            
                else:
                    mylog.info("file_set is empty,no file available to load")
                    print "file_set is empty,no file available to load"
            
            except Exception, e:
                raise
            
            finally:
                if transport:
                    close_transport(transport)
                    
        except IOError,e:
            raise
    
    except IOError,e:
            msg = 'another instance is running'
            print str(e)
            print msg
            mylog.error(str(e))
            mylog.error(msg)
            
    except ImportError,e:        
        print e
        mylog.error(e)
    except NameError,e:        
        print e
        mylog.error(e)
    except TypeError,e:        
        print e
        mylog.error(e)

    except Exception, e:
        print ('exception : '+str(e))
        sys.exit(0)

