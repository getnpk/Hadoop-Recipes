#!/usr/bin/env python
"""
Sweeps all tables in the database and stores them onto
a single table in the database. Preet is done on this central
table for pulling data on regular / weekly basis.

"""

__author__="nitin p kumar"
__email__="nitin2.kumar@one97.net"
__status__ = "Production"
__version__ = "0.1"

import sys
import os
from datetime import date
import datetime
import logging
import logging.handlers

import MySQLdb


operator = sys.argv[1].lower()
application = sys.argv[2].lower()
dbase = sys.argv[3].lower()
logtype = sys.argv[4].lower()
scriptdir = sys.argv[5].lower()
datefrom = sys.argv[6].lower()

per='%'
logdir = scriptdir + '/log'
if not os.path.isdir(logdir):
    os.mkdir(logdir)

def check_if_table_exists():
        """
        before insert overwirte check if table exists
        left to error handling
        """
        pass

########### connect to mysql ######################
def mysql_connect(mysql_host='localhost',mysql_port=3306,mysql_un='root',mysql_pw='',mysql_sock='/var/lib/mysql/mysql.sock'):
    try:
        db=MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_sock)
        return db
    except Exception ,e:
        raise

############## Mysql Disconnect ###################
def mysql_close(db):
    try:
        db.close()
    except Exception, e:
        raise

 ################ Mysql query Execute ###############
def execute_query(db,dbase,query):
    db.select_db(dbase)
    cursor = db.cursor()
    try:
        cursor.execute(query)
        resultset = cursor.fetchall()
    except Exception,e:
        raise
    cursor.close()
    db.commit()
    return(resultset)

def get_logging(log_file,log_max_size=1048576,log_max_files=10):
    logfile=logdir + '/'+log_file
    print "logfile : %s" %logfile
    mylog = logging.getLogger('mylog')
    mylog.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=log_max_size, backupCount=log_max_files)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S' )
    handler.setFormatter(formatter)
    mylog.addHandler(handler)
    return mylog


def getList(table,logtype):
    if logtype in ('applog'):
        sel_list='callid,cdrid,operator_id,circle_id,server_ip,appdomain_type,appgroup_type,network_type,app_name,msisdn,language,caller_type,alt_userid,alt_userid_type,dnis,device,pool,calltype,callflavour,call_patch,patch_status,asr_request,asr_status,call_starttime,call_endtime,call_duration,call_ended_by,call_appkeypress,param1_int,param2_int,param3_int,param1_str,param2_str,param3_str,callsource,insertdatetime,param4_int,param5_int,param4_str,param5_str,call_date,circle_code,dwh_timestamp,call_hour,call_minute,call_second'
        mylog.debug('col list for applog: ' +sel_list)

    elif logtype in ('sublog'):
        sel_list='callid,cdrid,operator_id,circle_id,server_ip,appdomain_type,appgroup_type,network_type,app_name,msisdn,language,caller_type,alt_userid,alt_userid_type,dnis,calltype,callflavour,asr_request,asr_status,call_starttime,sg_circleid,subscription_cat,subscription_subcat,subscription_offername,sub_mode,subscription_keyword,subscription_keypress,confirmation_keypress,songname,songcode,sub_requesttime,charging_flag,charging_response,charging_datetime,param1_int,param2_int,param3_int,param1_str,param2_str,param3_str,callsource,insertdatetime,param4_int,param5_int,param4_str,param5_str,call_date,circle_code,dwh_timestamp,call_hour,call_minute,call_second'
        mylog.debug('col list for sublog: ' +sel_list)

    elif logtype in ('eventlog'):
            sel_list='callid,cdrid,operator_id,circle_id,server_ip,appdomain_type,appgroup_type,network_type,app_name,msisdn,language,caller_type,alt_userid,alt_userid_type,dnis,calltype,callflavour,asr_request,asr_status,call_starttime,call_endtime,event_id,event_name,event_keyword,category,subcategory,event_starttime,event_endtime,event_keypress,event_stopped,sub_mode,param1_int,param2_int,param3_int,param1_str,param2_str,param3_str,callsource,insertdatetime,param4_int,param5_int,param4_str,param5_str,call_date,circle_code,dwh_timestamp,call_hour,call_minute,call_second'
            mylog.debug('col list for eventlog: ' +sel_list)

    elif logtype in ('successlog'):
        sel_list='circle_code,call_date,successlog_str_cdrid,successlog_int_msisdn,appconfig_str_service,successlog_int_dnis,successlog_str_channel,successlog_dtm_request_time,successlog_dtm_lasttriedat,successlog_int_noofattempts,successlog_str_appname,successlog_str_successresponse,successlog_str_channel1,successlog_str_channel2,successlog_str_channel3,successlog_str_channel4,successlog_str_channel5,time_stamp,call_hour,call_minute,call_second'
        mylog.debug('col list for successlog : ' +sel_list)

    elif logtype in ('faillog'):
        sel_list='circle_code,call_date,faillog_str_cdrid,faillog_int_msisdn,faillog_str_service,faillog_int_dnis,faillog_str_channel,faillog_dtm_request_time,faillog_dtm_lasttriedat,faillog_str_appname,faillog_int_noofattempts,faillog_str_urlresponse,faillog_str_responsereason,faillog_str_channel1,faillog_str_channel2,faillog_str_channel3,faillog_str_channel4,faillog_str_channel5,time_stamp,call_hour,call_minute,call_second'
        mylog.debug('col list for faillog : ' +sel_list)

    elif logtype in ('rbtfaillog'):
        sel_list='rbtfaillog_int_id ,rbtfaillog_int_msisdn ,rbtfaillog_int_clipid ,rbtfaillog_int_categoryid ,rbtfaillog_int_dnis ,rbtfaillog_str_channel ,rbtfaillog_str_songcode ,rbtfaillog_int_usercode ,rbtfaillog_int_servicetype ,rbtfaillog_int_caller1msisdn,rbtfaillog_int_caller2msisdn,rbtfaillog_int_caller3msisdn,rbtfaillog_dtm_request_time ,rbtfaillog_dtm_lasttried ,rbtfaillog_str_appname ,rbtfaillog_int_settype ,rbtfaillog_int_renttype ,rbtfaillog_int_chargeflag ,rbtfaillog_int_noofattempts ,rbtfaillog_str_errorcode ,rbtfaillog_str_errorreason ,rbtfaillog_str_circlecode ,rbtfaillog_str_serverip ,rbtfaillog_str_cdrid ,rbtfaillog_str_htstatus ,rbtfaillog_str_param14 ,rbtfaillog_str_param15 ,rbtfaillog_str_param16 ,rbtfaillog_int_priceflag ,time_stamp ,call_hour ,call_minute ,call_second ,call_date ,circle_code'
        mylog.debug('col list for rbtfaillog : ' +sel_list)

    elif logtype in ('rbtsuccesslog'):
        sel_list="rbtsuccess_int_id ,rbtsuccess_int_msisdn ,rbtsuccess_int_clipid ,rbtsuccesslog_int_categoryid,rbtsuccesslog_int_dnis ,rbtsuccess_str_channel ,rbtsuccess_str_songcode ,rbtsuccess_int_usercode ,rbtsuccess_int_servicetype ,rbtsuccess_int_caller1msisdn,rbtsuccess_int_caller2msisdn,rbtsuccess_int_caller3msisdn,rbtsuccess_dtm_request_time ,rbtsuccess_dtm_setat ,rbtsuccess_str_appname ,rbtsuccess_int_settype ,rbtsuccess_renttype ,rbtsuccess_int_chargeflag ,rbtsuccess_str_circlecode ,rbtsuccess_str_serverip ,rbtsuccess_str_cdrid ,rbtsuccess_str_htstatus ,rbtsuccess_str_param14 ,rbtsuccess_str_param15 ,rbtsuccess_str_param16 ,rbtsuccess_int_priceflag ,time_stamp ,call_hour ,call_minute ,call_second ,call_date ,circle_code"
        mylog.debug('col list for rbtsuccesslog : ' +sel_list)
    return sel_list


if __name__=="__main__" :
    try:

        logfile_name='%s-%s.log' %(operator,application)
        mylog=get_logging(logfile_name)
        mylog.debug('operator :' +operator)
        mylog.debug('application :' +application)
        mylog.debug('database :' +dbase)
        mylog.debug('logtype :' +logtype)

        db=mysql_connect()

        end_date=date.today()-datetime.timedelta(days=3)
        start_date=date.today()-datetime.timedelta(days=int(datefrom))

        D1=start_date.strftime('%Y-%m-%d')
        mylog.debug('Start date ' + D1)
        D2=end_date.strftime('%Y-%m-%d')
        mylog.debug('End date ' + D2)
        print('Start date ' + D1)
        print('End date ' + D2)

        monyear=end_date.strftime('%b%Y')
        start_monyear=start_date.strftime('%b%Y')
        mylog.debug('Current monyear ' + monyear )
        mylog.debug('Start monyear ' + start_monyear )
        print('Current monyear ' + monyear )
        print('Start monyear ' + start_monyear )

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)
        #print table_list
        mylog.debug('All entries to be based on this list:')
        mylog.debug(table_list)

        base_table="%s_%s"%(logtype,application)
        hql="truncate table %s"%(base_table)
        print hql
        mylog.debug('truncating query :' + hql)
        execute_query(db,dbase,hql)

        if table_list:
            for i in table_list:
                table=str(i[0])
                sel_list=getList(table,logtype)
                try:
                    if monyear <> start_monyear:
                        pre_table=table[:table.rfind('_')+1]+start_date.strftime('%b%Y')
                        try:
                            print 'Pre_table ' + pre_table
                            hql1="insert into %s select %s from %s where call_date >= '%s' "%(base_table,sel_list,pre_table,D1)
                            mylog.info('pre_table ' + pre_table)
                            mylog.debug(hql1)
                            execute_query(db,dbase,hql1)
                        except Exception,e:
                            mylog.error('Error inserting from previous table ' + pre_table)

                        try:
                            print 'Current table ' + table
                            hql2="insert into %s select %s from %s where  call_date <= '%s' "%(base_table,sel_list,table,D2)
                            mylog.info('current table ' + table)
                            mylog.debug(hql2)
                            execute_query(db,dbase,hql2)
                        except Exception,e:
                            mylog.error('Error inserting from current table ' + table)

                        # do the remaining months in between if any?

                        try:
                            year_toggle= True if (end_date.month-start_date.month < 0) else False

                            if (year_toggle):
                                #code to be added for if year change
                                times=(end_date.month-start_date.month) % 12
                                while(times != 1):
                                    if (start_date.month == 12):
                                        start_date=start_date.replace(year=start_date.year+1)
                                    start_date=start_date.replace(month=(start_date.month+1) % 12)
                                    pre_table=table[:table.rfind('_')+1]+start_date.strftime('%b%Y')
                                    print 'Middle pretable ' + pre_table
                                    hql="insert into %s select %s from %s "%(base_table,sel_list,pre_table)
                                    mylog.info('middle table ' + pre_table)
                                    mylog.debug(hql)
                                    execute_query(db,dbase,hql)
                                    times-=1
                                    # region to be checked
                            else:
                                times=(end_date.month-start_date.month) % 12
                                while(times != 1):
                                    start_date=start_date.replace(month=start_date.month+1)
                                    pre_table=table[:table.rfind('_')+1]+start_date.strftime('%b%Y')
                                    print 'middle pretable ' + pre_table
                                    hql="insert into %s select %s from %s "%(base_table,sel_list,pre_table)
                                    mylog.info('middle table ' + pre_table)
                                    mylog.debug(hql)
                                    execute_query(db,dbase,hql)
                                    times-=1
                        except Exception,e:
                            mylog.error('Error while doing middle table ' + pre_table)


                    else:
                        try:
                            hql="insert into %s select %s from %s where call_date >= '%s' and call_date <= '%s' "%(base_table,sel_list,table,D1,D2)
                            execute_query(db,dbase,hql)
                        except Exception,e:
                            mylog.error('Error inserting from current table ' + table )

                    mylog.debug('inserted data from table : ' +base_table)

                except Exception, e:
                    mylog.error('An exception occured while overwriting data ')
                    mylog.error(e)
            mylog.debug('############ Completed dumping into ' + base_table + '! ###############')
    except Exception,e:
        mylog.error(e)
        raise



