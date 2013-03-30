#!/usr/bin/env python

"""
creates xml files based on logtype, monyear selection
nitin2.kumar@one97.net
"""

import sys
import os
from datetime import date
import datetime
import logging
import logging.handlers

import MySQLdb

dbase='dw_arpu_40'
per='%'
monyear='Apr2012'
dump_path='/mysql/dwh-hadoop/arpu/arpulog'

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

start="""<?xml version="1.0" encoding="utf-8"?>
<!--
case_files:
current_time -n
0 : only on basis of no of files.
1 : on basis of no of files & last modified date format granular
2 : on basis of no of files & datetime from filename date format granular


case_db_dump:
cap -m and floor -n
0 : query_string
1 : static complete
2 : static datewise
3 : dynamic datewise
4 : static realtime datewise
5 : dynamic realtime datewise
6 : static realtime date hour wise
7 : dynamic realtime date hour wise -->
<config>
<common parent_work_path="%s">

"""%(dump_path)

end="""

</common>

</config>
<!--
case_files:
current_time -n
0 : last modified date hour granular
1 : datetime time from filename date hour granular


case_db_dump:
cap -m and floor -n
0 : query_string
1 : static complete
2 : static datewise
3 : static realtime
4 : dynamic
5 : realtime dynamic -->

"""

############### applog ######################

def generate_applog(db):
        logtype='applog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,device ,pool ,calltype ,callflavour ,call_patch ,patch_status ,asr_request ,asr_status ,call_starttime ,call_endtime ,call_duration ,call_ended_by ,call_appkeypress ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource ,insertdatetime ,param4_int ,param5_int ,param4_str ,param5_str ,call_date ,circle_code ,dwh_timestamp ,call_hour ,call_minute ,call_second"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="dwh_timestamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this



######################### sublog ##############################

def generate_sublog(db):
        logtype='sublog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,sg_circleid ,subscription_cat ,subscription_subcat ,subscription_offername,sub_mode ,subscription_keyword ,subscription_keypress ,confirmation_keypress ,songname ,songcode ,sub_requesttime ,charging_flag ,charging_response ,charging_datetime ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource ,insertdatetime ,param4_int ,param5_int ,param4_str ,param5_str ,call_date ,circle_code ,dwh_timestamp ,call_hour ,call_minute ,call_second"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="dwh_timestamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this





######################### eventlog ##############################

def generate_eventlog(db):
        logtype='eventlog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,call_endtime ,event_id ,event_name ,event_keyword ,category ,subcategory ,event_starttime,event_endtime ,event_keypress ,event_stopped ,sub_mode ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource ,insertdatetime ,param4_int ,param5_int ,param4_str ,param5_str ,call_date ,circle_code ,dwh_timestamp ,call_hour ,call_minute ,call_second"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="dwh_timestamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this


######################### faillog ##############################

def generate_faillog(db):
        logtype='faillog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="faillog_str_cdrid ,faillog_int_msisdn ,faillog_str_service ,faillog_int_dnis ,faillog_str_channel ,faillog_dtm_request_time ,faillog_dtm_lasttriedat ,faillog_str_appname ,faillog_int_noofattempts ,faillog_str_urlresponse ,faillog_str_responsereason,faillog_str_channel1 ,faillog_str_channel2 ,faillog_str_channel3 ,faillog_str_channel4 ,faillog_str_channel5 ,call_date ,circle_code ,time_stamp ,call_hour ,call_minute ,call_second"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="time_stamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this


######################### successlog ##############################

def generate_successlog(db):
        logtype='successlog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="successlog_str_cdrid ,successlog_int_msisdn ,appconfig_str_service ,successlog_int_dnis ,successlog_str_channel ,successlog_dtm_request_time ,successlog_dtm_lasttriedat ,successlog_int_noofattempts ,successlog_str_appname ,successlog_str_successresponse,successlog_str_channel1 ,successlog_str_channel2 ,successlog_str_channel3 ,successlog_str_channel4 ,successlog_str_channel5 ,call_date ,circle_code ,time_stamp ,call_hour ,call_minute ,call_second"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="time_stamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this


######################### rbtfaillog ##############################

def generate_rbtfaillog(db):
        logtype='rbtfaillog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="rbtfaillog_int_id ,rbtfaillog_int_msisdn ,rbtfaillog_int_clipid ,rbtfaillog_int_categoryid ,rbtfaillog_int_dnis ,rbtfaillog_str_channel ,rbtfaillog_str_songcode ,rbtfaillog_int_usercode ,rbtfaillog_int_servicetype ,rbtfaillog_int_caller1msisdn,rbtfaillog_int_caller2msisdn,rbtfaillog_int_caller3msisdn,rbtfaillog_dtm_request_time ,rbtfaillog_dtm_lasttried ,rbtfaillog_str_appname ,rbtfaillog_int_settype ,rbtfaillog_int_renttype ,rbtfaillog_int_chargeflag ,rbtfaillog_int_noofattempts ,rbtfaillog_str_errorcode ,rbtfaillog_str_errorreason ,rbtfaillog_str_circlecode ,rbtfaillog_str_serverip ,rbtfaillog_str_cdrid ,rbtfaillog_str_htstatus ,rbtfaillog_str_param14 ,rbtfaillog_str_param15 ,rbtfaillog_str_param16 ,rbtfaillog_int_priceflag ,time_stamp ,call_hour ,call_minute ,call_second ,call_date ,circle_code"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="time_stamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this


######################### rbtsuccesslog ##############################

def generate_rbtsuccesslog(db):
        logtype='rbtsuccesslog'

        hql="show tables like '%s_%s_%s'"%(logtype,per,monyear)
        #print hql
        table_list=execute_query(db,dbase,hql)

        if table_list:
                for i in table_list:
                        table=str(i[0])
                        table_name=table[:table.rfind('_')+1]
                        logtype=table[:table.rfind('_')]
                        #print table_name, logtype
                        this="""
<data   data_source_type="db_dump"
        mysql_host="localhost"
        mysql_port_list="3306"
        mysql_un="root"
        mysql_pw=""
        mysql_socket="/var/lib/mysql/mysql.sock"
        dbase="dw_arpu_40"
        table_name="%s"
        column_list="rbtsuccess_int_id ,rbtsuccess_int_msisdn ,rbtsuccess_int_clipid ,rbtsuccesslog_int_categoryid,rbtsuccesslog_int_dnis ,rbtsuccess_str_channel ,rbtsuccess_str_songcode ,rbtsuccess_int_usercode ,rbtsuccess_int_servicetype ,rbtsuccess_int_caller1msisdn,rbtsuccess_int_caller2msisdn,rbtsuccess_int_caller3msisdn,rbtsuccess_dtm_request_time ,rbtsuccess_dtm_setat ,rbtsuccess_str_appname ,rbtsuccess_int_settype ,rbtsuccess_renttype ,rbtsuccess_int_chargeflag ,rbtsuccess_str_circlecode ,rbtsuccess_str_serverip ,rbtsuccess_str_cdrid ,rbtsuccess_str_htstatus ,rbtsuccess_str_param14 ,rbtsuccess_str_param15 ,rbtsuccess_str_param16 ,rbtsuccess_int_priceflag ,time_stamp ,call_hour ,call_minute ,call_second ,call_date ,circle_code"
        case_db_dump="5"
        suffix=""
        m_mins="1400"
        enclosed_by=''
        terminated_by=","
        query_string=""
        date_range=""
        sweep_prev_table="7"
        datetime_field="time_stamp"
        datetime_format="YYYY-mm-DD"
        floor_timestamp="2012-02-01 00:00:00"
        timestamp_format="YYYY-mm-DD HH:MM:SS"
        table_datetime_format="MonYYYY"
        case_files=""
        log_type = "%s"
        raw_files_path =""
        file_ext="csv"
        n_mins="1400"
        precnt= "0"
        compressed_flag="n"
        unq_delim="@"
        nof_in_cf="120" />
                        """%(table_name, logtype)
                        print this



# do here..

print start

db=mysql_connect()
generate_applog(db)
generate_sublog(db)
generate_eventlog(db)
generate_faillog(db)
generate_successlog(db)
#generate_rbtfaillog(db)
#generate_rbtsuccesslog(db)

print end


