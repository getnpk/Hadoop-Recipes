
#
#dumps data from arpu tables
#<nitin2.kumar@one97.net>


operator="tata"
user="root"
pass=""

dbs="dw_arpu_40"
applications="applog sublog eventlog faillog successlog"
months="Aug Sep Oct Nov"

dump_dir="/mysql/dump2csv"
#log_file=${db}_`date +%Y_%m_%d_%H_%M_%S`.log
log_file=logger.log

uninor_col_list_applog="callid ,cdrid ,Operator_ID ,circle_id ,server_IP ,appDomain_type ,appGroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,null,dnis ,device ,pool ,callType ,null,call_patch ,patch_status ,asr_request ,asr_status ,call_starttime ,call_endtime ,call_duration ,call_ended_by ,call_date ,null,param1_int ,param2_int ,null,param1_str ,param2_str ,null,null"
uninor_col_list_sublog="callid , cdrid , Operator_ID , circle_id , server_IP , appDomain_type , appGroup_type , network_type , app_name , msisdn , language , caller_type , alt_userid ,null, dnis , callType ,null, asr_request , asr_status , call_starttime , SG_CircleID , subscription_cat , subscription_subcat , call_date ,null, sub_mode , subscription_keyword , subscription_keypress , confirmation_keypress , SongName , SongCode , sub_requesttime , charging_flag , charging_response , charging_datetime , param1_int , param2_int ,null, param1_str , param2_str ,null,null"
uninor_col_list_eventlog="callid , cdrid , Operator_ID , circle_id , server_IP , appDomain_type , appGroup_type , network_type , app_name , msisdn , language , caller_type , alt_userid ,null, dnis , callType ,null, asr_request , asr_status , call_starttime , call_endtime , call_date , event_id , event_name ,null, category , subcategory , event_starttime, event_endtime ,null,null,null, param1_int , param2_int ,null, param1_str , param2_str ,null,null"
uninor_col_list_faillog="circle_code ,call_date ,faillog_str_cdrid ,faillog_int_msisdn ,faillog_str_service ,faillog_int_dnis ,faillog_str_channel ,faillog_dtm_request_time ,faillog_dtm_lasttriedat ,faillog_str_appname ,faillog_int_noofattempts ,faillog_str_urlresponse ,faillog_str_responsereason,null,null,null,null,null"
uninor_col_list_successlog="circle_code ,call_date ,successlog_str_cdrid ,successlog_int_msisdn ,appconfig_str_service ,successlog_int_dnis ,successlog_str_channel ,successlog_dtm_request_time ,successlog_dtm_lasttriedat ,successlog_int_noofattempts ,successlog_str_appname ,null,null,null,null,null,null"

idea_col_list_applog="callid , cdrid , Operator_ID , circle_id , server_IP , appDomain_type , appGroup_type , network_type , app_name , msisdn , language , caller_type , alt_userid ,null, dnis , device , pool , callType ,null, call_patch , patch_status , asr_request , asr_status , call_starttime , call_endtime , call_duration , call_ended_by , call_date , call_appkeypress, param1_int , param2_int , param3_int , param1_str , param2_str , param3_str ,null"
idea_col_list_sublog="callid , cdrid , Operator_ID , circle_id , server_IP , appDomain_type , appGroup_type , network_type , app_name , msisdn , language , caller_type , alt_userid ,null, dnis , callType ,null, asr_request , asr_status , call_starttime , SG_CircleID , subscription_cat , subscription_subcat ,call_date, offer , sub_mode , subscription_keyword , subscription_keypress , confirmation_keypress , SongName , SongCode , sub_requesttime , charging_flag , charging_response , charging_datetime , param1_int , param2_int , param3_int , param1_str , param2_str , param3_str ,null"
idea_col_list_eventlog="callid ,cdrid ,Operator_ID ,circle_id ,server_IP ,appDomain_type ,appGroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,null,dnis ,callType ,null,asr_request ,asr_status ,call_starttime ,call_endtime ,call_date ,event_id ,event_name ,event_keyword ,category ,subcategory ,event_starttime ,event_endtime ,event_keypress ,event_stopped ,null,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,null"
idea_col_list_faillog="circle_code ,call_date ,faillog_str_cdrid ,faillog_int_msisdn ,faillog_str_service ,faillog_int_dnis ,faillog_str_channel ,faillog_dtm_request_time ,faillog_dtm_lasttriedat ,faillog_str_appname ,faillog_int_noofattempts ,faillog_str_urlresponse ,faillog_str_responsereason ,faillog_str_channel1,faillog_str_channel2,faillog_str_channel3,faillog_str_channel4,faillog_str_channel5"
idea_col_list_successlog="circle_code ,call_date ,successlog_str_cdrid ,successlog_int_msisdn ,appconfig_str_service ,successlog_int_dnis ,successlog_str_channel ,successlog_dtm_request_time ,successlog_dtm_lasttriedat ,successlog_int_noofattempts ,successlog_str_appname ,successlog_str_successresponse ,successlog_str_channel1 ,successlog_str_channel2 ,successlog_str_channel3 ,successlog_str_channel4 ,successlog_str_channel5"

tata_col_list_applog="callid , cdrid , operator_id , circle_id , server_ip , appdomain_type , appgroup_type , network_type , app_name , msisdn , language , caller_type , alt_userid , alt_userid_type , dnis , device , pool , calltype , callflavour , call_patch , patch_status , asr_request , asr_status , call_starttime , call_endtime , call_duration , call_ended_by ,call_date , call_appkeypress, param1_int , param2_int , param3_int , param1_str , param2_str , param3_str , callsource , dwh_timestamp"
tata_col_list_sublog="callid,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,sg_circleid ,subscription_cat ,subscription_subcat ,call_date ,subscription_offername,sub_mode ,subscription_keyword ,subscription_keypress ,confirmation_keypress ,songname ,songcode ,sub_requesttime ,charging_flag ,charging_response ,charging_datetime ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource ,dwh_timestamp"
tata_col_list_eventlog="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,call_endtime ,call_date ,event_id ,event_name ,event_keyword ,category ,subcategory ,event_starttime,event_endtime ,event_keypress ,event_stopped ,sub_mode ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource ,dwh_timestamp"
tata_col_list_faillog="circle_code ,call_date ,faillog_str_cdrid ,faillog_int_msisdn ,faillog_str_service ,faillog_int_dnis ,faillog_str_channel ,faillog_dtm_request_time ,faillog_dtm_lasttriedat ,faillog_str_appname ,faillog_int_noofattempts ,faillog_str_urlresponse ,faillog_str_responsereason,null,null,null,null,null,time_stamp"
tata_col_list_successlog="circle_code ,call_date ,successlog_str_cdrid ,successlog_int_msisdn ,appconfig_str_service ,successlog_int_dnis ,successlog_str_channel ,successlog_dtm_request_time,successlog_dtm_lasttriedat ,successlog_int_noofattempts,successlog_str_appname ,null,null,null,null,null,null,time_stamp"

function print_info(){
    echo "start" > $log_file
    echo "database : " $db >> $log_file
    echo "user : "$user >> $log_file
    echo "pass : "$pass >> $log_file
    echo "dump_dir : "$dump_dir >> $log_file
    echo "month : "$month >> $log_file
    echo "operator : "$operator >> $log_file
}

function create_dir(){
    mkdir -p $dump_dir
    chmod 777 $dump_dir
}

#dumps data
function dump_all(){

    print_info

    table_list=`mysql -u${user} --password=${pass} $db --skip-column-names -e "show tables like '${application}_____${month}2011'"`
    echo "table_list:"
    echo $table_list

    for table in $table_list
    do
        echo $table
        mysql -u${user} --password=${pass} $db -e "select ${!1} from $table into outfile '$dump_dir/arpu_${operator}_${db}_$table.csv' fields terminated by ',' lines terminated by '\r\n'" >> $log_file
        gzip $dump_dir/arpu_${operator}_${db}_$table.csv >> $log_file
    done

    echo "done for $db $application $month"  >> $log_file
}

###main###
create_dir
for db in $dbs
do
    for month in $months
    do
        for application in $applications
        do
           col_list=${operator}_col_list_${application}
           dump_all $col_list
        done
    done
done


