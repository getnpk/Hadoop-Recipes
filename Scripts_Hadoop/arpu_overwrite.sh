LOG="arpu_overwrite.log"
logtypes="applog sublog eventlog faillog successlog"
applications="52222"
operators="tata"
monyears="dec2011"

dates="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"

col_list_applog="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,device ,pool ,calltype ,callflavour ,call_patch ,patch_status ,asr_request ,asr_status ,call_starttime ,call_endtime ,call_duration ,call_ended_by ,call_date ,call_appkeypress ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource"
col_list_sublog="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,sg_circleid ,subscription_cat ,subscription_subcat ,call_date ,subscription_offername ,sub_mode ,subscription_keyword ,subscription_keypress ,confirmation_keypress ,songname ,songcode ,sub_requesttime ,charging_flag ,charging_response ,charging_datetime ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource"
col_list_eventlog="callid ,cdrid ,operator_id ,circle_id ,server_ip ,appdomain_type ,appgroup_type ,network_type ,app_name ,msisdn ,language ,caller_type ,alt_userid ,alt_userid_type ,dnis ,calltype ,callflavour ,asr_request ,asr_status ,call_starttime ,call_endtime ,call_date ,event_id ,event_name ,event_keyword ,category ,subcategory ,event_starttime ,event_endtime ,event_keypress ,event_stopped ,sub_mode ,param1_int ,param2_int ,param3_int ,param1_str ,param2_str ,param3_str ,callsource"
col_list_faillog="circle_code , call_date , faillog_str_cdrid , faillog_int_msisdn , faillog_str_service , faillog_int_dnis , faillog_str_channel , faillog_dtm_request_time , faillog_dtm_lasttriedat , faillog_str_appname , faillog_int_noofattempts , faillog_str_urlresponse , faillog_str_responsereason , faillog_str_channel1, faillog_str_channel2, faillog_str_channel3, faillog_str_channel4, faillog_str_channel5"
col_list_successlog="circle_code ,call_date ,successlog_str_cdrid ,successlog_int_msisdn ,appconfig_str_service ,successlog_int_dnis ,successlog_str_channel ,successlog_dtm_request_time,successlog_dtm_lasttriedat ,successlog_int_noofattempts,successlog_str_appname ,successlog_str_successresponse,successlog_str_channel1,successlog_str_channel2,successlog_str_channel3,successlog_str_channel4,successlog_str_channel5"

function setMonthYear(){

    year=`echo $monyear | cut -c 4-`
    m=`echo $monyear | cut -c -3`
    
    if [ $m = 'jan' ]; then
            month="01"
    elif [ $m = 'feb' ]; then
            month="02"
    elif [ $m = 'mar' ]; then
            month="03"
    elif [ $m = 'apr' ]; then
            month="04"
    elif [ $m = 'may' ]; then
            month="05"
    elif [ $m = 'jun' ]; then
            month="06"
    elif [ $m = 'jul' ]; then
            month="07"
    elif [ $m = 'aug' ]; then
            month="08"
    elif [ $m = 'sep' ]; then
            month="09"
    elif [ $m = 'oct' ]; then
            month="10"
    elif [ $m = 'nov' ]; then
            month="11"
    elif [ $m = 'dec' ]; then
            month="12"
    else
            echo "Exception!"
    fi
    
    echo "Current application : $application" >> $LOG
    echo "Current application : $application"
    echo "Current month : $month" >> $LOG
    echo "Current month : $month"
    echo "Current year : $year" >> $LOG
    echo "Current year : $year"
    
}

#overwrites data
function overwrite_arpu(){

    setMonthYear
    
    for day in $dates
    do
        echo "Current date : $year-$month-$day" >> $LOG
        echo "Current date : $year-$month-$day for application $application"
        echo "hive -e \"from arpu_${logtype}_text insert overwrite table arpu_${logtype} partition (operator='$operator',application='$application', monyear='$monyear', calldate='$year-$month-$day') select ${!1} where operator='$operator' and application='$application' and monyear='$monyear' and call_date='$year-$month-$day'\""
        hive -e "from arpu_${logtype}_text insert overwrite table arpu_${logtype} partition (operator='$operator',application='$application', monyear='$monyear', calldate='$year-$month-$day') select ${!1} where operator='$operator' and application='$application' and monyear='$monyear' and call_date='$year-$month-$day'" >> $LOG
    done
}
    
###main###

for logtype in $logtypes
    do
    for operator in $operators
    do
        for application in $applications
        do
            for monyear in $monyears
            do
                col_list=col_list_${logtype}
                overwrite_arpu $col_list
            done
        done
    done
done

echo "Done overwriting!" >> $LOG