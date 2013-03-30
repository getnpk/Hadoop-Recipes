

#To overwrite to outlog_obd from ${operator}_obd tables
#To load files to ${operator}_obd tables
#<nitin2.kumar@one97.net>


#loads data
function obd_loader(){
    cd $path
    for i in `ls *.gz`
    do
        echo "Gunzipping file : $i"
        gunzip $i
        myfile=`ls *.csv`
        c=`echo $i | awk 'BEGIN{FS="_"}{for (i=1; i<=NF; i++) if (i==NF-1) print $i}'`
        echo "Current load file : $myfile"
        echo "Current load file : $myfile" >> $LOG
        hive -e "load data local inpath '$myfile' into table ${operator}_obd_text partition (circle='$c',monyear='$monyear')" >> $LOG
        echo "Gzipping $myfile"
        gzip $myfile
    done
    echo "Done loading data into table ${operator}_obd" >> $LOG
    echo "Done loading data into table ${operator}_obd"

}


#overwrites data
function overwrite_obd(){

    operator=$1

    for circle in $circles
    do
        echo "Current circle : $circle" >> $LOG
        echo "Current circle : $circle"
        for day in $dates
        do
        echo "Current date : $year-$month-$day" >> $LOG
        echo "Current date : $year-$month-$day for circle $circle"
        echo $operator

        if [ $operator = 'globacom' ]; then
            echo "hive -e \"from ${operator}_obd_text insert overwrite table outlog_obd partition (operator='$operator',circle='$circle', monyear='$monyear', calldate='$year-$month-$day') select $col_list where circle='$circle' and monyear='$monyear' and to_date(connect_time)='$year-$month-$day'\""
                hive -e "from ${operator}_obd_text insert overwrite table outlog_obd partition (operator='$operator',circle='$circle', monyear='$monyear', calldate='$year-$month-$day') select $col_list where circle='$circle' and monyear='$monyear' and to_date(connect_time)='$year-$month-$day'" >> $LOG
        else
            echo "hive -e \"from ${operator}_obd_text insert overwrite table outlog_obd partition (operator='$operator',circle='$circle', monyear='$monyear', calldate='$year-$month-$day') select $col_list where circle='$circle' and monyear='$monyear' and calldate='$year-$month-$day'\""
            hive -e "from ${operator}_obd_text insert overwrite table outlog_obd partition (operator='$operator',circle='$circle', monyear='$monyear', calldate='$year-$month-$day') select $col_list where circle='$circle' and monyear='$monyear' and call_date='$year-$month-$day'" >> $LOG
        fi

        size=`hadoop fs -du /hive/warehouse/outlog_obd/operator=${operator}/circle=${circle}/monyear=${monyear}/calldate=${year}-${month}-${day} | awk 'NR > 1' | awk '{print $1}'`
        size_mb=`echo $size / 1024 / 1024 | bc `
        echo "Loaded file size $size_mb MB deflate"
        echo "Loaded file size $size_mb MB deflate" >> $LOG
        done
    done
    echo "Overwriting data done for table outlog_obd ${operator}"
    echo "Done overwriting!" >> $LOG

}


now=`date +"%b%d_%Y_%H_%M"`
LOG="obd_outlog.log_${now}"

echo $LOG
FAIL="99"


if [ $# -eq "2" ]; then
    echo "Overwriting only"
    operator=$1
    monyear=$2

elif [ $# -eq "3" ]; then
        folder=$3
        if [ -d $folder ]; then
        operator=$1
        monyear=$2
        path="/backup/data/obd/${operator}"
        path=$3
        else
            echo "Folder does not exist"
            echo "Usage: obd_outlog <operator> <monyear> <path_to_folder>"
            exit $FAIL
        fi

else
        echo "Usage: To overwrite: obd_outlog <operator> <monyear>"
        echo "Usage: To load (and overwrite): obd_outlog <operator> <monyear> <path_to_folder>"
        exit $FAIL

fi


dates="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"

# $1 operator
# $2 monyear

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


case $1 in
aircel)
                operator="aircel"
                circles="70 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96"
                col_list="msisdn,call_from_cli,appname,null,null,null,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,null,null,null,null,null,null,null,null,null,keypressed1,keypressed2,null,null,null,null,null,null,null,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response_id,null,null,null,charging_rate_id"
                ;;
airtel)
                operator="airtel"
                circles="50 60 61 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 99"
                col_list="msisdn,call_from_cli,appname,multipack,null,null,status,subscription_status_code,wcflag,baseresetflag,server_id,main_id,outdialer_id,userbaseid,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,eup,langkey,null,null,null,null,null,null,null,keypressed1,keypressed2,null,param1,param2,param3,param4,param5,param6,null,null,null,null,vcode,failure_isdn_reason,failure_reason,ivr_response,song_selected,time_stamp,null,null"
                ;;
bsnl)
                operator="bsnl"
                circles="10 20"
                col_list="msisdn,call_from_cli,appname,packname,btype,stype,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,platform,null,device,pool,pack_value,null,null,null,null,null,null,null,null,null,keypressed1,keypressed2,null,null,null,null,null,null,null,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,failure_isdn_reason,failure_reason,ivr_response,song_selected,time_stamp,null,charging_rate_id"
                ;;
idea)
                operator="idea"
                circles="10 11 12 14 20 30 40 50 60 70 80 81 90 91 92 93 94 95 96"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,null,null,null,null,null,null,null,null,null,keypressed1,keypressed2,null,null,null,null,null,null,null,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response_id,null,null,null,charging_rate_id"
                ;;
mts)
                operator="mts"
                circles="10 20 30 40 50 60 70 80 90 91 92 93 95 96 97"
                col_list="msisdn,call_from_cli,appname,null,null,null,status,subscription_status_code,wcflag,baseresetflag,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,eup,langkey,null,null,null,null,null,null,null,keypressed1,keypressed2,null,param1,param2,param3,param4,param5,param6,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response_id,null,time_stamp,null,charging_rate_id"
                ;;
uninor)
                operator="uninor"
                circles="10 11 12 13 14 20 30 40 50 60 70 80 90"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,baseresetflag,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,eup,langkey,null,null,null,null,null,null,null,keypressed1,keypressed2,null,param1,param2,param3,param4,param5,param6,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,null,null,null,song_selected,time_stamp,null,null"
                ;;
videocon)
                operator="videocon"
                circles="13 20 30 40 50 60 70 80"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,baseresetflag,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,platform,vendorname,device,pool,null,eup,langkey,null,null,null,null,null,null,null,keypressed1,keypressed2,null,param1,param2,param3,param4,param5,param6,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response_id,null,time_stamp,null,charging_rate_id"
                ;;
globacom)
                operator="globacom"
                circles="10"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscriptionflag,wcflag,baseresetflag,server_id,main_id,outdialer_id,circle_id,centralappid,connect_time,start_time,end_time,call_duration,platform,vendorname,device,pool,null,eup,langkey,null,null,null,null,null,null,null,keypress1,keypress2,null,param1,param2,param3,param4,param5,param6,null,null,null,null,vcode,failure_isdn_reason,failure_reason,ivr_response,songselected,null,null,null"
                ;;
airtelnigeria)
                operator="airtelnigeria"
                circles="10"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,baseresetflag,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,vendorname,device,pool,null,eup,langkey,null,maxtime,null,null,null,null,null,keypressed1,keypressed2,keypressed3,param1,param2,param3,param4,param5,param6,null,null,null,null,vcode,failure_isdn_reason,failure_reason,ivr_response,song_selected,time_stamp,null,null"
                ;;
vodafone)
                operator="vodafone"
                circles="11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 41 52 55 58 66 69 75"
                col_list="msisdn,call_from_cli,appname,null,null,null,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,circleappid,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,null,device,pool,null,null,null,null,max_times,null,null,busyornotanswer,clipunavailable,successretry,keypressed1,keypressed2,keypressed3,null,null,null,null,null,null,content_type_id,log_type_id,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response_id,null,null,null,charging_rate_id"
                ;;
tata)
                operator="tata"
                circles="10 11 12 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,circleappid,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,null,device,pool,null,null,null,filename,maxtime,missingclip,sattempts,null,null,null,keypressed1,keypressed2,keypressed3,null,null,null,null,null,null,content_type_id,log_type_id,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response,null,time_stamp,null,charging_rate_id"

                ;;
reliance)
                operator="reliance"
                circles="10 20 30 40 50 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76"
                col_list="msisdn,call_from_cli,appname,packname,btype,stype,status,subscription_status_code,wcflag,userbaseid,server_id,main_id,outdialer_id,null,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,platform,vendorname,device,pool,pack_value,null,null,null,null,null,null,null,null,null,keypressed1,keypressed2,null,null,null,null,null,null,null,content_type_id,null,failure_isdn_reason_id,failure_reason_id,vcode,failure_isdn_reason,failure_reason,ivr_response,song_selected,time_stamp,producttype,charging_rate_id"
                ;;

tata_docomo)
                operator="tata_docomo"
                circles="11 12 13 14 15 16 17 18 19 20 21"
                col_list="msisdn,call_from_cli,appname,packname,null,null,status,subscription_status_code,wcflag,null,server_id,main_id,outdialer_id,circleappid,central_app_id,call_initiate_time,call_start_time,call_end_time,call_duration,null,null,device,pool,null,null,null,filename,maxtime,missingclip,sattempts,null,null,null,keypressed1,keypressed2,keypressed3,null,null,null,null,null,null,content_type_id,log_type_id,failure_isdn_reason_id,failure_reason_id,vcode,null,null,ivr_response,null,time_stamp,null,charging_rate_id"
                ;;
              
airtel_iffco)
                operator="airtel_iffco"
                circles="1 3 4 5 6 7 8 9 11 12 13 14 15 16 17 18"
                col_list="msisdn ,callfromcli ,servicename ,null,null,null,status ,null,null,null,server_id ,null,outdialer_id ,null,centralappid ,null,starttime ,endtime ,callduration ,null,null,null,null,null,null,null,null,max_times ,null,null,null,null,null,null,null,null,param1 ,param2 ,param3 ,param4 ,param5 ,param6 ,null,null,null,null,null,null,null,null,null,null,null,null"
                ;;
                
roshan)
                operator="roshan"
                circles="10"
                col_list="msisdn ,call_from_cli ,appname ,null,null,null,status ,subscription_status_code ,wcflag ,baseresetflag ,server_id ,main_id ,outdialer_id ,connect_time ,central_app_id ,null,start_time ,end_time ,call_duration ,null,vendorname ,device ,pool ,null,eup ,langkey ,null,null,null,null,busyornotanswer ,null,null,keypressed1 ,keypressed2 ,null,param1 ,param2 ,param3 ,param4 ,param5 ,param6 ,null,null,null,null,vcode ,failure_isdn_reason ,failure_reason ,null,song_selected ,null,null,null"
                ;;
                
        *)      echo "Wrong operator"
esac

date >> $LOG
echo "monyear is $monyear" >> $LOG
echo "operator is $operator" >> $LOG
echo "month is $month" >> $LOG
echo "col_list is : $col_list" >> $LOG

### Continue only if user agrees
case $# in
"3")
    echo "monyear is $monyear"
    echo "operator is $operator"
    echo "month is $month"
    #echo "col_list:"
    #echo $col_list
    echo "files are:"
    for i in `ls ${path}/*.gz`
    do
        echo $i
    done

    echo "Continue?"
    read answer
    case $answer in
    y*|Y*)
        obd_loader
        #overwrite_obd $operator
        ;;
    n*|N*)
        echo "Exiting"
        exit $FAIL
        ;;
    *)
        echo "Exiting"
        exit $FAIL
    esac
    ;;

"2")
    echo "monyear is $monyear"
    echo "operator is $operator"
    echo "month is $month"
    echo "col_list:"
    echo $col_list
    echo "circles:"
    echo $circles
    echo "Continue?"
    read answer
        case $answer in
            y*|Y*)
                overwrite_obd $operator
            ;;
            *)
            echo "Exiting!"
            exit $FAIL
            ;;
        esac
    ;;
"*")
    echo "Expection in argument list!"
esac



echo "Completed for $operator and for monyear $monyear"
echo "Completed for $operator and for monyear $monyear" >> $LOG

