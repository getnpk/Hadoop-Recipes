
#Aussuming files are inside a folder and of type
#obd_aircel_Tran_OBD_Out_Log_82_Jul2011.csv.gz
#gunzips and load to $operator obd
#no airtel and globacom
#change tables for docomo
#<nitin2.kumar@one97.net>

now=`date | awk '{print $1"_"$2"_"$3"_"$4}'`
LOG="obd_loader.log"

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
        path=$3
        else
            echo "Folder does not exist"
            echo "Usage: obd_loader <operator> <monyear> <path_to_folder>"
            exit $FAIL
        fi
        
else
        echo "Usage: To overwrite: obd_loader <operator> <monyear>"
        echo "Usage: To load (and overwrite): obd_loader <operator> <monyear> <path_to_folder>"
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
#aircel)
#                operator="aircel"
#                circles="70 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96"
#                ;;
#airtel) echo "airtel"
#               operator="airtel"
#               circles="0 50 60 64 65 67 68 70 71 72 73 74 75 76 77 78 79 80"
#               ;;
#bsnl)
#                operator="bsnl"
#                circles=""
#                ;;
idea)
                operator="idea"
                circles="10 11 12 14 20 30 40 50 60 70 80 81 90 91 92 93 94 95 96"
                ;;
mts)
                operator="mts"
                circles="10 20 30 40 50 60 70 80 90 91 92 93 95 96 97"
                col_list="status,outdialer_id,main_id,msisdn,call_duration,pool,device,subscription_status_code,wcflag,keypressed1,keypressed2,vcode,call_from_cli,vendorname,appname,circle_code,server_id,central_app_id,eup,baseresetflag,langkey,param1,param2,param3,param4,param5,param6,call_initiate_time,call_start_time,call_end_time,call_date,failure_reason_id,failure_isdn_reason_id,content_type_id,charging_rate_id,ivr_response_id,time_stamp"
                ;;
uninor)
                operator="uninor"
                circles="10 11 12 13 14 20 30 40 50 60 70 80 90"
		col_list="status,outdialer_id,main_id,msisdn,call_duration,pool,device,subscription_status_code,wcflagtinyint,keypressed1,keypressed2,vcode,call_from_cli,vendorname,appname,circle_code,server_id,central_app_id,eup,baseresetflag,langkey,param1,param2,param3,param4,param5,param6,call_initiate_time,call_start_time,call_end_time,call_date,failure_reason_id,failure_isdn_reason_id,content_type_id,packname,song_selected,time_stamp"
                ;;
videocon)
                operator="videocon"
                circles="13 20 30 40 50 60 70 80"
                ;;
vodafone)
                operator="vodafone"
                circles="11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 41 52 55 58 66 69 75"
                ;;
tata)
                operator="tata"
                circles="10 11 12 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29"
				#raw to obd directly
                ;;
reliance)
                operator="reliance"
                circles="10 20 30 40 50 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76"
		col_list="status ,outdialer_id ,main_id ,msisdn ,start_time ,connect_time ,end_time ,call_duration ,failure_reason ,failure_isdn_reason ,pool ,device ,ivr_response ,subscription_status_code ,wcflag ,keypressed1 ,keypressed2 ,vcode ,song_selected ,call_from_cli ,vendorname ,appname ,circle_code ,server_id ,central_app_id ,packname ,platform ,campign_product ,url_hit_flag ,no_of_time_url_hit ,btype ,stype ,pack_value ,prompt_length ,userbaseid ,producttype ,call_initiate_time ,call_start_time ,call_end_time ,call_date ,failure_reason_id ,failure_isdn_reason_id ,content_type_id ,charging_rate_id ,ivr_response_id ,time_stamp"
                ;;

tata_docomo)
                operator="tata_docomo"
                circles="11 12 13 14 15 16 17 18 19 20 21"
		col_list="status,outdialer_id,main_id,maxtime,msisdn,pool,device,subscription_status_code,call_from_cli,circle_code,server_id,central_app_id,wcflag,appname,packname,vcode,keypressed1,keypressed2,keypressed3,call_initiate_time,call_start_time,call_end_time,call_duration,call_date,failure_reason_id,failure_isdn_reason_id,content_type_id,charging_rate_id,circleappid,ivr_response,missingclip,sattempts,log_type_id,filename,time_stamp"
                ;;
globacom)
                operator="globacom"
                circles=""
                ;;
        *)      echo "Wrong operator"
esac

date >> $LOG
echo "monyear is $monyear" >> $LOG
echo "operator is $operator" >> $LOG
echo "month is $month" >> $LOG


### Continue only if user agrees
case $# in
"3")
    echo "monyear is $monyear"
    echo "operator is $operator"
    echo "month is $month"
    echo "col_list:"
    echo $col_list
    echo "files are:"
    for i in `ls ${path}/*.gz`
    do
        echo $i
    done
    ;;
"2")
    echo "monyear is $monyear"
    echo "operator is $operator"
    echo "month is $month"
    echo "Continue?"
    read answer
        case $answer in
            y|Y|yes|Yes|YES)
                overwrite_obd
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


echo "Continue?"
read answer

case $answer in
y|Y|yes|Yes|YES)
        :
        echo "Overwrite too?"
        read answer
        
        case $answer in
            y|Y|yes|Yes|YES)
                obd_loader
                overwrite_obd
            ;;
            n|N|no|No|NO)
                echo "Only loading."
                obd_loader
            ;;
            *)
                echo "Only loading."
                obd_loader
        esac
n|N|no|No|NO)
        echo "Exiting"
        exit $FAIL
        ;;
*)
        echo "Exiting"
        exit $FAIL
esac



#loads data
function obd_loader(){
    
    cd $path
    for i in `ls *.gz`
    do
        gunzip $i
        myfile=`ls *.csv`
        c=`echo $i | awk -F '_' '{print $7}'`
        echo "Current load file : $myfile"
        hive -e "load data local inpath '$myfile' into table ${operator}_obd partition (circle='$c',monyear='$monyear')" >> $LOG
        gzip $myfile
    done
    echo "Done loading data into table ${operator}_obd"
  
}


#overwrites data
function overwrite_obd(){

    for circle in $circles
    do
        for day in $dates
        do
        echo "Current date : $year-$month-$day"
        hive -e "from ${operator}_obd insert overwrite table ${operator}_obd_deflate partition (circle='$circle', monyear='$monyear', calldate='$year-$month-$day') select $col_list where circle='$circle' and monyear='$monyear' and call_date='$year-$month-$day'" >> $LOG
        done
    done
    echo "Overwriting data done for table ${operator}_obd_deflate"
    echo "Done overwriting!" >> $LOG
   
}


echo "Completed for $operator and for monyear $monyear"
echo "Completed for $operator and for monyear $monyear" >> $LOG
