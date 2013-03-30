#Aussuming that the files are as follows
#USSD_CSV_DATEWISE_20110829/01-06-2011/clean3_cdr.txt_01_06_00_00_00_125.sql.csv.gz
#folders="USSD_CSV_DATEWISE_20110829  USSD_DATE_WISE USSD_DATE_WISE1"

__author__="nitin p kumar"

FAIL="99"
time=`echo $(date) | awk '{print $4}'`
LOG="ussd_logger_${time}"


if [ $# -eq "0" ]; then
        echo "Usage: ussd_loader <pass folder paths as args>"
        exit $FAIL
else
        folders=$*
        for t in $folders
        do
                if [ -d $t ]; then
                :
                else
                echo "Folder does not exist"
                echo "Usage: ussd_loader <pass folder paths as args>"
                exit $FAIL
                fi
        done
fi


###to get the new ip as last argument - not used
echo "Argument count = $#"
if [ $# -gt 0 ]
then
   eval last_arg=\$$#
else
   last_arg='default_ip'
fi
thisip=$last_arg


for folder in $folders
do
        for i in `ls $folder`
        do
                        #delete folders of zero size from the given folders
                        find ${folder}/${i}/ -size 0 -exec rm -rf {} \;

                        ### rename files
                        for j in `ls $folder/$i`
                        do

                        echo "file $j"
                        temp=`echo $j | tr ":" "_"`
                        new_name=`echo $temp | tr "-" "_"`

                        mv $folder/$i/$j $folder/$i/$new_name

                        done
        done
done

echo "files of size 0 removed!"
echo "renaming of files done!"


for folder in $folders
do
        for i in `ls $folder`
        do
                d=`echo $i | awk -F '-' '{print $1}'`
                m=`echo $i | awk -F '-' '{print $2}'`
                y=`echo $i | awk -F '-' '{print $3}'`

                echo "$d-$m-$y"
                if [ $m = '01' ]; then
                        monyear="jan2011"
                elif [ $m = '02' ]; then
                        monyear="feb2011"
                elif [ $m = '03' ]; then
                        monyear="mar2011"
                elif [ $m = '04' ]; then
                        monyear="apr2011"
                elif [ $m = '05' ]; then
                        monyear="may2011"
                elif [ $m = '06' ]; then
                        monyear="jun2011"
                elif [ $m = '07' ]; then
                        monyear="jul2011"
                elif [ $m = '08' ]; then
                        monyear="aug2011"
                elif [ $m = '09' ]; then
                        monyear="sep2011"
                elif [ $m = '10' ]; then
                        monyear="oct2011"
                elif [ $m = '11' ]; then
                        monyear="nov2011"
                elif [ $m = '12' ]; then
                        monyear="dec2011"
                else
                        echo "Exception!"
                fi

                for j in `ls ${folder}/${i}/*.gz`
                do
                        echo "$i #date folder"
                        echo "$j #gz file"
                        gunzip $j
                        lf=`ls ${folder}/${i}/*.csv`

                        #sed 's/||/|/g' $lf > done_$lf
                        hive -e "load data local inpath '$lf' into table ussd_text partition (operator='tata', monyear='$monyear', starttime='2011${m}${d}',ip='10.124.157.17')"
                        gzip $lf

                done
        done
done

