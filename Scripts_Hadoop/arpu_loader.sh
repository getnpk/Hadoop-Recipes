#
#loads arpu files to the corresponding text tables
#<nitin2.kumar@one97.net>

FAIL=99
LOG="arpu.log"

if [ -n "$1" ]; then
    path=$1
else
    path=$PWD
fi

function printFiles(){
    echo "Files to load:"
    for f in `ls $path/*.gz`
    do
    echo $f
    done
}

function arpuLoad(){
    for f in `ls $path/*.gz`
    do
        operator=`echo $f | awk -F '_' '{print $2}'`
        application=`echo $f | awk -F '_' '{print $4}'`
        logtype=`echo $f | awk -F '_' '{print $5}'`
        monyear=`echo $f | awk -F '_' '{print $7}' | awk -F '.' '{print tolower($1)}'`
        
        echo "gunzipping $f"
        gunzip $f
        myfile=`ls *.csv`
        echo "hive -e \"load data local inpath '$myfile' into table arpu_${logtype}_text partition (operator='$operator', application='$application', monyear='$monyear')\""
        hive -e "load data local inpath '$myfile' into table arpu_${logtype}_text partition (operator='$operator', application='$application', monyear='$monyear')"  >> $LOG
        echo "gzip *.csv"
        gzip *.csv
    done    
}


###main###
printFiles
echo "Continue?"
read answer
case $answer in
    y*|Y*)
    arpuLoad
    ;;
    *)
    echo "Exiting!"
    exit $FAIL
    ;;
esac

echo "done loading!"