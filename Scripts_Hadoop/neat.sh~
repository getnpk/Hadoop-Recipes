####################################
#Tips and commands                 #
#<nitin2.kumar@one97.net>          #
####################################

#alias#
alias today='date +"%A, %B %-d, %Y"' #Tuesday, October 18, 2011

#awk#
awk -F '_' '{print tolower($3)}'
c=`echo $i | awk 'BEGIN{FS="_"}{for (i=1; i<=NF; i++) if (i==NF-1) print $i}'` # get the second last wordsplit

hms=`echo $time | awk '{split($0,a,":"); print a[1], a[2], a[3]}'`
c=`echo $i | awk 'BEGIN{FS="_"}{for (i=1; i<=NF; i++) if (i==NF-1) print $i}'` # NF contains total number of splits

text=`echo $text | awk '{sub("shower","stream"); print $0}'` # substitution only first
text1=`echo $text | awk '{gsub("a[a-z]","x"); print $0}'` # global, a followed by an alpha replaced by x
text2=`echo $text | awk '{sub("a*d","tt"); print $0}'` # a followed by anything till d, replace d with tt

catalogue=`echo $name | awk '{sub("[0-9]+",""); print $0}'` # first occurance removes numbers
short=`echo $name | awk '{gsub("[b-z]",""); print $0}'` # global removes all from b to z and replace with ''

object=`echo $caption | awk '{print substr($0,12,8)}'` # substring start at 12 and i need 2 chars

awk 'NR < 11' # number of lines read in a file

#bc#
echo 2*30/3 | bc #20 calculators

#excel#
=VLOOKUP(A:A,Sheet1!A:A,1,0)

#ctime,cmin#
find . -type d -cmin +2 -exec rm -rf {} \; # delete data two minutes or older
find /path/ -type f -ctime +10 -exec ls {} \; # delete data 10 days ago

#cut#
year=`echo $monyear | cut -c 4-` # all after 6th
m=`echo $monyear | cut -c -3` # all before 3rd
cut -c4,8 data # display 4th and 8th
cut -d'_' -f3 data

#crontab#
*     *     *   *    *        command to be executed
-     -     -   -    -
|     |     |   |    |
|     |     |   |    +----- day of week (0 - 6) (Sunday=0)
|     |     |   +------- month (1 - 12)
|     |     +--------- day of        month (1 - 31)
|     +----------- hour (0 - 23)
+------------- min (0 - 59)

#date#
date +"%b-%d-%Y" #Oct-18-2011
date +"%m-%d-%Y" #10-18-2011
date +"%m-%d-%y" #10-18-11
date +'%b%Y' -d$mm  #prints Sep2011 mm='20110905'
date --date='1 day ago' # day, week, month
date +'%H:%M:%S' -d '13:00:00'
date --date='next Friday'

#dnstop
Installation: http://bash.cyberciti.biz/software-build-scripts/displays-various-tables-of-dns-server-traffic/
Documentation: http://dns.measurement-factory.com/tools/dnstop/dnstop.8.html
dnstop -l 6 eth0 # start in level six; ^ to display full info with hostnames

#dmidecode#
/usr/sbin/dmidecode -t system # system info

#excel
=VLOOKUP(A:A,Sheet2!A:A,1,0)

#export
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`hbase classpath`

#expr
sum=0
for i in `hadoop fs -dus /hive/warehouse/outlog_obd/*/*/monyear=jan2012/calldate=2012-01-03 | awk '{print $2}'`
do
    sum=`expr $sum + $i`
done
size_mb=`expr $sum / 1024 / 1024`
size_gb=`expr $sum / 1024 / 1024 / 1024`
extra=`expr $size_gb \* 1000`
echo "Size $size_gb GB `expr $size_mb % $extra` MB"

#find

find /var/log/hadoop/ -type f -mtime +3 -name "hadoop-hadoop-*" -delete #delete 3 days ago -cmin for mins
find /var/log/hadoop/ -type f -mtime +50 -name "job_*_conf.xml" -exec ls {} \;

#fuser
/sbin/fuser -k 2356/tcp # kill process running on this port

#grep#
cat nohup.out | grep failed -B 3 #before match
cat nohup.out | grep failed -A 3 #after
cat nohup.out | grep failed -C 3 #both ways

#gzip

gzip -t file.gz # check if gzip file is valid

#hadoop
hadoop fs -stat '%n %F %o %r %b %y' /hive/warehouse/active_vcode/a.csv # name file/dir blocksize replicaton filesize timestamp
hadoop fs -ls file:/backup/nitin/ # local fs
hive> ! ls # local fs

#hive#
set mapred.job.priority=VERY_HIGH;
describe formatted <tablename>
hive -f hive-queries.txt # running hive queries file
nohup hive --service hiveserver & # starting hiveserver
hive -hiveconf hive.root.logger=INFO,console #dump info output to screen

#iptables#
iptables -I INPUT \! --src 0.0.0.0 -m tcp -p tcp --dport 10000 -j ACCEPT #accept (all) (DROP)
#jar
jar -tf some.jar
jar cf jar-file input-file(s) # create jar
java -jar app.jar# run app

#lsof
/usr/sbin/lsof -Pnl +M -i4 # ipv4 port listing

#mount
sudo mkdir /media/iso 
sudo modprobe loop
sudo mount filename.iso /media/iso -t iso9660 -o loop #mounting an iso

#mysql
use mysql;
select Host,User,Password from user;
CREATE USER 'hive'@'10.0.3.133' IDENTIFIED BY 'ujgs';
GRANT select ON *.* TO 'hive'@'10.0.3.133';
grant all privileges on *.* TO 'hive'@'10.0.3.133';
flush privileges;

SELECT a.val1, a.val2, b.val, c.val FROM a JOIN b ON (a.key = b.key) LEFT OUTER JOIN c ON (a.key = c.key)

alter table successlog_service add time_stamp timestamp not null default CURRENT_TIMESTAMP;

SELECT CONCAT(table_schema, '.', table_name),
       CONCAT(ROUND(table_rows / 1000000, 2), 'M')                                    rows,
       CONCAT(ROUND(data_length / ( 1024 * 1024 * 1024 ), 2), 'G')                    DATA,
       CONCAT(ROUND(index_length / ( 1024 * 1024 * 1024 ), 2), 'G')                   idx,
       CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2), 'G') total_size,
       ROUND(index_length / data_length, 2)                                           idxfrac
FROM   information_schema.TABLES
ORDER  BY data_length + index_length DESC
LIMIT  10; # shows top 10 tables by size in format <CONCAT(table_schema, '.', table_name) | rows   | DATA   | idx   | total_size | idxfrac>

mysql -udwh -p'!dwh197$' --socket='/db/archival2/mysql.sock'

#netstat# 
netstat -plten | grep  10000 #check all active ports now 10000

#paramiko
http://segfault.in/2010/03/paramiko-ssh-and-sftp-with-python/
http://jessenoller.com/2009/02/05/ssh-programming-with-paramiko-completely-different/

#putty forwarding
tata@10.0.1.173 22 # user@ip port
L4922   <next_ip:22> # source_port IP:22
root@localhost  4922
L4822   <next_ip:22>
root@localhost 4822

taskkill  /f  /im  "putty.exe" # kill all putty

#sed#
sed 's/\"\,\"/||/g' one.txt > two.txt
sed 1,1d hello > out # delete lines in the range 1 to 1 or del first line
sed -e '3,5d' # deletes lines 3 to 5
sed -e '7,11!d' # delete all except 7 to 11

sed -n '2,$p' hello > newfile # same as above but selection till end

sed -e '7a\ # append after line 7, use i for before.
an extra line.\
another one.\
one more.'

cat testout | sed -e '/one/,/three/d' # delete lines starting from one to three
sed -e '/Love Jane/i\ # this is like insert between
Love Carol\
Love Beth'

#split
split -l 60000 access.log #split file based on lines

#threading
http://www.tutorialspoint.com/python/python_multithreading.htm

#tr#
echo 'linux' | tr "[:lower:]" "[:upper:]"
echo ${confirmation} | tr 'A-Z' 'a-z'

a=`echo $var | tr -d "[0-9]"` #check if number
    if [[ -z $a ]]; then
            echo "Ya number"
    fi

[[:alpha:]] is the same as [a-zA-Z]
[[:upper:]] is the same as [A-Z]
[[:lower:]] is the same as [a-z]
[[:digit:]] is the same as [0-9]
[[:alnum:]] is the same as [0-9a-zA-Z]
[[:space:]] matches any white space including tabs

#vnstat
http://webhostingneeds.com/Install_vnstat


#wget

wget \
     --recursive \
     --no-clobber \
     --page-requisites \
     --html-extension \
     --convert-links \
     --restrict-file-names=windows \
     --domains website.org \
     --no-parent \
         www.website.org/tutorials/html/
 
 --limit-rate=200K




mysql -uroot -pvodafone23

10.0.3.133  hadoop@197

===============================================================
\\10.0.10.114\Softwares

hive -e "select b from (select o.msisdn a, t.msisdn b from outlog_obd o right outer join temp_obdbase t where o.operator='vodafone'
and o.circle=11 and o.monyear in ('feb2012','jan2012','dec2011') and o.status='SU' and o.subscription_status_code=1) s  where a!=NULL"

hive -e "select b from (select o.msisdn a, t.msisdn b from outlog_obd o right outer join temp_obdbase t where o.operator='vodafone'
and o.circle=11 and o.monyear in ('feb2012','jan2012','dec2011') and o.status='SU' and o.subscription_status_code=1) s  where a!=NULL"

hive -e "insert overwrite table msisdn_notsub_djf select a from (select t.msisdn a, o.msisdn b from temp_obdbase t left outer join temp_out_msisdn o ) s where b!=NULL"

hive -e "select a from (select t.msisdn a, o.msisdn b from temp_obdbase t left outer join temp_out_msisdn o ) s where b!=NULL"

select a from (select t.msisdn a, o.msisdn b from temp_obdbase t left outer join temp_out_msisdn o ) s where b!=NULL;

ALTER TABLE table_name SET TBLPROPERTIES table_properties ('fileformat':'textfile')
table_properties: (property_name = property_value, property_name = property_value, ... )

alter table tablename set fileformat textfile