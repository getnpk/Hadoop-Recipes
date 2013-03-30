a=`netstat -plten | grep 10000 | awk '{print $9}' | awk -F "/" '{print $1}'`
if [ $a -gt 0  ]; then
 echo  "present" 
else 
	cd ~/scripts/
	nohup hive --service hiveserver &
fi
