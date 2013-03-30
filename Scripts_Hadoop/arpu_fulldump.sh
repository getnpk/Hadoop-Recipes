#!/bin/sh


FAIL=99

user='root'
pass=''
db='dw_arpu40'
operator=''


if [ $# -eq "3" ]; then
    operator=$1
    db=$2
    dump_dir=$3
else
    echo "Usage: <script> <operator> <db> <dump_dir>"
    exit $FAIL
fi

mkdir -p $dump_dir
chmod -R 777 $dump_dir

table_list=`mysql -u${user} --password=${pass} $db --skip-column-names -e "show tables like '%log_%_%20%'"`

echo "Operator: $operator"
echo "Database: $db"
echo "Dump_dir: $dump_dir"
echo ""

for table in $table_list
do
echo "Current table $table"
mysql -u${user} --password=${pass} $db -e "select * from $table into outfile '$dump_dir/${operator}_${table}.csv' fields terminated by ',' lines terminated by '\r\n'"
gzip $dump_dir/${operator}_${table}.csv
done

echo "Done!"

