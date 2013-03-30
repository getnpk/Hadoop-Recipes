
#/usr/local/bin/python central_purging.py "root" "" "localhost" 3306 "/var/lib/mysql/mysql.sock" "etl_config40" 10 "obd" "outlog"

import os, re, MySQLdb, string, time,ConfigParser,sys,datetime,glob
from time import strftime
from string import replace
from datetime import date
import platform
import logging

if __name__ == "__main__":

#################### mysql connection parameters
     mysql_un=sys.argv[1].strip()
     mysql_pw=sys.argv[2].strip()
     mysql_host=sys.argv[3].strip()
     mysql_port=int(sys.argv[4].strip())
     mysql_socket=sys.argv[5].strip()
     dbase=sys.argv[6].strip()

#################### purging parameter
     date_diff_argv=sys.argv[7].strip()
     bu_argv=sys.argv[8].strip()
     product_argv=sys.argv[9].strip()

####################################################################   logging    #########################################################################

     strosname = platform.platform()
     stros = strosname[0:strosname.index("-",1)] # plaform.system()
     
     log_dir=os.path.realpath(os.getcwd()+'/logs/')
     print 'log_dir : ' +log_dir

     try:
         if os.path.exists(log_dir)==0: #create log_type directory if doesnot exists
             mk_log_dir_cmd="mkdir -p "+log_dir
             a=os.system(mk_log_dir_cmd)
             a=os.system('chmod -R 777 log_dir')
         LOG_FILENAME = log_dir + '/central_purging.txt'
         logging.basicConfig(filename=LOG_FILENAME,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S' ,level=logging.INFO, filemode='w',)
     except Exception,e:
         print(e)

#############################
#       connect to database #
#############################
     try:
         logging.info('connecting with database')
         db = MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_socket)
         logging.info('database connection succesful')
     except Exception,e:
         db = MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_socket)
         logging.info('database connection unsuccesful :: %s',str(e))

########################################################
#       function to execute and commit mysql query     #
########################################################
     def fun_execute_query(dbase,query):
         try:

             db.select_db(dbase)
             cursor = db.cursor()
             cursor.execute(query)
             resultset = cursor.fetchall()
             cursor.close()
             db.commit()

             return(resultset)
         except Exception,e:
             logging.info('Exception in running query :: %s - %s',query,str(e))

#############################################################
#       function to delete files on lastmodified date basis #
#############################################################
     def del_files(folderpath,date_diff_argv):
          if os.path.exists(folderpath)<>0:
               os.chdir(folderpath)
               date_diff=datetime.timedelta(days=-int(date_diff_argv))
               logging.info("date_diff: " + str(date_diff))
               del_date=str(date.today() + date_diff)
               logging.info("del_date: " + del_date)
               y1, m1, d1 = (int(x) for x in del_date.split('-'))

               date_file_list = []                                                                                                                          
               for file in glob.glob(folderpath + '/*.*'):
##                    for file in glob.glob(folder + '/*.*'):
                    stats = os.stat(file)
                    lastmod_date = time.strftime("%Y-%m-%d",time.localtime(stats[8]))
                    y2, m2, d2 = (int(x) for x in lastmod_date.split('-'))
                    if (datetime.date(y1,m1,d1)-datetime.date(y2,m2,d2)).days>=0:
                         date_file_tuple = file
                         date_file_list.append(date_file_tuple)                                                                                             
               date_file_list.sort()
               date_file_list.reverse() # newest mod date now first
               if date_file_list==[]:
                    logging.info("folder " + folderpath + " has no files to delete")
               else:
                    for file in date_file_list:
                         folder, file_name = os.path.split(file)
##                         file_date = time.strftime("%Y-%m-%d", file[0])
##                         y2, m2, d2 = (int(x) for x in file_date.split('-'))
##                         if (datetime.date(y1,m1,d1)-datetime.date(y2,m2,d2)).days>=0:
                         logging.info("deleting : " + file_name)
                         os.remove(file_name)
          else:
               logging.info(folderpath + " doesn't exists.")

#######################################

     query="SELECT concat(b.basepath,'/',b.operator,'/',lt.product,'/',lt.log_type) FROM bu_config b, master_bocs m,  map_product_log_type lt \
                        WHERE b.bu=m.bu AND b.operator=m.operator AND m.bocs_id=lt.bocs_id \
                        AND b.bu=\""+bu_argv+"\" AND b.product=\""+product_argv+"\""
     logging.info(query)

     data_path=fun_execute_query(dbase,query)

     try:
          for data in data_path:
               delete_path_merge=str(data[0].strip())+'/merge'
               logging.info("delete_path_merge: " + delete_path_merge)
               delete_path_cf=str(data[0].strip())+'/cf'
               logging.info("delete_path_cf: " + delete_path_cf)
               try:
                    del_files(delete_path_merge,date_diff_argv)
                    del_files(delete_path_cf,date_diff_argv)
               except Exception,e:
                    errorMessage = "path not found" + str(e)
     except Exception,e:
          errorMessage = "Error while loading: " + str(e)
          logging.info("errorMessage: " + errorMessage)
     logging.info("$$$  SUCCESS  $$$")
