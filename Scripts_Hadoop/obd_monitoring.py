"""
nitin2.kumar@one97.net
"""

import sys
import os
import MySQLdb
import datetime
import paramiko
import pexpect
import commands
import time

class et:

    #case 1 for all and direct central server connectivity
    use_case={'aircel':'1',\
              'idea':'1',\
              'reliance':'1',\
              'uninor':'2',\
              'videocon':'2',\
              'mts':'2',\
              'bsnl':'2',\
              'tata':'2',\
              'airtel_north':'1',\
              'airtel_south':'1',\
              'airtel_iffco':'2',\
              'vodafone':'2'}

    server_ip={'aircel':'58.68.109.190',\
               'idea':'112.110.32.185',\
               'reliance':'220.226.188.21',\
               'tata':'172.17.104.48',\
               'uninor':'10.64.4.152',\
               'videocon':'10.64.10.178',\
               'mts':'10.130.0.186',\
               'bsnl':'192.168.1.4',\
               'airtel_north':'10.2.96.184',\
               'airtel_south':'10.49.36.21',\
               'airtel_iffco':'10.91.3.152',\
               'vodafone':'10.215.9.13'}

    user_ip={'aircel':'aircel',\
             'idea':'idea',\
             'reliance':'reliance',\
             'tata':'root',\
             'uninor':'uninor',\
             'videocon':'videocon',\
             'mts':'mts',\
             'bsnl':'bsnl',\
             'airtel_north':'airtel',\
             'airtel_south':'airtel',\
             'airtel_iffco':'airtel',\
             'vodafone':'vodafone'}

    pass_ip={'aircel':'aircel@197',\
             'idea':'idea@197',\
             'reliance':'reliance@197',\
             'tata':'redhat',\
             'uninor':'uninor@197',\
             'videocon':'videocon@197',\
             'mts':'mts@197',\
             'bsnl':'bsnl@197',\
             'airtel_north':'airtel@197',\
             'airtel_south':'airtel@197',\
             'airtel_iffco':'airtel@197',\
             'vodafone':'vodafone@197'}

    source_path={'aircel':'/home/aircel',\
                 'idea':'/home/idea',\
                 'reliance':'/home/reliance',\
                 'tata':'/home/tata',\
                 'uninor':'/home/uninor',\
                 'videocon':'/home/videocon',\
                 'mts':'/home/mts',\
                 'bsnl':'/home/bsnl',\
                 'airtel_north':'/home/airtel_north',\
                 'airtel_south':'/home/airtel_south',\
                 'airtel_iffco':'/home/airtel_iffco',\
                 'vodafone':'/home/vodafone'}

    dest_path={'aircel':'/backup/data/obd/aircel',\
               'idea':'/backup/data/obd/idea',\
               'reliance':'/backup/data/obd/reliance',\
               'tata':'/backup/data/obd/tata',\
               'uninor':'/backup/data/obd/uninor',\
               'videocon':'/backup/data/obd/videocon',\
               'mts':'/backup/data/obd/mts',\
               'bsnl':'/backup/data/obd/bsnl',\
               'airtel_north':'/backup/data/obd/airtel_north',\
               'airtel_south':'/backup/data/obd/airtel_south',\
               'airtel_iffco':'/backup/data/obd/airtel_iffco',\
               'vodafone':'/backup/data/obd/vodafone'}

    #case 3 for two server gateways.
    sg_ip_one={'tata':'10.0.1.173',\
               'uninor':'10.64.4.148',\
               'videocon':'180.214.158.155',\
               'mts':'202.78.174.28',\
               'bsnl':'218.248.80.123',\
               'airtel_north':'117.99.128.51',\
               'airtel_south':'117.99.128.51',\
               'airtel_iffco':'59.145.145.211',\
               'vodafone':'203.199.126.79'}

    sg_ip_two={'tata':'172.16.2.49',\
               'airtel_north':'10.2.29.67',\
               'airtel_south':'10.2.29.67'}

    sg_un_one={'tata':'dwh',\
               'uninor':'unigate',\
               'videocon':'datacom',\
               'mts':'administrator',\
               'bsnl':'administrator',\
               'airtel_north':'administrator',\
               'airtel_south':'administrator',\
               'airtel_iffco':'root',\
               'vodafone':'pinkfloyd'}

    sg_un_two={'tata':'root',\
               'airtel_north':'puttyuser',\
               'airtel_south':'puttyuser'}

    sg_pass_one={'tata':'dwh@197',\
                 'uninor':'un1g@te@971',\
                 'videocon':'datacom@197',\
                 'mts':'mts@lockpicking',\
                 'bsnl':'f0rever0ne197',\
                 'airtel_north':'raj@one97',\
                 'airtel_south':'raj@one97',\
                 'airtel_iffco':'lockpicking',\
                 'vodafone':'Creativity'}

    sg_pass_two={'tata':'lockpicking',\
                 'airtel_north':'secured!197',\
                 'airtel_south':'secured!197'}

    port_one={'tata':'1173',\
              'uninor':'5147',\
              'videocon':'5499',\
              'mts':'7427',\
              'bsnl':'8013',\
              'airtel_north':'2971',\
              'airtel_south':'2978',\
              'airtel_iffco':'3878',\
              'vodafone':'5789'}

    port_two={'tata':'6773',\
              'airtel_north':'6186',\
              'airtel_south':'3678'}



############ display details #################
def display_details(myet,operator):
    print("Operator : %s" %operator)
    print("Server IP : %s" %myet.server_ip[operator])
    print("Username : %s" %myet.user_ip[operator])
    print("Password : %s" %myet.pass_ip[operator])
    print("Source path : %s" %myet.source_path[operator])
    print("Destionation path : %s" %myet.dest_path[operator])


########### connect to mysql ######################
def mysql_connect(mysql_host='localhost',mysql_port=3306,mysql_un='root',mysql_pw='',mysql_sock='/var/lib/mysql/mysql.sock'):
    try:
        db=MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_sock)
        return db
    except Exception ,e:
        raise

############## Mysql Disconnect ###################
def mysql_close(db):
    try:
        db.close()
    except Exception, e:
        raise

################ Mysql query Execute ###############
def execute_query(db,dbase,query):
    db.select_db(dbase)
    cursor = db.cursor()
    cursor.execute(query)
    resultset = cursor.fetchall()
    cursor.close()
    db.commit()
    return(resultset)

################ Run ssh querry ####################
def execute_cmd(myet,operator,cmd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(myet.server_ip[operator], username=myet.user_ip[operator],password=myet.pass_ip[operator])
    stdin, stdout, stderr=ssh.exec_command(cmd)
    #stdin.write('hadoophive\n')
    #data=stdout.readlines()
    #fh = open('','w')
    #for line in data:
    #    f.write(line.strip('\n'))
    ssh.close()
    return 0


############ Pull data from remote servers #################
def pull(myet,operator,filename="*_count.txt"):

    if not os.path.exists(myet.dest_path[operator]):
        os.system("mkdir -p %s" %myet.dest_path[operator])
        print("Directory created %s" %myet.dest_path[operator])


    if myet.use_case[operator]=='1':

        display_details(myet,operator)

        try :
            print("scp -r %s@%s:%s/%s  %s" %(myet.user_ip[operator],myet.server_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo5=pexpect.spawn("scp -r %s@%s:%s/%s  %s" %(myet.user_ip[operator],myet.server_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo5.expect('.ssword:*')
            print("Sending password %s" %(myet.pass_ip[operator]))
            foo5.sendline("%s" %(myet.pass_ip[operator]))
            foo5.interact()
            foo5.close()
        except Exception,e:
            print("Exception for operator %s!" %myet.user_ip[operator])
            print(e)
            pass

    #case 2
    if myet.use_case[operator]=='2':
        #kill any active ports if any
        os.system("/sbin/fuser -k %s/tcp" %myet.port_one[operator])
        #display et details
        display_details(myet,operator)

        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(myet.sg_ip_one[operator],myet.sg_un_one[operator],myet.port_one[operator],myet.server_ip[operator],myet.sg_ip_one[operator]))
            foo9=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(myet.sg_ip_one[operator],myet.sg_un_one[operator],myet.port_one[operator],myet.server_ip[operator],myet.sg_ip_one[operator]))
            foo9.expect('.ssword:*')
            print("Sending password %s" %myet.sg_pass_one[operator])
            foo9.sendline("%s" %myet.sg_pass_one[operator])
            #foo1.interact()
            foo9.close()
            print("Connection made to SG1 %s" %myet.sg_ip_one[operator])
        except Exception,e:
            print("Exception for operator %s first connection!" %operator)
            print (e)
            pass

        try :
            print("spawning")
            print("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(myet.server_ip[operator],myet.port_one[operator],myet.user_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo8=pexpect.spawn("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(myet.server_ip[operator],myet.port_one[operator],myet.user_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo8.expect('.ssword:*')
            print("sending password %s" %myet.pass_ip[operator])
            foo8.sendline("%s" %myet.pass_ip[operator])
            foo8.interact()
            foo8.close()
        except Exception,e:
            print("Exception for operator %s final scp!" %operator)
            print(e)
            pass

    #case 3
    if myet.use_case[operator]=='3':
        #kill active ports if any
        os.system("/sbin/fuser -k %s/tcp" %myet.port_one[operator])
        os.system("/sbin/fuser -k %s/tcp" %myet.port_two[operator])
        #display et details
        display_details(myet,operator)

        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(myet.sg_ip_one[operator],myet.sg_un_one[operator],myet.port_one[operator],myet.sg_ip_two[operator],myet.sg_ip_one[operator]))
            foo5=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(myet.sg_ip_one[operator],myet.sg_un_one[operator],myet.port_one[operator],myet.sg_ip_two[operator],myet.sg_ip_one[operator]))
            foo5.expect('.ssword:*')
            print("Sending password %s" %myet.sg_pass_one[operator])
            foo5.sendline("%s\r\n" %myet.sg_pass_one[operator])
            foo5.sendline("\r\n")
            #foo5.interact()
            foo5.close()
            print("Connection made to SG1 %s" %myet.sg_ip_one[operator])
        except Exception,e:
            print("Exception for operator %s first connection!" %operator)
            print (e)
            pass
        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -p %s -f -N -l %s -L %s:%s:22 localhost" %(myet.sg_ip_two[operator],myet.port_one[operator],myet.sg_un_two[operator],myet.port_two[operator],myet.server_ip[operator]))
            foo6=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -p %s -f -N -l %s -L %s:%s:22 localhost" %(myet.sg_ip_two[operator],myet.port_one[operator],myet.sg_un_two[operator],myet.port_two[operator],myet.server_ip[operator]))
            foo6.expect('.ssword:*')
            foo6.sendline("%s\r\n" %myet.sg_pass_two[operator])
            #foo6.interact()
            foo6.close()
            print("Connection made to SG2 %s" %myet.sg_ip_two[operator])
        except Exception,e:
            print("Exception for operator %s second connection!" %operator)
            print (e)
            pass

        try :
            print("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(myet.server_ip[operator],myet.port_two[operator],myet.user_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo7=pexpect.spawn("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s %s" %(myet.server_ip[operator],myet.port_two[operator],myet.user_ip[operator],myet.source_path[operator],filename,myet.dest_path[operator]))
            foo7.expect('.ssword:*')
            print("Sending password %s" %myet.pass_ip[operator])
            foo7.sendline("%s\r\n" %myet.pass_ip[operator])
            #foo5.sendline("\r\n")
            foo7.interact()
            foo7.close()
        except Exception,e:
            print("Exception for operator %s final scp!" %operator)
            print(e)
            pass


if __name__=="__main__" :

    operator=sys.argv[1]
    monyear=sys.argv[2]

    cmd="nohup sh /home/%s/dump_counts.sh %s &" %(operator, monyear)
    
    myet=et()
    
    print cmd
    
    try:
        runcheck = execute_cmd(myet,operator,cmd)
    except Exception,e:
        print("cannot run querry")
    time.sleep(3600)
    if (runcheck == 0):
        filename="%s_count.txt" %(operator)
        pull(myet,operator,filename)
    
    print("Done!")

