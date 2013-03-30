'''
custom et created for hadoop on 24
<nitin2.kumar@one97.net>
'''


import sys
import os
import datetime
import time
import re
import MySQLdb
import pexpect
import random


dbase='etl_config_sm_40'
filenames=['*.gz']

if len(sys.argv) <> 5:
        print 'usage python script.py <bu> <operator> <src> <dest>'
        sys.exit()

bu=sys.argv[1]
operator=sys.argv[2]
src=sys.argv[3]
dest=sys.argv[4]


def pullfile(bocs_id,server_ip,user_name,password,sg_ip_one='NA',sg_un_one='NA',sg_pw_one='NA',sg_ip_two='NA',sg_un_two='NA', sg_pw_two='NA'):

    if not os.path.exists(dest):
        os.system("mkdir -p %s" %dest)
        print("Directory created %s" %dest)

    if sg_ip_one == 'NA':

        for filename in filenames:
            try :
                print("scp -r %s@%s:%s/%s  %s" %(user_name,server_ip,src,filename,dest))
                foo5=pexpect.spawn("scp -r %s@%s:%s/%s  %s" %(user_name,server_ip,src,filename,dest))
                foo5.expect(['.ssword:*',pexpect.EOF])
                print("Sending password %s" %(password))
                foo5.sendline("%s" %(password))
                foo5.sendline("\r\n")
                foo5.interact()
                #foo5.expect(pexpect.EOF)
            except Exception,e:
                print("Exception for operator %s!" %operator)
                print(e)
            finally:
                foo5.close()

    #case 2 yet to test
    if sg_un_two=='NA':
        #sg_port_one=sg_ip_one.split('.')[3]+'22'
        sg_port_one=get_port_num()
        #kill any active ports if any
        os.system("/sbin/fuser -k %s/tcp" %(sg_port_one))

        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(sg_ip_one,sg_un_one,sg_port_one,server_ip,sg_ip_one))
            foo9=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(sg_ip_one,sg_un_one,sg_port_one,server_ip,sg_ip_one))
            foo9.expect('.ssword:*')
            print("Sending password %s" %sg_pw_one)
            foo9.sendline("%s" %sg_pw_one)
            #foo1.interact()
            foo9.close()
            print("Connection made to SG1 %s" %sg_ip_one)
        except Exception,e:
            print("Exception for operator %s first connection!" %operator)
            print (e)
            pass

        for filename in filenames:
            try :
                print("spawning")
                print("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(server_ip,sg_port_one,user_name,src,filename,dest))
                foo8=pexpect.spawn("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(server_ip,sg_port_one,user_name,src,filename,dest))
                foo8.expect('.ssword:*')
                print("sending password %s" %password)
                foo8.sendline("%s" %password)
                foo8.sendline("\r\n")
                foo8.interact()
                foo8.close()
            except Exception,e:
                print("Exception for operator %s final scp!" %operator)
                print(e)
                pass

    #case 3 yet to test
    if sg_un_two=='NA' <> 'NA':
        #sg_port_one=sg_ip_one.split('.')[3]+'22'
        #sg_port_two=sg_ip_two.split('.')[3]+'22'
        
        sg_port_one=get_port_num()
        sg_port_two=get_port_num()
        
        #kill active ports if any
        os.system("/sbin/fuser -k %s/tcp" %sg_port_one)
        os.system("/sbin/fuser -k %s/tcp" %sg_port_two)

        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(sg_ip_one,sg_un_one,sg_port_one,sg_ip_two,sg_ip_one))
            foo5=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -f -N -l %s -L %s:%s:22 %s" %(sg_ip_one,sg_un_one,sg_port_one,sg_ip_two,sg_ip_one))
            foo5.expect('.ssword:*')
            print("Sending password %s" %sg_pass_one)
            foo5.sendline("%s\r\n" %sg_pass_one)
            foo5.sendline("\r\n")
            #foo5.interact()
            foo5.close()
            print("Connection made to SG1 %s" %sg_ip_one)
        except Exception,e:
            print("Exception for operator %s first connection!" %operator)
            print (e)
            pass
        try :
            print("/usr/bin/ssh -o HostKeyAlias=%s -p %s -f -N -l %s -L %s:%s:22 localhost" %(sg_ip_two,sg_port_one,sg_un_two,sg_port_two,server_ip))
            foo6=pexpect.spawn("/usr/bin/ssh -o HostKeyAlias=%s -p %s -f -N -l %s -L %s:%s:22 localhost" %(sg_ip_two,sg_port_one,sg_un_two,sg_port_two,server_ip))
            foo6.expect('.ssword:*')
            foo6.sendline("%s\r\n" %sg_pass_two)
            #foo6.interact()
            foo6.close()
            print("Connection made to SG2 %s" %sg_ip_two)
        except Exception,e:
            print("Exception for operator %s second connection!" %operator)
            print (e)
            pass

        for filename in filenames:
            try :
                print("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s  %s" %(server_ip,sg_port_two,user_name,src,filename,dest))
                foo7=pexpect.spawn("/usr/bin/scp -o HostKeyAlias=%s -P %s %s@localhost:%s/%s %s" %(server_ip,sg_port_two,user_name,src,filename,dest))
                foo7.expect('.ssword:*')
                print("Sending password %s" %password)
                foo7.sendline("%s\r\n" %password)
                #foo5.sendline("\r\n")
                foo7.interact()
                foo7.close()
            except Exception,e:
                print("Exception for operator %s final scp!" %operator)
                print(e)
                pass


def mysql_connect(mysql_host="localhost",mysql_port=3306,mysql_un="root",mysql_pw="",mysql_sock="/var/lib/mysql/mysql.sock"):
    try:
        db = MySQLdb.connect(host=mysql_host,port=mysql_port,user=mysql_un,passwd=mysql_pw,unix_socket=mysql_sock)
        return db
    except Exception,e:
        sys.stderr.write('Unable to connect to db %s '%(dbase))
        raise


def mysql_close(db):
    try:
        db.close()
    except:
        sys.stderr.write('Unable to close db connection')


def fun_execute_query(db,dbase,query):
    '''
    executes and returns resultset
    '''
    db.select_db(dbase)
    cursor = db.cursor()
    cursor.execute(query)
    resultset = cursor.fetchall()
    cursor.close()
    db.commit()
    return(resultset)

def get_port_num():
    return random.randint(6000,9000)

if __name__ == "__main__":

    db=mysql_connect()

    #get bocs_id from input
    bquery="select bocs_id from master_bocs where bu='%s' and operator='%s'"%(bu, operator)
    resultset=fun_execute_query(db, dbase, bquery)
    if resultset:
        bocs_id=resultset[0][0]
        print 'bocs_id', bocs_id
    else:
        sys.stderr.write('Unable to find operator or bu\n')
        sys.exit(1)


    fields=('bocs_id','server_ip','user_name','password','sg_ip','sg_un','sg_pw')
    query="select m.bocs_id, s.server_ip,s.user_name, s.password, s.sg_ip, s.sg_un, s.sg_pw from server_config s, master_bocs m \
           where m.server_id = s.server_id and m.bocs_id='%s'"%(bocs_id)
    try:
        resultset=fun_execute_query(db,dbase,query)
        result=resultset[0]
        field_dict=dict(zip(fields,result))
        bocs_id=field_dict['bocs_id']
        server_ip=field_dict['server_ip']
        user_name=field_dict['user_name']
        password=field_dict['password']
        print field_dict
        if field_dict['sg_ip'] == 'NA':
            print 'case one'
            pullfile(bocs_id,server_ip,user_name,password)


        elif field_dict['sg_ip'].find('||') == -1:
            print 'case two'
            sg_ip_one=field_dict['sg_ip']
            sg_un_one=field_dict['sg_un']
            sg_pw_one=field_dict['sg_pw']
            pullfile(bocs_id,server_ip,user_name,password,sg_ip_one,sg_un_one,sg_pw_one)

        else:
            print 'case three'
            sg_ip_one=field_dict['sg_ip'].split('||')[0]
            sg_un_one=field_dict['sg_un'].split('||')[0]
            sg_pw_one=field_dict['sg_pw'].split('||')[0]
            sg_ip_two=field_dict['sg_ip'].split('||')[1]
            sg_un_two=field_dict['sg_un'].split('||')[1]
            sg_pw_two=field_dict['sg_pw'].split('||')[1]
            #pullfile(bocs_id,server_ip,user_name,password,sg_ip_one,sg_un_one,sg_pw_one,sg_ip_two,sg_un_two,sg_pw_two)

    except Exception,e:
        print e
    finally:
        mysql_close(db)


