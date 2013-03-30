__author__="nitin p kumar"

import os
import sys
import paramiko
import pexpect


server_list=("hadoopserverexample",)

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

def scp_query(file,source_path):
    try:
        for server in server_list:
            query="scp %s hadoop@%s:%s/ "%(file,server,source_path)
            print query
            os.system(query)
    except Exception,e:
        print e

def execute_query(cmd):
    try:
        for server in server_list:
            ssh.connect(server, username='hadoop',password='548853ele')
            stdin, stdout, stderr=ssh.exec_command(cmd)
            data=stdout.readlines()
            for line in data:
                print line.strip('\n')
        ssh.close()

    except Exception,e:
        print e

def main():
    file=sys.argv[1]
    source_path=sys.argv[2]
    print file
    print source_path
    scp_query(file,source_path)
    newline="* 10 * * * java -jar %s/%s"%(source_path,file)
    cmd='(crontab -l; echo "%s") | crontab -'%(newline)
    print(cmd)
    execute_query(cmd)
    per="chmod 755 %s/%s"%(source_path,file)
    print(per)
    execute_query(per)

def run_query(query):
        foo=pexpect.spawn(query)
        foo.close()

if __name__ == "__main__":
    main()


