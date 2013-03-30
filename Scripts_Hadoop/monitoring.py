#!/usr/bin/env python

import os
import sys
import commands

# to be used for monitoring obd data.
# nitin2.kumar@one97.net

mondict={'jan':'31','feb':'28','mar':'31','apr':'30','may':'31','jun':'30','jul':'31','aug':'31','sep':'30','oct':'31','nov':'30','dec':'31'}

def get_circlelist(operator):
    i=0
    circles=[]
    circle=''

    cmd="hadoop fs -ls /hive/warehouse/outlog_obd/operator=%s/ | awk '{print $8}' | awk -F '/' '{print $6}' | awk -F '=' '{print $2}'"%(operator)
    status,output=commands.getstatusoutput(cmd)

    for f in output:
        if f == '\n':
            continue
        else:
            circle+=f
            i+=1
            if i == 2:
                circles.append(circle)
                circle=''
                i=0
    return circles

def get_count(operator, monyear,circlelist):
        mon=monyear[:3]
        req_days=int(mondict[mon])+1

        # take care of leap years
        if mon == 'feb':
                year=int(monyear[3:])
                if ( ( year % 400 == 0 ) or ( year % 100 != 0 and year % 4 == 0)):
                        req_days=30
                else:
                        req_days=29

        for circle in circlelist:
                cmd="hadoop fs -ls /hive/warehouse/outlog_obd/operator=%s/circle=%s/monyear=%s | wc -l"%(operator,circle,monyear)
                status,output=commands.getstatusoutput(cmd)
                if status == 0:
                        if output == str(req_days):
                                print '%s is okay!'%circle
                        else:
                                try:
                                        print '%s is not okay : missing records %d'%(circle,(req_days-int(output)))
                                except Exception,e:
                                        print 'Dont have anything for %s :('%circle

def check_args(operator,monyear):
        cmd="hadoop fs -ls /hive/warehouse/outlog_obd/operator=%s/*/monyear=%s"%(operator, monyear)
        status,output=commands.getstatusoutput(cmd)
        if status == 0:
                return True
        else:
                return False

if __name__=='__main__':
        if len(sys.argv) <> 3:
                print 'usage: monitoring.py <operator> <monyear>'
                sys.exit(1)

        operator=sys.argv[1].lower().strip()
        monyear=sys.argv[2].lower().strip()

        if check_args(operator, monyear):
                print 'Checking for operator %s ..'%operator
                circlelist=get_circlelist(operator)
                get_count(operator, monyear,circlelist)
        else:
                print 'Invalid operator or monyear!'
