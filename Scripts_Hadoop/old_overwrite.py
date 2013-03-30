"""
nitin2.kumar@one97.net
"""

import os
import sys
import commands


def get_circlelist(operator):
    i=0
    circles=[]
    circle=''

    cmd="hadoop fs -ls /hive/warehouse/%s_obd/ | awk '{print $8}' | awk -F '=' '{print $2}'"%(operator)
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


def get_monyearlist(operator,circle):
    i=0
    monyears=[]
    monyear=''

    cmd="hadoop fs -ls /hive/warehouse/%s_obd/circle=%s | awk '{print $8}' | awk -F '=' '{print $3}'"%(operator,circle)
    status,output=commands.getstatusoutput(cmd)

    for f in output:
        if f == '\n':
            continue
        else:
            monyear+=f
            i+=1
            if i == 7:
                monyears.append(monyear)
                monyear=''
                i=0
    return monyears


def main():
    operator=sys.argv[1]
    list = get_circlelist(operator)
    print list

    print get_monyearlist(operator,10)
    col_list='status ,outdialer_id ,main_id ,maxtime ,msisdn ,pool ,device ,subscription_status_code,call_from_cli ,circle_code ,server_id ,central_app_id ,wcflag ,appname ,packname ,vcode ,keypressed1 ,keypressed2 ,keypressed3 ,call_initiate_time ,call_start_time ,call_end_time ,call_duration ,calldate ,failure_reason_id ,failure_isdn_reason_id ,content_type_id ,charging_rate_id ,circleappid ,ivr_response ,missingclip ,sattempts ,log_type_id ,filename ,time_stamp'
    for circle in list:
        for monyear in get_monyearlist(operator, circle):
            os.system("hive -e \" from %s_obd insert overwrite table %s_obd_deflate partition (circle=%s, monyear='%s') \
            select %s where circle=%s and monyear='%s' \"" %(operator,operator,circle,monyear,col_list,circle,monyear))

if __name__ == '__main__':
    main()
