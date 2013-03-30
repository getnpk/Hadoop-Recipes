import os
import sys


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

def main():
    list = get_circlelist('tata')
    print list


if __name__ == '__main__':
    main()