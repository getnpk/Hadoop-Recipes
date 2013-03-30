#! /usr/bin/env python

import glob
import os
import sys


basepath=sys.argv[1]

def fun_rename():
        os.chdir(basepath)
        file_list=glob.glob('*')
        for file in file_list:
            temp_file=file.replace(':','_')
            new_file=temp_file.replace('-','_')
            #os.system('mv %s %s'%(file,new_file))
            os.rename(file,new_file)


if __name__=='__main__':
        fun_rename()


