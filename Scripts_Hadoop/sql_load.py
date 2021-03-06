
import glob
import os
import sys
import fnmatch

__author__="nitin p kumar"

pattern='.sql'

tablename=sys.argv[1]
rootpath=sys.argv[2]

def fun_load(tablename, rootpath):
    dirname='%s/*'%(rootpath)
    fd_list=glob.glob(dirname)
    for fd in fd_list:
        if os.path.isdir(fd):
            fun_load(fd)
        else:
            filename,ext=os.path.splitext(fd)
            if (ext == pattern):
                print('mysql -uroot %s < %s'%(tablename, os.path.abspath(fd)))
                os.system("gzip %s" %(os.path.abspath(fd)))

def search_file_load(tablename, rootpath):
    for root, dirs, files in os.walk(rootpath):
        for filename in fnmatch.filter(files, pattern):
                #print("%s" %os.path.join(root, filename))
                print("mysql -uroot %s < %s"  %(tablename, os.path.join(root, filename)))
                os.system("mysql -uroot %s < %s"  %(tablename, os.path.join(root, filename)))
                print("gzip %s"%(os.path.join(root, filename)))
                os.system("gzip %s"%(os.path.join(root, filename)))


if __name__=='__main__':
    #fun_load(tablename, rootpath)
    #print("*" * 10)
    pattern="*.sql"
    search_file_load(tablename, rootpath)
