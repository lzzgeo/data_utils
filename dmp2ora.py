#!/usr/bin/env python
#coding=utf-8
import getopt, sys, os, stat
import traceback

global oraconn
global path
global to_user
global impdp_filter
global ora_directory
global tables

oraconn     = ""
path        = ""
to_user     = ""
impdp_filter= []
ora_directory= ""
tables      = ""

def imp_pattern( dump_file, from_user ):
    global oraconn
    global path
    global to_user
    global impdp_filter
    global ora_directory
    global tables

    content = "imp %s \
        file=%s \
        FROMUSER=%s TOUSER=%s \
        ignore=y \
        buffer=40960000 \
        log=%s.log \
        INDEXES=N \
        grants=n \
        constraints=n \
        statistics=none \
        COMMIT=N \
        ANALYZE=N \
        TABLES=%s"

    content = content % (oraconn, dump_file, from_user, to_user, dump_file[:-4], tables)
    os.popen(content)

def impdp_pattern( schema ):
    global oraconn
    global path
    global to_user
    global impdp_filter
    global ora_directory
    global tables

    try:
        # 拼接导入表
        in_param = []
        tables = tables.split(",")
        for t_ in tables:
            in_param.append( schema.upper() + "." + t_ );
        tables = ",".join(in_param)

        content = "directory=%s \
            DUMPFILE=%s.dmp \
            TABLE_EXISTS_ACTION=APPEND \
            LOGFILE=%s.log \
            TABLES=%s \
            EXCLUDE=PACKAGE,FUNCTION,VIEW,CONSTRAINT,INDEX,STATISTICS \
            REMAP_SCHEMA=%s:%s"
        content = content  % (ora_directory, schema, schema, tables, schema.upper(), to_user)
        os.popen( "impdp %s %s" % (oraconn, content) )

    except:
        print traceback.print_exc()

def walk( ):
    global oraconn
    global path
    global to_user
    global impdp_filter
    global ora_directory
    global tables

    for item in os.listdir(path):
        try:
            subpath = os.path.join(path, item)
            mode = os.stat(subpath)[stat.ST_MODE]
            if item[-4:].lower()!='.dmp':
                continue

            print item[item.rfind("_"):-4].upper()
            print impdp_filter

            if item[item.rfind("_"):-4].upper() in impdp_filter:
                print "====impdp : %s=================================" % item
                impdp_pattern( item[:-4] )
            else:
                print "====imp : %s=================================" % item
                imp_pattern( path + "/" + item, item[:-4] )
        except:
            print traceback.print_exc()

def Usage():
    print "load dump files into oracle"
    print "--path           where all the dump file could be found"
    print "--oraconn        user name and password, for example: user/password"
    print "--ora_directory  this parameter is only available for impdp command"
    print "--to_user        where all the data should be imported to"
    print "--impdp_filter   which dump files should be used impdp pattern to load data，this is a list of part of file names, for example '_shandong','_tianjin'"
    print "--tables         which tables will be loaded from dump files into oracle database"

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], ':', ['help','path=', 'oraconn=', 'ora_directory=', 'to_user=', 'impdp_filter=', 'tables='])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)

    for o, a in opts:
        if o in ('--help'):
            Usage()
            sys.exit(2)
        elif o in ('--path='):
            path = a
        elif o in ('--oraconn='):
            oraconn = a
        elif o in ('--ora_directory='):
            ora_directory = a
        elif o in ('--to_user='):
            to_user = a
        elif o in ('--impdp_filter='):
            impdp_filter = a.split(",")
        elif o in ('--tables='):
            tables = a
        else:
            print 'unhandled parameter'
            Usage()
    walk( )
