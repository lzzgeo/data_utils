#!/usr/bin/python
# -*- coding: utf-8 -*-
import getopt, sys, os
import urllib2, urllib, httplib, socket
import re
import MySQLdb
import cx_Oracle

import time,threading,thread
import string,StringIO
import Queue
import traceback
import gzip
#import chardet
import json
import math

from utils import *
from logManager import *

reload(sys)
sys.setdefaultencoding('utf-8')

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
#os.environ["NLS_LANG"] = ".UTF8"
#meta_mutex = threading.Lock()

g_owner_tables = []
tables_mutex = threading.Lock()



# 操作oracle库
class OracleManager:
    def __init__(self, logMgr, conn_param):
        self.logMgr = logMgr
        self.utils = Utils()
        try:
            cstr = conn_param['ip'] + ":%s/" + conn_param['db']
            cstr = cstr % conn_param['port']
            self.conn = cx_Oracle.connect(conn_param['user'], conn_param['pwd'], cstr)
            self.cur = self.conn.cursor()
            self.cur.execute("ALTER SYSTEM SET \"_ALLOW_LEVEL_WITHOUT_CONNECT_BY\"=TRUE SCOPE=BOTH")
        except:
            self.logMgr.write(traceback.format_exc())

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def execute(self, sql):
        try:
            self.cur.execute( sql )
        except:
            self.logMgr.write(traceback.format_exc())

    def getRecCount(self, owner, table):
        count = 0
        res = None
        try:
            sql = "SELECT COUNT(*) FROM %s.%s" % (owner, table)
            self.cur.execute( sql )
            res = self.cur.fetchone()
            count = res[0]
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            return count

    def getTables(self, filter):
        res = None
        try:
            sql = "SELECT OWNER, TABLE_NAME FROM DBA_TABLES %s" % filter
            self.cur.execute( sql )
            res = self.cur.fetchall()
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            return res

    """
    select sql ( from oralce )
    drop table sql ( in mysql )
    create table sql ( in mysql )
    insert sql ( to mysql )
    """
    def buildSQL(self, owner, table):
        drClause = ''
        creClause = ''
        selClause = ''
        insClause = ''
        valClause = ''
        col_types = []
        try:
            sql = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH, NULLABLE FROM sys.DBA_TAB_COLUMNS WHERE OWNER='%s' AND TABLE_NAME='%s'" % (owner, table)
            self.cur.execute( sql )
            res = self.cur.fetchall()
            if res is None or len(res)<1:
                return ('', '', '',None)

            for col in res:
                isNull = " "
                if col[6] == 'Y':
                    isNull = " NOT NULL "
                col_types.append(col[1])
                valClause += ', %s'

                if col[1]=="DATE":
                    #tt=time.mktime(row[i].timetuple())
                    #stime = time.strftime ('%Y-%m-%d %H:%M:%S', time.localtime(tt) )
                    #midsql += "'" + stime + "'"
                    creClause += ", `" + col[0] + "` VARCHAR(30)" + isNull + "DEFAULT ''"
                    selClause += ', ' + col[0]
                    insClause += ', `' + col[0] + "`"
                elif col[1]=="ROWID":
                    creClause += ", " + col[0] + " CHAR(18)" + isNull + "DEFAULT ''"
                    selClause += ', ' + col[0]
                    insClause += ', `' + col[0] + "`"
                elif col[1]=="SDO_GEOMETRY":
                    creClause += ", `Geo` GEOMETRY"
                    selClause += ", SDO_UTIL.TO_WKTGEOMETRY(" + col[0] + ") Geo"
                    insClause += ", `Geo`"
                    valClause += ', %s'
                elif col[1]=="NUMBER":
                    if col[4]!=0:
                        creClause += ", `" + col[0] + "` DOUBLE"
                    else:
                        tt = ''
                        if col[3]<=2:
                            tt = " TINYINT"
                        elif col[3]<=4:
                            tt = " SMALLINT"
                        elif col[3]<=10:
                            tt = " INT"
                        elif col[3]>10:
                            tt = " BIGINT"
                        creClause += ", `" + col[0] + "`" + tt
                    selClause += ", " + col[0]
                    insClause += ", `" + col[0] + "`"
                elif col[1]=="CHAR":
                    creClause += ( (", `" + col[0] + "` CHAR(%d)" + isNull + "DEFAULT ''") % col[5])
                    selClause += ', ' + col[0]
                    insClause += ', `' + col[0] + "`"
                elif col[1]=="VARCHAR2":
                    if col[5]<=6:
                        creClause += ( (", `" + col[0] + "` CHAR(%d)" + isNull + "DEFAULT ''") % col[5])
                    elif col[5]<500:
                        creClause += ( (", `" + col[0] + "` VARCHAR(%d)" + isNull + "DEFAULT ''") % col[5])
                    else:
                       creClause += (", `" + col[0] + "` TEXT" + isNull)
                    selClause += ', ' + col[0]
                    insClause += ', `' + col[0] + "`"
                else:
                    print "error: have no mapping columns  ====", owner, table, col[0]
            
            
            creClause = creClause.strip(',')
            drClause  = "DROP TABLE IF EXISTS " + table;
            creClause = "CREATE TABLE " + table + "(" + creClause + ") ENGINE = MyISAM DEFAULT CHARACTER SET = utf8"
            
            selClause = selClause.strip(',')
            selClause = "SELECT " + selClause.strip(',') + " FROM " + owner + "." + table
            
            insClause = insClause.strip(',')
            insClause = "INSERT INTO " + table + "(" + insClause +") "
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            return (selClause, drClause, creClause, insClause, col_types)

    """
    deprecated
    """
    def buildSQL2(self, owner, table):
        creClause = ''
        selClause = ''
        insClause = ''
        valClause = ''
        col_types = []
        try:
            sql = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH FROM sys.DBA_TAB_COLUMNS WHERE OWNER='%s' AND TABLE_NAME='%s'" % (owner, table)
            self.cur.execute( sql )
            res = self.cur.fetchall()
            if res is None or len(res)<1:
                return ('', '', '',None)
                
            for col in res:
                col_types.append(col[1])
                if col[1]=="SDO_GEOMETRY":
                    creClause += ", Geo GEOMETRY"
                    #'GEOMFROMTEXT('||chr(39)||||chr(39)||')'
                    selClause += ", SDO_UTIL.TO_WKTGEOMETRY(" + col[0] + " Geo"
                    insClause += ", Geo"
                    valClause += ', %s'
                elif col[1]=="NUMBER":
                    if col[4]!=0:
                        creClause += ( (", " + col[0] + " DECIMAL(%s,%)") % (col[3],col[4]) )
                        valClause += ', %s'
                    else:
                        tt = ''
                        if col[3]<=2:
                            tt = " TINYINT"
                        elif col[3]<=4:
                            tt = " SMALLINT"
                        if col[3]<=6:
                            tt = " MEDIUMINT"
                        elif col[3]<=10:
                            tt = " INT"
                        elif col[3]>10:
                            tt = " BIGINT"
                        
                        creClause += ", " + col[0] + tt
                        valClause += ', %s'

                    selClause += ", " + col[0]
                    insClause += ", " + col[0]
                elif col[1]=="VARCHAR2":
                    if col[5]>500:
                        creClause += ( (", " + col[0] + " VARCHAR(%d) NOT NULL DEFAULT ''") % col[5])
                    else:
                        creClause += (", " + col[0] + " TEXT NOT NULL DEFAULT ''")
                        selClause += ', ' + col[0]
                        insClause += ', ' + col[0]
                        valClause += ', %s'
                else:
                    print "error: have no mapping columns  ====", owner, table, col[0]
            
            
            creClause = creClause.strip(',')
            creClause = "DROP TABLE IF EXISTS " + table + "; CREATE TABLE " + table + "(" + creClause + ") ENGINE = MyISAM DEFAULT CHARACTER SET = utf8"
            
            selClause = selClause.strip(',')
            selClause = "SELECT " + selClause.strip(',') + " FROM " + owner + "." + table
            
            insClause = insClause.strip(',')
            valClause = valClause.strip(',')
            insClause = "INSERT INTO " + table + "(" + insClause +") VALUES(" + valClause + ")"
            #insClause = "INSERT INTO " + table + "(" + insClause +") "
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            print creClause
            return (selClause, creClause, insClause, col_types)


    # 构建数据来源表
    def setSource(self, sql, col_types):
        self.col_types = col_types
        try:
            self.cur.execute( sql )
        except:
            self.logMgr.write(traceback.format_exc())

    """
    get records from oracle, and build 
    """
    def getRecords(self):
        vals = []
        try:
            while True:
                if len(vals)>500:
                    break
                    
                res = self.cur.fetchmany(50)
                if res == None or len(res)<1:
                    break
                    
                for rc in res:
                    i = 0
                    col_vals = []
                    for col in rc:
                        if isinstance(col, int) or isinstance(col, float):
                            col_vals.append(str(col))
                        #elif isinstance(col, str) or isinstance(col, datetime):
                        elif self.col_types[i]=="DATE":
                            if col==None:
                                col_vals.append("''")
                            else:
                                tt=time.mktime(col.timetuple())
                                col_vals.append( "'" + time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime(tt) ) + "'")
                        elif isinstance(col, str):
                            #col = self.utils.strQ2B(col.decode('utf8')).encode('utf8')
                            col = self.utils.strQ2B(col.decode('utf8'))
                            col = MySQLdb.escape_string(col.encode("utf-8"))
                            #col_vals.append( "'%s'" % col.replace("'", "\'") )
                            col_vals.append( "'%s'" % col )
                        elif self.col_types[i]=="SDO_GEOMETRY":
                            col = MySQLdb.escape_string(col.read().encode("utf-8"))
                            col_vals.append( "GEOMFROMTEXT('%s')" % col.replace("'", "\'") )
                        elif isinstance(col, cx_Oracle.LOB):
                            #col = self.utils.strQ2B(col.decode('utf8')).encode('utf8')
                            col = self.utils.strQ2B(col.decode('utf8'))
                            col = MySQLdb.escape_string(col.read().encode("utf-8"))
                            #col_vals.append( "'%s'" % col.replace("'", "\'") )
                            col_vals.append( "'%s'" % col )
                        elif col==None:
                            if self.col_types[i]=="VARCHAR2":
                                col_vals.append("''")
                            else:
                                col_vals.append("null")
                        else:
                            print " >>>>data type error        ", type(col)
                        i += 1
                    vals.append( "(%s)" % (",".join(col_vals)) )
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            return ( len(vals), ",".join(vals) )

    """
    使用原始数据返回的record list
    """
    def getRecords2(self, num):
        res = None
        try:
            res = self.cur.fetchmany(num)
        except:
            self.logMgr.write(traceback.format_exc())
        finally:
            rr = []
            for rc in res:
                t_cols = []
                for col in rc:
                    if isinstance(col, cx_Oracle.LOB):
                        t_cols.append( col.read() )
                    else:
                        t_cols.append( col )
                rr.append( tuple(t_cols) )

            return rr

"""
create tables and insert values in mysql
"""
class MysqlManager:
    def __init__( self, logMgr, conn_param ):
        self.ins_pat= ''
        self.logMgr = logMgr
        try:
            self.conn = MySQLdb.connect( db=conn_param['db'], host=conn_param['ip'], user=conn_param['user'], passwd=conn_param['pwd'], port=conn_param['port'], charset='utf8')
            self.cur = self.conn.cursor()
            self.cur.execute("SET NAMES utf8")
        
        except:
            self.logMgr.write(traceback.format_exc())

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def execute(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except:
            self.logMgr.write(traceback.format_exc())
            self.logMgr.write(sql)
        #finally:
            
    def writeRecords(self, res):
        if res==None or len(res)<1:
            return 0
        try:
            self.cur.executemany( self.ins_pat, res )
            self.conn.commit()
        except:
            self.logMgr.write( traceback.format_exc() )
        finally:
            return len(res)


def getTable():
    tables_mutex.acquire()
    tt = None
    if ( g_owner_tables!=None and len(g_owner_tables)>0 ):
        tt = g_owner_tables.pop()
    tables_mutex.release()
    
    return tt


class Work_Thread(threading.Thread):
    def __init__(self, threadname, logMgr, ora_con, my_con ):
        threading.Thread.__init__(self,name = threadname)
        self.logMgr     = logMgr
        self.my_con     = my_con
        self.ora_con    = ora_con
    def expimp(self):
        logMgr.write('thread %s : start to ......\n' % self.name)
        owner_tab = None
        try:
            myMgr = MysqlManager( self.logMgr, self.my_con )
            oraMgr = OracleManager( self.logMgr, self.ora_con )
            insClause = ''
            drClause = ''
            creClause = ''
            selClause = ''
            col_types = None
            sql = ''
            num = 0
            while True:
                owner_tab = getTable( )
                if owner_tab==None:
                    break
                logMgr.write('import %s.%s into mysql\n' % (owner_tab[0], owner_tab[1]))
                (selClause, drClause, creClause, insClause,col_types) = oraMgr.buildSQL(owner_tab[0], owner_tab[1])
                oraMgr.setSource( selClause, col_types )
                
                myMgr.execute( drClause )
                myMgr.execute( creClause )
                
                while True:
                    (num, sql) = oraMgr.getRecords()
                    if num == 0:
                        break
                    myMgr.execute( "%s VALUES %s" % (insClause, sql) )
        except:
            #logMgr.write( 'error: %s.%s\n%s\n' % (owner_tab[0], owner_tab[1], traceback.format_exc()) )
            logMgr.write( 'error: %s\n' % (traceback.format_exc()) )

        finally:
            logMgr.write('thread %s : finished \n' % self.name)
    def exp(self):
        logMgr.write('thread %s : start to ......\n' % self.name)
        try:
            myMgr = MysqlManager( self.logMgr, self.my_con )
            oraMgr = OracleManager( self.logMgr, self.ora_con )
            insClause = ''
            drClause = ''
            creClause = ''
            selClause = ''
            col_types = None
            owner_tabs = oraMeta.getTables( 1 )
            sql = ''
            num = 0
            num_count = 0
            while owner_tabs:
                for tab in owner_tabs:
                    logMgr.write('import %s.%s into mysql\n' % (tab[0], tab[1]))
                    o_f = file("./"+tab[0]+tab[1],'w')
                    (selClause, drClause, creClause, insClause,col_types) = oraMeta.buildSQL(tab[0], tab[1])
                    oraMgr.setSource( selClause, col_types )
                    myMgr.execute( drClause )
                    myMgr.execute( creClause )
                    while True:
                        (num, sql) = oraMgr.getRecords()
                        num_count += num
                        if num == 0:
                            break
                        o_f.write("insClause")
                        o_f.write(" VALUES ")
                        o_f.write( sql )
                        #myMgr.execute( "%s VALUES %s" % (insClause, sql) )
                        print "\r\n     %s    " % num_count
                    o_f.close()
                owner_tabs = oraMeta.getTables( 1 )
        except:
            #logMgr.write( 'error: %s.%s, \n %s' % (rc[0], rc[1], traceback.format_exc()) )
            logMgr.write( 'error: %s\n' % traceback.format_exc() )

        finally:
            logMgr.write('thread %s : finished \n' % self.name)
    def run(self):
        self.expimp()

def Usage():
    print "migrate data from oracle to mysql"
    print "thread_num    how many threads will be used to do this task"
    print "ora_ip        connect to orace: ip"
    print "ora_port      connect to orace: port"
    print "ora_dbname    connect to orace: db name"
    print "ora_user      connect to orace: user"
    print "ora_pwd       connect to orace: password"
    print "my_ip         connect to mysql: ip"
    print "my_port       connect to mysql: port"
    print "my_dbname     connect to mysql: db name"
    print "my_user       connect to mysql: user"
    print "my_pwd        connect to mysql: password"
    print "ora_filter    the filter for the tables of oracle will be migrated from oracle to mysql"
    print "log           log file for saving warning, error and other info outputed by this task"

if __name__=='__main__':
    print sys.argv[1:]
    try:
        opts, args = getopt.getopt(sys.argv[1:], ':', ["help","thread_num=", "ora_ip=", "ora_port=", "ora_dbname=", "ora_user=", "ora_pwd=", "my_ip=", "my_port=", "my_dbname=", "my_user=", "my_pwd=", "ora_filter=", "log="])
    except getopt.GetoptError, err:
        print str(err)
        Usage()
        sys.exit(2)

    for o, a in opts:
        if o in ('--help'):
            Usage()
            sys.exit(2)
        elif o in ('--thread_num='):
            thread_num = int(a)
        elif o in ('--ora_ip='):
            ora_ip = a
        elif o in ('--ora_port='):
            ora_port = int(a)
        elif o in ('--ora_dbname='):
            ora_dbname = a
        elif o in ('--ora_user='):
            ora_user = a
        elif o in ('--ora_pwd='):
            ora_pwd = a
        elif o in ('--my_ip='):
            my_ip = a
        elif o in ('--my_port='):
            my_port = int(a)
        elif o in ('--my_dbname='):
            my_dbname = a
        elif o in ('--my_user='):
            my_user = a
        elif o in ('--my_pwd='):
            my_pwd = a        
        elif o in ('--ora_filter='):
            ora_filter = a
        elif o in ('--log='):
            log_file = a
        else:
            print 'unhandled parameter %s' % o
            Usage()

    logMgr = LogManager('./%s' % log_file)
    logMgr.out_screen = True

    ora_param  = {'ip':ora_ip, 'port':ora_port, 'db':ora_dbname, 'user':ora_user, 'pwd':ora_pwd}
    my_param   = {'ip':my_ip, 'port':my_port, 'db':my_dbname, 'user':my_user, 'pwd':my_pwd}

    oraMgr = OracleManager( logMgr, ora_param)
    g_owner_tables = oraMgr.getTables( ora_filter )
    del oraMgr
    
    i = 0
    thread_pool = []
    for i in range(0, thread_num):
        tt = Work_Thread( i, logMgr, ora_param, my_param )
        thread_pool.append( tt )
    for thread_i in thread_pool:
        thread_i.start()
    for thread_i in thread_pool:
        thread_i.join() 


    time.sleep(5)
    logMgr.write('all have been done!\n')
    sys.exit()
