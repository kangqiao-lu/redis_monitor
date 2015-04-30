#!/usr/bin/python
#coding=utf-8
import MySQLdb,logging

class WriteLog(object):
    def write(self,log_msg):
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %X',
                filename='/data/mysql/opdir/lujiajun/redis_mon/monitor.log',
                filemode='a')
        logging.error(log_msg)

class MySQLHandler(object):
    def __init__(self,host,port):
        self.Log = WriteLog()
        self.host = host
        self.port = int(port)
        self.user = 'dba_monitor'
        self.pw = 'XXXXXX'
        
        _failed_times = 0
        while True:
            try:
                self.con_db = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.pw)
                self.con_db.autocommit(1)
                self.cursor = self.con_db.cursor()
            except Exception as e:
                _failed_times += 1
                if _failed_times >= 3:
                    print e
                    log_msg = "%s:%s %s" % (self.host,self.port,e)
                    self.Log.write(log_msg)
                else:
                    continue
            break
    def reconnect(self):
        _failed_times = 0
        while True:
            try:
                self.con_db = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self.pw)
                self.con_db.autocommit(1)
                self.cursor = self.con_db.cursor()
            except Exception as e:
                _failed_times += 1
                if _failed_times >= 3:
                    print e
                    log_msg = "%s:%s %s" % (self.host,self.port,e)
                    self.Log.write(log_msg)
                else:
                    continue
            break
    def get_mysql_data(self,sql):
        try:
            self.cursor.execute(sql)
            sql_data = self.cursor.fetchall()
            return sql_data
        except MySQLdb.OperationalError as e:
            if 2006 == e.args[0]:
                self.reconnect()
                try:
                    self.cursor.execute(sql)
                    sql_data = self.cursor.fetchall()
                    return sql_data
                except MySQLdb.Error as e1:
                    print e1.args[1]
                    log_msg = "%s:%s %s" % (self.host,self.port,e1)
                    self.Log.write(log_msg)
                    return 0
            else:
                print e.args[1]
                log_msg = "%s:%s %s" % (self.host,self.port,e)
                self.Log.write(log_msg)
                return 0
        except MySQLdb.Error as e2:
            print e2.args[1]
            log_msg = "%s:%s %s" % (self.host,self.port,e2)
            self.Log.write(log_msg)
            return 0
        except Exception as e3:
            log_msg = "%s:%s %s" % (self.host,self.port,e3)
            self.Log.write(log_msg)
            return 0
    def execute_sql(self,sql):
        try:
            self.cursor.execute(sql)
            status = 1
        except  MySQLdb.OperationalError as e:
            if 2006 == e.args[0]:
                self.reconnect()
                try:
                    self.cursor.execute(sql)
                    status = 1
                except MySQLdb.Error as e1:
                    print e1.args[1]
                    log_msg = "%s:%s %s" % (self.host,self.port,e1)
                    self.Log.write(log_msg)
                    status = 0
            else:
                print e.args[1]
                log_msg = "%s:%s %s" % (self.host,self.port,e)
                self.Log.write(log_msg)
                status = 0
        except  MySQLdb.Error as e2:
            print e2.args[1]
            log_msg = "%s:%s %s" % (self.host,self.port,e2)
            self.Log.write(log_msg)
            status = 0
        except Exception as e3:
            log_msg = "%s:%s %s" % (self.host,self.port,e3)
            self.Log.write(log_msg)
            status = 0
        finally:
            return status

    def close_connection(self):
        self.con_db.close()
