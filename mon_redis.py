#!/usr/bin/python
#encoding=utf-8
#redis  monitor by lujiajun
import MySQLdb
import time
import socket
import sys
import urllib
import datetime
import logging
import os
import string
import redis

""" 连接mysql，获取数据"""
def get_mysql_data(sql):
    try:
        con_db =  MySQLdb.connect(host='yz-log-ku-m00',port=3306,user='dba_monitor',passwd='ganjicac',db='dba_stats')
        cursor = con_db.cursor()
        cursor.execute(sql)
        sql_data = cursor.fetchall()
    except:
        sql_data = 0
    finally:
        return sql_data

def execute_sql(sql):
    try:
        con_db = MySQLdb.connect(host='yz-log-ku-m00',port=3306,user='dba_monitor',passwd='ganjicac',db='dba_stats')
        con_db.autocommit(1)
        cursor= con_db.cursor()
        cursor.execute(sql)
    except:
        return 0

""" ping redis"""
def conn_redis(h,p):
    redisClient = redis.StrictRedis(host=h,port=p,db=0,socket_timeout=5)
    try:
        redisClient.ping()
        return 1
    except:
        return 0

def send_sms(msg, phones, sms_gateway='http://sms.dns.ganji.com:20000/WebGate/ShortMsg.aspx'):
    '''
        send short message to phones, no return value(empty string anyway)
    '''
    res = True
    unique_id = 'Tan'
    query_dict = {'opt': 'send',
                  'uniqueId': unique_id,
                  'serviceId': 'bf33195b-2a42-a847-b8d8-351a233a2c87',
                  'phones': phones,
                  'content': msg}
                  #'content': msg.encode('utf8')}
    try:
        query = urllib.urlencode(query_dict)
        url = '%s?%s' % (sms_gateway, query)
        fh = urllib.urlopen(url)
        status = fh.read()
        if status.strip() != '1':
            # print "send_sms failed:", status
            print "send_sms failed: returned status != 1"
    except Exception as ex:
        print 'send sms failed:', ex
        res = False
    return res


def send_mail_http(receiver, body, title=None,
                   sid='dba-alert', email_gateway='http://edmpost.dns.ganji.com:8080/emailservice/postdata'
                   ):
    res = True
    if title is None:
        title = 'warning from dba python'
        title = title +  datetime.datetime.now().strftime(u"%m-%d %H:%M")
    if not body:
        print('empty body, no email sent')
        return False
    post_data = {'data':
                 "{'Sid': '%s', 'Mail': '%s', 'Type': '1', 'Subject': '%s'}"
                 % (sid, receiver, title),
                 'mailbody': '%s' % body}
                 #% (sid, receiver, title.encode('utf-8')),
                 #'mailbody': '%s' % body.encode('utf-8')}
    try:
        fh = urllib.urlopen(email_gateway, data=urllib.urlencode(post_data))
        result = fh.readlines()
        if result != ['result=1']:
            print(u'send failed:%s' % result)
            res = False
        else:
            print(u'send ok:%s' % result)
    except Exception as ex:
        print (u'send fail:', ex)
        res = False
    return res    

""" send sms according class"""
def send_sms_class(mon_class,msg):
    get_phones_sql = "select phones from dba_stats.redis_mon_phones where class='%s'" % mon_class
    phones = get_mysql_data(get_phones_sql)[0][0]
    if phones is ():
        phones = '13381109027,15910707764,13811018735'
    send_sms(msg,phones)

""" send mail according class"""
def send_mail_class(mon_class,msg):
    get_mail_sql = "select emails from dba_stats.redis_mon_phones where class='%s'" % mon_class
    mails = get_mysql_data(get_mail_sql)[0][0]
    if mails is ():
        mails = 'dba.mon@ganji.com'
    send_mail_http(mails,msg,msg)


def get_redis_mem(h,p):
    redisClient = redis.StrictRedis(host=h,port=p,db=0,socket_timeout=5)
    try:
        used_mem = redisClient.info()['used_memory_rss']
        return used_mem
    except:
        return None

def compare_mem(id):
    sql = "select host,port,class,usefor,is_master,mem_limit,mem_status from dba_stats.redis_conf where id=%d" % id
    alldata = get_mysql_data(sql)
    if alldata == 0:
        print 'Connect mysql has some problem......'
        exit(0)
    elif alldata == ():
        print "There is no monitor data......"
        exit(0)
    data = alldata[0]
    host = data[0]
    port = int(data[1])
    mem_limit = data[5]
    use_mem = get_redis_mem(host,port)
    mem_status = int(data[6])
    if mem_limit > use_mem:
        redis_class = data[2]
        if mem_status is not 1:
            update_sql = "update dba_stats.redis_conf set mem_status=1 where id=%d" % id
            execute_sql(update_sql)
    else:
        if mem_status <= 3:
            now_mem_status = mem_status + 1
            update_sql = "update dba_stats.redis_conf set mem_status=%d where id=%d" % (now_mem_status,id)
            execute_sql(update_sql)
            time_format = '%Y-%m-%d %X'
            now_date = time.strftime(time_format, time.localtime())
            usefor = data[3]
            is_master = data[4]
            if is_master == 0:
                role = 'Slave'
            elif is_master == 1:
                role = 'Master'
            else:
                role = 'Twemproxy'
            redis_class = data[2]
            mem_used = use_mem/1024/1024/1024
            if mem_used == 0:
                now_use == use_mem/1024/1024
                mem_msg = '%s %s:%d more than memory limit,mem_limit is %dG,now used %dM, %s %s %s' % (redis_class,host,port,mem_limit/1024/1024/1024,now_use,usefor,role,now_date)
                send_sms_class(redis_class,mem_msg)
                send_mail_class(redis_class,mem_msg)
            else:
                now_use = use_mem/1024/1024/1024
                mem_msg = '%s %s:%d more than memory limit,mem_limit is %dG,now used %dG, %s %s %s' % (redis_class,host,port,mem_limit/1024/1024/1024,now_use,usefor,role,now_date)
                send_sms_class(redis_class,mem_msg)
                send_mail_class(redis_class,mem_msg)

"""check slave status"""
def check_slave_status(h,p):
    redisClient = redis.StrictRedis(host=h,port=p,db=0,socket_timeout=5)
    try:
        slave_status = redisClient.info()['master_link_status']
        if slave_status == 'up':
            res = 1
        else:
            res = 0
    except:
            res = None
    finally:
        return res

def start_mon(id,mon_type_time):
    sql = "select UNIX_TIMESTAMP(%s) from dba_stats.redis_conf where id=%s" % (mon_type_time,id)
    start_time = int(get_mysql_data(sql)[0][0])
    now_time =  int(get_mysql_data("select UNIX_TIMESTAMP(now())")[0][0])
    if mon_type_time == 'two_starttime':
        update_sql = "update dba_stats.redis_conf set is_mon=1,mon_two=1,mon_ten=1 where id=%s" % id
    elif mon_type_time == 'mem_starttime':
        update_sql = "update dba_stats.redis_conf set mon_mem=1 where id=%s" % id
    if now_time >= start_time:
        execute_sql(update_sql)       
        



""" 获取redis监控列表"""
now_sql = 'select host,port,class,mon_two,usefor,port_detail,is_mon,is_master,mem_limit,mon_mem,id from dba_stats.redis_conf'
alldata = get_mysql_data(now_sql)
""" 判断获取的数据知否正确"""
if alldata == 0:
    print 'Connect mysql has some problem......'
    exit(0)
elif alldata == ():
    print "There is no monitor data......"
    exit(0)

for data in alldata:
    is_mon = data[6]
    redis_id = data[10]
    if is_mon == 0:
        start_mon(redis_id,'two_starttime')
    else:
        mon_two = data[3]
        if mon_two == 0:
            start_mon(redis_id,'two_starttime')
        else: 
            is_master = data[7]
            if is_master == 0:
                role = 'Slave'
            elif is_master == 1:
                role = 'Master'
            else:
                role = 'Twemproxy'
            host = data[0]
            port = int(data[1])
            port_status = conn_redis(host,port)
            redis_class = data[2]
            usefor = data[4]
            if port_status is not 1:
                detail = data[5]
                time_format = '%Y-%m-%d %X'
                now_date = time.strftime(time_format, time.localtime())
                new_msg='%s,%s %s:%d Cannot Connect Redis,%s  %s' % (redis_class,role,host,port,usefor,now_date)
                send_sms_class(redis_class,new_msg)
                send_mail_class(redis_class,new_msg)
            else:
                mem_mon = data[9]
                if role == 'Twemporxy':
                    continue
                elif role == 'Master':
                    if mem_mon == 0:
                        start_mon(redis_id,'mem_starttime')
                    else:
                        compare_mem(redis_id)
                elif role == 'Slave':
                    if mem_mon == 1:
                        compare_mem(redis_id)
                    elif mem_mon == 0:
                        start_mon(redis_id,'mem_starttime')
                    slave_status =  check_slave_status(host,port)
                    if slave_status == 1:
                        continue
                    else:
                        time_format = '%Y-%m-%d %X'
                        now_date = time.strftime(time_format, time.localtime())
                        slave_msg = "%s,%s %s:%d slave error,%s %s" % (redis_class,role,host,port,usefor,now_date)
                        send_sms_class(redis_class,slave_msg)
                        send_mail_class(redis_class,slave_msg)
