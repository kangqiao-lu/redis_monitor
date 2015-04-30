#!/usr/bin/python
#coding=utf-8
#redis  monitor by lujiajun
#我的第一个Python脚本,里面很多写的不对,比如没有用到redis的长连接,没去一次数据都是新建一个连接,浪费资源.
#还是窜行的区监控每一个redis实例,可以改成Python的多线程或者多进程.
#需要改进和学习的地方太多
#刚开始貌似没弄明白is 和 == 的区别.
import time
import socket
import sys
import urllib
import datetime
import logging
import os
import string
import redis
import MySQLHandler

""" 连接mysql，获取数据"""
dbhost = ""
dbport = ""
db = MySQLHandler.MySQLHandler(dbhost,dbport)

""" ping redis"""
def conn_redis(h,p):
    redisClient = redis.StrictRedis(host=h,port=p,db=0,socket_timeout=5)
    try:
        redisClient.ping()
        return 1
    except:
        return 0

""" send sms according class"""
def send_sms_class(mon_class,msg):
	pass
""" send mail according class"""
def send_mail_class(mon_class,msg):
	pass


def get_redis_mem(h,p):
    redisClient = redis.StrictRedis(host=h,port=p,db=0,socket_timeout=5)
    try:
        used_mem = redisClient.info()['used_memory_rss']
        return used_mem
    except:
        return None

def compare_mem(id):
    sql = "select host,port,class,usefor,is_master,mem_limit,mem_status from dba_stats.redis_conf where id=%d" % id
    alldata = db.get_mysql_data(sql)
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
            db.execute_sql(update_sql)
    else:
        if mem_status <= 3:
            now_mem_status = mem_status + 1
            update_sql = "update dba_stats.redis_conf set mem_status=%d where id=%d" % (now_mem_status,id)
            db.execute_sql(update_sql)
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
    start_time = int(db.get_mysql_data(sql)[0][0])
    now_time =  int(db.get_mysql_data("select UNIX_TIMESTAMP(now())")[0][0])
    if mon_type_time == 'two_starttime':
        update_sql = "update dba_stats.redis_conf set is_mon=1,mon_two=1,mon_ten=1 where id=%s" % id
    elif mon_type_time == 'mem_starttime':
        update_sql = "update dba_stats.redis_conf set mon_mem=1 where id=%s" % id
    if now_time >= start_time:
        db.execute_sql(update_sql)       
        



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
