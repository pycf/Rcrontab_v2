# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 11:23:55 2017

@author: Administrator
"""


import configparser
import json
import os
import socketserver
import threading
import time
import sys
base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_path)
from Rcrontab.master import maintain_plans
from Rcrontab.master import maintain_slaves_status
from Rcrontab.master.MasterPackages.sql_codes import program_result_log
from Rcrontab.master.MasterPackages import send_to_slave
from Packages import mysql
from Packages.write_log import write_log
from Rcrontab.master.loop_check_programs_result import LoopCheckProgramResult
from Rcrontab.master.sending_calculate_program import SendingCalculateProgram
from Rcrontab.master.MasterPackages.send_mail import SendMail

service_path = os.path.join("/", base_path, "config/service.conf")
slave_path = os.path.join("/", base_path, "config/slaves.conf")


conf = configparser.ConfigParser()
conf.read(service_path)
master_ip = conf.get("master", "MASTER_IP_INNER")
master_port = int(conf.get("master", "MASTER_PORT"))
mysql_host = conf.get("master", "MYSQL_IP")
mysql_user = conf.get("master", "MYSQL_USER")
mysql_pwd = conf.get("master", "MYSQL_PWD")
log_file = conf.get("master", "MASTER_LOG_FILE")
db_name = conf.get("master", "DATABASE_NAME")
conf.clear()
conf.read(slave_path)
slaves = conf.sections()
lock = threading.RLock()


'''
接收数据
'''


# 服务器接收信息
class MasterServer(socketserver.BaseRequestHandler):
    # 数据类型定义
    task_list = {0: "_execution_map",  # slave 方法
                 1: "_program_start",  # master 方法
                 2: "_stop_master",  # master 方法
                 3: "_stop_slave",  # slave 方法
                 4: "_program_end",  # master 方法
                 5: "_calculate_distribute",  # slave 方法
                 7: "_redistribute_plan",  # master 方法
                 8: "_get_server_info",  # master 方法
                 9: "_hello"  # slave 方法
                 }
    maps = {}

    def handle(self):
        conn = self.request
        len_total = int(conn.recv(1024).decode('utf-8'))
        conn.sendall('ok'.encode('utf-8'))
        length = 0
        data = ''
        while True:
            rev_data = conn.recv(1024)
            length += len(rev_data)
            data += rev_data.decode('utf-8')
            if length >= len_total:
                break
        self.maps = json.loads(data)
        print("master_save:", self.maps)
        self.distribute()

    def distribute(self):
        try:
            msg_type = int(self.maps['type'])
            if msg_type in self.task_list:
                func_name = self.task_list[msg_type]
                print(func_name)
                if hasattr(self, func_name):
                    func = getattr(self, func_name)
                    func()
                else:
                    print(0)
                    self.request.close()
            else:
                log_string = "数据类型type: %s 没有定义！" % msg_type
                print(log_string)
                write_log(log_string)
                self.request.close()
        except Exception as err:
            print("distribute Err", str(err))
            write_log(str(err))
            self.request.close()

    def _program_end(self):
        event_time = self.maps['data']['time']
        sid = self.maps['data']['sid']
        result_code = self.maps['data']['result']
        version = self.maps['data']['version']
        subversion = self.maps['data']["subversion"]
        if result_code == 0:
            result = "succeed"
            event_type = 2
            extra_info = ""
        else:
            result = "failed"
            event_type = 3
            extra_info = self.maps['data']['extra_info']
            if extra_info == "":
                extra_info = "Err_code:", str(result_code)
        log_string = ('脚本编号: %s  脚本执行结束时间: %s result:%s' % (sid, event_time, result))
        write_log(log_string)
        sql_str = program_result_log.format(version=version, event_time=event_time, sid=sid,
                                            event_type=event_type, extra_info=extra_info, subversion=subversion)
        print("sql_str:", sql_str)
        self.sql_write(sql_str)
        response = "ok"
        self.request.sendall(response.encode('utf-8'))

    def _program_start(self):
        try:
            sid = int(self.maps['data']["sid"])
            event_time = self.maps['data']["time"]
            name = self.maps['data']["program"]
            version = self.maps['data']['version']
            subversion = self.maps['data']["subversion"]
            event_type = 100
            sql_str = program_result_log.format(version=version, event_time=event_time, sid=sid,
                                                event_type=event_type, extra_info="", subversion=subversion)
            print(sql_str)
            sql = mysql.Mysql()
            sql.mysql_write(sql_str)
            log_string = '程序编号: %s  程序名称: %s  程序执行开始时间: %s' % (str(sid), str(name), str(event_time))
            write_log(log_string)
            response = "ok"
            self.request.sendall(response.encode('utf-8'))
        except Exception as e:
            print(str(e))

#     def _program_rerun(self):
#         sid = int(self.maps["sid"])
#         rerun = _GetRerunProgram(sid)
#         rerun.run()
#         self.request.sendall("OK")

    @staticmethod
    def _stop_master():
        ModifyInfo.master_status = 1

    def _get_alive_slave(self):
        info_name = self.maps['info_name']
        if info_name == 'alive_slave':
            data = send_to_slave.SlaveStatus.get_slave_status()
            send_data = json.dumps(data).encode('utf-8')
            self.request.sendall(send_data)

    @staticmethod
    def sql_write(sql_str):
        sql = mysql.Mysql(mysql_host, mysql_user, mysql_pwd, db_name)
        sql.mysql_write(sql_str)


# 主服务器 启动信息
class Master:
    def start(self):
        log_str = "Master Server 主程序启动"
        write_log(log_str)
        self._maintain_plans()
        self._distribute_crawler()
        self._send_mail()
        time.sleep(10)  # 等待数据库读取完毕
        self._run_hello()
        time.sleep(5)
        self._loop_check_program_result()
        self._sending_calculate_program()
        self._monitor_stop()
        self._master_server()

    @staticmethod
    def _run_hello():
        log_str = "<slave状态>监测线程启动!"
        write_log(log_str)
        t = threading.Thread(target=maintain_slaves_status.SendingHello().run)
        t.setName("check-slave-stats")
        print(log_str)
        t.setDaemon(True)
        t.start()

    @staticmethod
    def _master_server():
        global master_ip
        global master_port
        s_server = socketserver.ThreadingTCPServer((master_ip, master_port), MasterServer)
        log_str = ('master_server start! bind_ip: %s bind_port:%s ' % (master_ip, master_port))
        write_log(log_str)
        with s_server:
            s_server.serve_forever()

    @staticmethod
    def _distribute_crawler():
        log_str = "<Master执行计划监控程序>线程启动!"
        print(log_str)
        write_log(log_str)
        t = threading.Thread(target=maintain_plans.run)
        t.setName("distribute-plan")
        t.setDaemon(True)
        t.start()

    @staticmethod
    def _monitor_stop():
        log_str = "<Master服务状态监测>线程启动!"
        write_log(log_file, log_str)
        t = threading.Thread(target=stop)
        t.setDaemon(True)
        t.start()

    @staticmethod
    def _loop_check_program_result():
        log_str = "<Master程序结果监测>线程启动!"
        t = threading.Thread(target=LoopCheckProgramResult().run)
        t.setName("monitor-result")
        t.setDaemon(True)
        print(log_str)
        t.start()

    @staticmethod
    def _sending_calculate_program():
        log_str = "<Master计算程序依赖监测>线程启动!"
        t = threading.Thread(target=SendingCalculateProgram().loop_check_calculate_programs)
        t.setName("calculate-programs-check")
        t.setDaemon(True)
        print(log_str)
        t.start()

    @staticmethod
    def _send_mail():
        log_str = "<邮件发送模块>线程启动!"
        t = threading.Thread(target=SendMail())
        t.setName("send-mail")
        t.setDaemon(True)
        print(log_str)
        t.start()


# 获取信息
class ModifyInfo:
    def __init__(self):
        self._master_status = 0

    @property
    def master_status(self):
        return self._master_status

    @master_status.setter
    def master_status(self, value):
        self._master_status = value
# 监控 master_status 为 1 关闭程序


def stop():
    while True:
        status = ModifyInfo.master_status
        time.sleep(3)
        if status == 1:
            log_str = "master server shutdown!"
            write_log(log_file, log_str)
            os._exit(0)


if __name__ == '__main__':
    Master().start()
