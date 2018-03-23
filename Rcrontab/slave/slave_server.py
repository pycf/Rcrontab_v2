# -*- coding: utf-8 -*-

import socketserver
import datetime
import json
import os
import time
import configparser
import threading
import sys
import subprocess
from urllib import request, error
base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(base_path)
from Packages.write_log import write_log
from Rcrontab.slave.send_data import SendToMaster
from crontab import CronTab


"""
Created on Fri Oct 13 12:13:15 2017

@author: Administrator
"""
service_conf_path = os.path.join("/", base_path, "config/service.conf")

conf = configparser.ConfigParser()
conf.read(service_conf_path)
slave_host = conf.get("slave", "SLAVE_HOST")
slave_port = int(conf.get("slave", "SLAVE_PORT"))
slave_log = conf.get("slave", "SLAVE_LOG_FILE")
_slave_status = 0
_programs_list = {}


# slave 服务端
class MyServer(socketserver.BaseRequestHandler):
    # 数据类型定义
    task_list = {0: "_execution_map",  # slave 方法
                 3: "_stop_slave",  # slave 方法
                 5: "_program_exec",  # slave 方法
                 9: "_hello"  # slave 方法
                 }
    maps = {}

    def handle(self):
        global slave_log
        global _programs_list
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
        print('=================', self.maps, '=================')
        msg_type = int(self.maps['type'])
        self._distribute(msg_type)

    def _distribute(self, msg_type):
        try:
            if msg_type in self.task_list:
                func_name = self.task_list[msg_type]
                if hasattr(self, func_name):
                    func = getattr(self, func_name)
                    func()
                else:
                    self.request.close()
            else:
                self.request.close()
        except Exception as err:
            write_log(err, name=slave_log)
            self.request.close()

    # 执行计划更新
    def _execution_map(self):
        cron = CronTab()
        for program in self.maps['list']:
            crontab = program['crontab']
            cmd = GetCommand(program)()
            job = cron.new(command=cmd)
            job.setall(crontab)
            if job.is_valid():
                job.enable()
        log_str = "slave 服务器 执行计划更新，执行计划: %s" % cron
        print(log_str)
        cron.write_to_user(user='crontab')
        self.request.sendall("ok".encode('utf-8'))

    def _hello(self):
        send_data = "hello_ack".encode('utf-8')
        conn = self.request
        conn.sendall(send_data)
        self.request.close()

    # close slave service
    @staticmethod
    def _stop_slave():
        _set_slave_status(1)

    # 执行master发来的程序
    def _program_exec(self):
        program_info = self.maps['program_info']
        sid = int(program_info['sid'])
        program = program_info['path']
        run_type = program_info['run_type']
        result_id = None
        if 'id' in program_info:
            result_id = program_info['id']
        version = None
        if 'version' in program_info:
            version = program_info['version']
        print(sid, program, run_type, version)
        exec_program = ExecProgram(sid, program, run_type, version=version, result_id=result_id)
        thread = threading.Thread(target=exec_program)
        thread.start()
        # 跳出所有循环


#  program 扫描
class GetCommand:

    def __init__(self, program_info):
        self.program_info = program_info
        self.sid = program_info['sid']
        self.program = program_info['path']
        self.run_type = program_info['run_type']

    def __call__(self, *args, **kwargs):
        if self.run_type == 1:
            return self._exec_program()
        else:
            return self._url_program()

    def _url_program(self):
        parameter_str = "?sid={sid}\&version=$(date '+\%F')".format(sid=self.sid)
        get_url = "curl  " + self.program + parameter_str
        return get_url

    def _exec_program(self):
        if os.path.splitext(self.program)[1] == '.jar':
            get_cmd = "java  -jar" + self.program
        elif os.path.splitext(self.program)[1] == '.sh':
            get_cmd = "sh " + self.program
        elif os.path.splitext(self.program)[1] == '.py':
            get_cmd = "python -u " + self.program
        else:
            get_cmd = "%{program} 程序类型仅支持 shell(.sh) python(.py) java(.jar)".format(program=self.program)
        return get_cmd


class ExecProgram:

    def __init__(self, sid, program, run_type, version=None, subversion=None, result_id=None):
        self.sid = sid
        self.program = program
        self.run_type = run_type
        self.subversion = subversion
        if subversion:
            self.subversion = subversion
        else:
            self.subversion = ""
        if version:
            self.version = version
        else:
            self.version = time.strftime("%Y-%m-%d")
        self.result_id = result_id

    def __call__(self, *args, **kwargs):
        if self.run_type == 1:
            self._exec_program()
        else:
            self._url_exec_program()

    def _exec_program(self):
        self._pre_exec()
        time.sleep(5)
        try:
            if os.path.splitext(self.program)[1] == '.jar':
                proc = subprocess.Popen(["java", "-jar", self.program],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
                out_value, err_value = proc.communicate()
            elif os.path.splitext(self.program)[1] == '.sh':
                proc = subprocess.Popen(["sh", self.program], stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
                out_value, err_value = proc.communicate()
            elif os.path.splitext(self.program)[1] == '.py':
                proc = subprocess.Popen(["python", self.program], stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
                out_value, err_value = proc.communicate()
            else:
                out_value = ""
                err_value = "程序类型仅支持 shell(.sh) python(.py) java(.jar)"
            if not err_value:
                extra_info = out_value.decode('utf-8')
                result = 0
            else:
                extra_info = err_value.decode('utf-8')
                result = 1
            print("out_value:", out_value, "err_value", err_value)
        except Exception as e:
            result = 1
            extra_info = str(e)
        finally:
            self._after_exec(result, extra_info)

    def _url_exec_program(self):
        time.sleep(5)
        url_path = self._join_url_parameter()
        print(url_path)
        result = 1
        extra_info = "url调用未执行"
        try:
            request.urlopen(url_path)
            result = 0
        except error.HTTPError:
            result = 2
            extra_info = "Http 访问错误"
        except error.URLError:
            result = 3
            extra_info = "Url 路径错误"
        except Exception as e:
            result = 4
            extra_info = str(e)
        finally:
            if result == 0:
                string = 'sid:%s program:%s is accessed !result: %s' % (self.sid, url_path, result)
                extra_info = ''
            else:
                string = '[ err ] sid:%s program:%s is  !result: %s, error info: %s' \
                         % (self.sid, self.program, result, extra_info)
                extra_info = string
            print(string)
            write_log(string, name=slave_log)
            self._pre_exec(extra_info)

    def _join_url_parameter(self):
        parameter_str = "?sid={sid}&version={version}".format(sid=self.sid, version=self.version)
        extra_parameter = ""
        if self.result_id:
            extra_parameter += "&id={id}".format(id=self.result_id)
        if self.subversion:
            extra_parameter += "&subversion={subversion}".format(subversion=self.subversion)
        parameter_str += extra_parameter
        url_path = self.program + parameter_str
        return url_path

    def _pre_exec(self, extra_info=''):
        ti = time.strftime('%Y-%m-%d %H:%M:%S')
        msg_type = 1
        script_map = {'sid': self.sid, 'time': ti, 'program': self.program,
                      'version': self.version, 'extra_info': extra_info, 'subversion': self.subversion}
        string = 'sid:%s program:%s is running start !' % (self.sid, self.program)
        print(string)
        write_log(string, name=slave_log)
        send_master = SendToMaster(msg_type, script_map)
        send_master.sending()

    def _after_exec(self, result, extra_info=""):
        ti = time.strftime('%Y-%m-%d %H:%M:%S')
        msg_type = 4
        script_map = {'sid': self.sid, 'time': ti, 'program': self.program, "result": result,
                      "extra_info": extra_info, 'version': self.version, 'subversion': self.subversion}
        string = 'sid:%s program:%s is running ended !result: %s' % (self.sid, self.program, result)
        write_log(string, name=slave_log)
        send_master = SendToMaster(msg_type, data=script_map)
        send_master.sending()


# 开启 slave 服务端
class SlaveServer:

    def start(self):
        self._monitor_stop()
        self._server_start()

    @staticmethod
    def _server_start():
        global slave_port
        global slave_host
        host = str(slave_host)
        port = int(slave_port)
        s_server = socketserver.ThreadingTCPServer((host, port), MyServer)
        with s_server:
            log_str = 'slave 服务器启动! bind_ip: %s bind_port: %s' % (host, port)
            print(log_str)
            write_log(log_str, name=slave_log)
            s_server.serve_forever()

    @staticmethod
    def _monitor_stop():
        log_str = '<slave服务状态监控启动>'
        print(log_str)
        write_log(log_str, name=slave_log)
        t = threading.Thread(target=_stop)
        t.setDaemon(True)
        t.start()


def _get_slave_status():
    global _slave_status
    return _slave_status


def _set_slave_status(value):
    global _slave_status
    _slave_status = value


def _stop():
    while True:
        status = _get_slave_status()
        time.sleep(3)
        if status == 1:
            log_str = 'slave服务器 关闭'
            write_log(log_str, name=slave_log)
            sys.exit(0)


def start():
    slave = SlaveServer()
    t1 = threading.Thread(target=_stop)
    t1.setDaemon(True)
    t1.start()
    slave.start()


if __name__ == "__main__":
    start()
