# -*- coding: utf-8 -*-
import socketserver
import socket
import datetime
import json
import os
import time
from Packages import write_log
import configparser
"""
Created on Fri Oct 13 12:13:15 2017

@author: Administrator
"""

""
conf = configparser.ConfigParser()
conf.read("service.conf")
master_ip = conf.get("slave", "MASTER_IP_OUTER")
master_port = int(conf.get("slave", "MASTER_PORT_OUTER"))
slave_host = conf.get("slave", "SLAVE_HOST")
slave_port = int(conf.get("slave", "SLAVE_PORT"))
slave_log = conf.get("slave", "SLAVE_LOG_FILE")


class ExecScript:
    def __init__(self, script, sid):
        self.script = script
        self.sid = sid

    def exec_script(self):
        time.sleep(5)
        os.system(self.script)
        self.reply_master()

    def reply_master(self):
        global master_ip
        global master_port
        ti = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        script_map = {'sid': self.sid, 'time': ti, 'spt': self.script}
        string = '[%s] sid:%s spt:%s is done !' % (ti, self.sid, self.script)
        print(string)
        write_log.WriteLog.write_log(slave_log, string)
        script_str = json.dumps(script_map).encode('utf-8')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((master_ip, master_port))
            s.sendall(script_str)
            response = s.recv(1024).decode('utf-8')
            print("[%s] : %s" % (script_map['sid'], response))


class MyServer(socketserver.BaseRequestHandler):

    def handle(self):
        global slave_log
        ti = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = self.request
        print('已经连接:', self.client_address)
        da = conn.recv(1024).decode('utf-8')
        maps = json.loads(da)
        string = ('[%s] script_name : %s ,sid: %s ' % (str(ti), maps['spt'], maps['sid']))
        print(string)
        write_log.WriteLog.write_log(slave_log, string)
        response = "ok"
        conn.sendall(response.encode('utf-8'))
        ex_s = ExecScript(maps['spt'], maps['sid'])
        ex_s.exec_script()


class SlaveServer:
    @staticmethod
    def start():
        global slave_port
        global slave_host
        host = str(slave_host)
        port = int(slave_port)
        print(host, port)
        s_server = socketserver.ThreadingTCPServer((host, port), MyServer)
        print('slave_server start !')
        with s_server:
            s_server.serve_forever()


if __name__ == "__main__":
    slave = SlaveServer()
    slave.start()

