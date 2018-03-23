import configparser
import json
import os
import socket
import sys

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_path)
from Rcrontab.slave import slave_server
from Rcrontab.master import master_server

service_path = os.path.join("/", base_path, "config/service.conf")
conf = configparser.ConfigParser()
conf.read(service_path)
role = conf.get("role", "ROLE")
threads = []


def send_to_master(msg_type, **kwargs):
    ip = conf.get("master", "MASTER_IP_INNER")
    port = int(conf.get("master", "MASTER_PORT"))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        string = {'type': msg_type}
        if kwargs:
            string = dict(string, **kwargs)
            print(string)
        send_str = json.dumps(string).encode('utf-8')
        print(ip, port)
        s.connect((ip, port))
        s.sendall(send_str)
        data = s.recv(1024).decode('utf-8')
        print(data)


class Server:
    global role

    def __init__(self, *args):
        self.args = args

    def start(self):
        print("This server role is :", role)
        if role == 'slave':
            self._slave_run()
        elif role == 'master':
            self._master_run()

    @staticmethod
    def stop():
        if role == 'slave':
            ip = conf.get("slave", "SLAVE_HOST")
            port = int(conf.get("slave", "SLAVE_PORT"))
            msg_type = 3
        elif role == 'master':
            ip = conf.get("master", "MASTER_IP_INNER")
            port = int(conf.get("master", "MASTER_PORT"))
            msg_type = 2
        else:
            return "stop service failed!"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            string = {'type': msg_type}
            send_str = json.dumps(string).encode('utf-8')
            s.connect((ip, port))
            s.sendall(send_str)

    @staticmethod
    def get_alive_slaves():
        send_to_master(8)

    @staticmethod
    def redistribute_plan():
        send_to_master(7)

    @staticmethod
    def _slave_run():
        slave_server.star()

    @staticmethod
    def _master_run():
        master = master_server.Master()
        master.start()

    def rerun_program(self):
        sid = {'sid': self.args[0], }
        send_to_master(5, **sid)


def _main():
    print(sys.argv)
    if hasattr(Server, sys.argv[1]):
        server = Server(*sys.argv[2:])
        func = getattr(server, sys.argv[1])
        func()
    else:
        print("Can't find  function")


if __name__ == '__main__':
    # Server.redistribute_plan()
    # Server.rerun_program()
    _main()
