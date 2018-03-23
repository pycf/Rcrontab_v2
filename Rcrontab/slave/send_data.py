from socket import socket
import json
import os
import configparser
from Packages.write_log import write_log

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
service_conf_path = os.path.join("/", base_path, "config/service.conf")

conf = configparser.ConfigParser()
conf.read(service_conf_path)
master_ip = conf.get("slave", "MASTER_IP_OUTER")
master_port = int(conf.get("slave", "MASTER_PORT_OUTER"))


# 给master服务器发送数据
# class SendToMaster:
#
#     def __init__(self, msg_type, data):
#         self.msg_type = msg_type
#         self.data = data
#
#     def sending(self):
#         global master_ip
#         global master_port
#         script_map = {'type': str(self.msg_type), "data": self.data}
#         script_str = json.dumps(script_map).encode('utf-8')
#         with socket() as s:
#             s.connect((master_ip, master_port))
#             s.sendall(script_str)
#             response = s.recv(4096).decode('utf-8')
#             if response:
#                 self._response(response)
#
#     def _response(self, response):
#         pass


class SendToMaster:

    def __init__(self, msg_type, data=None):
        global master_ip
        global master_port
        self.msg_type = msg_type
        self.ip = master_ip
        self.port = int(master_port)
        self.data = data
        self.send_data = ''
        if data is None:
            raise ValueError('Data send to slave can not be None!')

    # 定义连接slave行为
    def sending(self):
        # 处理数据
        self._data_processing()
        address = (self.ip, self.port)
        send_data = self.send_data
        c = socket()
        try:
            c.connect(address)
            length = len(send_data)
            c.sendall(str(length).encode('utf-8'))
            res = c.recv(1024).decode('utf-8')
            if res == "ok":
                # print("start to send data")
                print(send_data)
                while send_data:
                    size = c.send(send_data)
                    send_data = send_data[size:]

                self._connect_success()
                slave_response = c.recv(1024).decode('utf-8')
                if slave_response:
                    self._response(slave_response, c)
                if not slave_response:
                    c.close()
        except Exception as e:
            self._connect_err(e)
        finally:
            c.close()

    # 对数据进行处理，处理成字符串类型
    def _data_processing(self):
        script_map = {'type': str(self.msg_type), "data": self.data}
        self.send_data = json.dumps(script_map).encode('utf-8')

    # 对输出进行处理
    def _response(self, receive, conn):
        if receive == "ok":
            conn.close()

    # 连接成功后
    def _connect_success(self):
        pass

    # 连接异常
    def _connect_err(self, e):
        err_str = "连接服务器 :%s 异常,错误信息:%s" % (self.ip, str(e))
        write_log(err_str)


