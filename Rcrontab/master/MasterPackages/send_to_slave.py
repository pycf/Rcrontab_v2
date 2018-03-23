"""
定义所有发送给客户端数据的行为
"""

import json
import socket
from Rcrontab.master.MasterPackages.sql_codes import update_slave_status
from Packages.mysql import Mysql
from Packages.write_log import write_log
import time
import copy
from Rcrontab.master.MasterPackages.sql_codes import get_online_slaves
from Packages import mysql


_slave_status = {}


class SlaveStatus:

    @staticmethod
    def load_slave_status():
        global _slave_status
        sql = mysql.Mysql()
        online_slaves = sql.mysql_read(get_online_slaves)
        slaves_info_online = list()
        if len(online_slaves) != 0:
            for idx in online_slaves.index:
                slaves_info_online.append(online_slaves.loc[idx, ['ip', 'port']].to_dict())
            if len(slaves_info_online) != 0:
                for slave_info in slaves_info_online:
                    _slave_status[slave_info['ip']] = {"port": slave_info['port'], "count": 0}

    @staticmethod
    def get_slave_status():
        global _slave_status
        slave = copy.deepcopy(_slave_status)
        return slave


# 连接 slave 服务器 --父类,向slave端传送数据继承于此类
class ToClient(object):
    data = ''

    def __init__(self, slave_ip, slave_port, data=None):
        self.ip = slave_ip
        self.port = int(slave_port)
        self.data = data
        self.send_data = ''
        if data is None:
            raise ValueError('Data send to slave can not be None!')

    # 定义连接slave行为
    def send(self):
        # 处理数据
        self._data_processing()
        address = (self.ip, self.port)
        send_data = self.send_data.encode('utf-8')
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            c.connect(address)
            length = len(send_data)
            c.sendall(str(length).encode('utf-8'))
            slave_res = c.recv(1024).decode('utf-8')
            if slave_res == "ok":
                # print("start to send data")
                # print(send_data)
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
        self.send_data = self.data

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


# 发送执行计划 到 slave 服务器
class SendCmdClient(ToClient):

    def __init__(self, slave_ip, slave_port, cmd_list):
        try:
            self.data = {"type": "0", "list": cmd_list}
        except Exception as e:
            write_log('SendCmdClient Err:', str(e))
            print('SendCmdClient Err:', str(e))
        finally:
            super(SendCmdClient, self).__init__(slave_ip, slave_port, self.data)

    # 数据处理将字典-》json字符串
    def _data_processing(self):
        self.send_data = json.dumps(self.data)

    def _connect_success(self):
        file_str = "更新服务器: %s 执行计划" % self.ip
        write_log(file_str)


# 发送hello包
class SendHello(ToClient):

    def __init__(self, slave_ip, slave_port, slave_id):
        self.data = {"type": "9"}
        super(SendHello, self).__init__(slave_ip, slave_port, self.data)
        self.sql_code = update_slave_status
        self.slave_id = slave_id

    # 数据处理将字典->json字符串
    def _data_processing(self):
        self.send_data = json.dumps(self.data)

    def _response(self, receive, conn):
        global _slave_status
        if receive == "hello_ack":
            if self.ip in _slave_status:
                _slave_status[self.ip]["count"] = 0
            else:
                _slave_status[self.ip] = {"port": self.port, "count": 0}
                log_string = "服务器: %s 上线 " % self.ip
                # 需要更新数据库服务器状态
                try:
                    update_time = time.strftime("%Y-%m-%d %H:%M:%S")
                    sql_code = self.sql_code.format(status=1, uptime=update_time, id=self.slave_id)
                    sql = Mysql()
                    sql.mysql_write(sql_code)
                    # print(sql_code)
                except Exception as e:
                    print("SendHello._connect_err Err:", str(e))
                    write_log("SendHello._connect_err Err:", str(e))
                finally:
                    write_log(log_string)
                    time.sleep(10)
                    send_plan = DistributePlans(ip_list=(self.ip, ))
                    send_plan.distribute()
        conn.close()

    def _connect_err(self, e):
        global _slave_status
        if self.ip in _slave_status:
            if _slave_status[self.ip]["count"] < 3:
                _slave_status[self.ip]["count"] += 1
            elif _slave_status[self.ip]["count"] == 3:
                del(_slave_status[self.ip])
                log_string = "服务器: %s 下线 !" % self.ip
                # 需要更新数据库状态
                try:
                    update_time = time.strftime("%Y-%m-%d %H:%M:%S")
                    sql_code = self.sql_code.format(status=0, uptime=update_time, id=self.slave_id)
                    # print(sql_code)
                    sql = Mysql()
                    sql.mysql_write(sql_code)
                except Exception as e:
                    print("SendHello._connect_err Err:", str(e))
                    write_log("SendHello._connect_err Err:", str(e))
                finally:
                    write_log(log_string)
            else:
                del(_slave_status[self.ip])


# 发送计算程序任务到slave节点
class SendProgram(ToClient):

    def __init__(self, slave_ip, slave_port, program_info,):
        if 'id' in program_info:
            program_info['id'] = str(program_info['id'])
        if 'version' in program_info:
            if not isinstance(program_info['version'], str):
                program_info['version'] = str(program_info['version'])
        print("program_info:", program_info)
        self.data = {'type': "5", 'program_info': program_info}
        super(SendProgram, self).__init__(slave_ip, slave_port, self.data)

    # 数据处理将字典-》json字符串
    def _data_processing(self):
        self.send_data = json.dumps(self.data)


