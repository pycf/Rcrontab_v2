import threading
import time

from Rcrontab.master.MasterPackages.send_to_slave import SendHello
from Rcrontab.master.MasterPackages.sql_codes import get_online_slaves, get_all_slaves
from Packages import mysql
from Packages.write_log import write_log
from Rcrontab.master.MasterPackages import send_to_slave

# 执行 发送 hello 信息


class SendingHello:
    @staticmethod
    def _get_online_slaves():
        sql = mysql.Mysql()
        online_slaves = sql.mysql_read(get_online_slaves)
        slaves_info = list()
        for idx in online_slaves.index:
            slaves_info.append(online_slaves.loc[idx, ['id', 'ip', 'port']].to_dict())
        return slaves_info

    @staticmethod
    def _get_all_slaves():
        sql = mysql.Mysql()
        online_slaves = sql.mysql_read(get_all_slaves)
        slaves_info = list()
        for idx in online_slaves.index:
            slaves_info.append(online_slaves.loc[idx, ['id', 'ip', 'port']].to_dict())
        return slaves_info

    def run(self):
        send_to_slave.SlaveStatus().load_slave_status()
        while True:
            waite_t = 5 - time.time() % 5
            # a = send_to_slave.SlaveStatus().get_slave_status()
            time.sleep(waite_t)
            slaves_info = self._get_all_slaves()
            # print(slaves_info)
            threads = list()
            try:
                for slave in slaves_info:
                    ip = slave['ip']
                    port = slave['port']
                    slave_id = slave['id']
                    send_hello = SendHello(ip, int(port), slave_id)
                    threads.append(threading.Thread(target=send_hello.send))
                for thread in threads:
                    thread.start()
            except Exception as e:
                write_log("SendingHello Err:", str(e))
                print("SendingHello Err:", str(e))


