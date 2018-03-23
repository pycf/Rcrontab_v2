"""循环检查数据库
    并更新抓取程序的执行计划
    和 计算脚本的执行计划
    提供方法查询抓取程序 执行计划
    和 方法 修改 执行计划
"""

import copy
import hashlib
import json
import threading
import time
from Rcrontab.master.MasterPackages import sql_codes
from Rcrontab.master.MasterPackages.send_to_slave import SendCmdClient
from Packages import mysql
from Packages.write_log import write_log
import datetime
from Rcrontab.master.MasterPackages.redis_conn import r

_programs_crawler_info = {}
'''抓取程序执行计划信息'''

_programs_calculate_info_dic = {}
"""保存计算程序的多个版本，
    执行过的程序将从版本列表中删除
    版本列表为空，则从dic中删除版本列表
    需要一个执行中列表保存正在执行的程序
"""
_programs_calculate_info_last = list()
"""计算程序最新程序全量，
    用于比较数据库的变化， 
    并更新计算程序集合中的数据
"""


class CatchScript(object):

    def __init__(self, sql_code, func):
        self.sql_code = sql_code
        self._cmd_list = []
        self.func = str(func)

    # 获取抓取程序 执行信息
    @staticmethod
    def _get_crawler_command(df, programs_info=None):
        if programs_info is None:
            programs_info = {}
        if len(df) == 0:
            return programs_info
        else:
            for idx in df.index:
                server = df.loc[idx, ['deploy_server']].values[0].strip()
                exec_info = df.loc[idx, ['sid', 'path', 'run_type', 'crontab']].to_dict()
                if server not in programs_info:
                    programs_info[server] = list()
                programs_info[server].append(exec_info)
            return programs_info

    # 获取计算程序 执行信息
    @staticmethod
    def _get_calculate_command(df, programs_info=None):
        if programs_info is None:
            programs_info = []
        if len(df) == 0:
            return programs_info
        else:
            for idx in df.index:
                programs_info += [df.loc[idx, ['sid', 'path', 'run_type', 'deploy_server', 'port']].to_dict(), ]
            return programs_info

    def run(self):
        programs_info = None
        sql = mysql.Mysql()
        df = sql.mysql_read(self.sql_code)
        if len(df) != 0:
            if self.func == 'crawler':
                programs_info = self._get_crawler_command(df, programs_info)
            elif self.func == 'calculate':
                programs_info = self._get_calculate_command(df, programs_info)
        return programs_info


# 将已经执行成功的程序从列表中删除
# 如果版本内所有程序执行完毕， 删除版本列表
class SetProgramsCalculateInfo:
    def __init__(self, sid, version):
        self.version = version
        self.sid = sid

    def program_start_running(self):
        global _programs_calculate_info_dic
        version = self.version
        sid = self.sid
        try:
            if version in _programs_calculate_info_dic:
                for program in _programs_calculate_info_dic[version]:
                    if int(sid) == int(program['sid']):
                        _programs_calculate_info_dic[version].remove(program)
                        r.set('maintain_plans_programs_calculate_info_dic', _programs_calculate_info_dic)
            else:
                raise Exception("SetProgramsCalculateInfoDic.program_start_running:"
                                "version is not in _programs_calculate_info_dic")
        except Exception as e:
            print(str(e))


# 监测 抓取程序执行计划信息 变化则返回更新后的列表
def diff_programs_info(sql_code, programs_info, func):
    programs_info_old = programs_info
    hash_old = hashlib.md5(json.dumps(programs_info_old).encode('utf-8')).hexdigest()
    # 转换成dict
    catch_script = CatchScript(sql_code, func)
    # 与执行计划比对
    programs_info_new = catch_script.run()

    if programs_info_new != 0:
        hash_new = hashlib.md5(json.dumps(programs_info_new).encode('utf-8')).hexdigest()
        if hash_new != hash_old:
            return programs_info_new
    return None


def last_version():
    last_version_num = datetime.date.today()
    return last_version_num


def loop_flush_crawler_info():
    while True:
        global _programs_crawler_info
        programs_info = copy.deepcopy(_programs_crawler_info)
        try:
            sql_code_crawler = sql_codes.get_crawler_sql_codes()
            programs_info_new = diff_programs_info(sql_code_crawler, programs_info, 'crawler')
            if programs_info_new:
                _programs_crawler_info = copy.deepcopy(programs_info_new)
                distribute = DistributePlans()
                distribute.distribute()
                r.set('maintain_plans_programs_crawler_info', _programs_crawler_info)
        except Exception as e:
            print(e)
            write_log(str(e))
        finally:
            time.sleep(30)


def loop_flush_calculate_info():
    while True:
        global _programs_calculate_info_last
        programs_info = list(copy.deepcopy(_programs_calculate_info_last))
        try:
            global _programs_calculate_info_dic
            version = last_version()
            sql_code_calculate = sql_codes.get_calculate_sql_codes()
            programs_info_new = diff_programs_info(sql_code_calculate, programs_info, 'calculate')
            # ("programs_info_new:", programs_info_new)
            if programs_info_new:
                if version not in _programs_calculate_info_dic:
                    # 如果队列有更新 且 没有最新的版本则将最新版本赋值为 最新的队列
                    _programs_calculate_info_dic[version] = copy.deepcopy(programs_info_new)
                else:
                    # 更新最新版本 队列的 计算程序计划任务
                    add_programs_list = list()
                    remove_programs_list = list()
                    for program in programs_info_new:
                        if program not in programs_info:
                            add_programs_list.append(program)
                    for program in programs_info:
                        if program not in programs_info_new:
                            remove_programs_list.append(program)
                    _programs_calculate_info_last = copy.deepcopy(programs_info_new)
                    # 将最新队列写入缓存
                    r.set('maintain_plans_programs_calculate_info_last', _programs_calculate_info_last)
                    for program in add_programs_list:
                        if program not in _programs_calculate_info_dic[version]:
                            _programs_calculate_info_dic[version].append(program)
                    for program in remove_programs_list:
                        if program in _programs_calculate_info_dic[version]:
                            _programs_calculate_info_dic[version].remove(program)
                # 写入缓存数据库
                r.set('maintain_plans_programs_calculate_info_dic', _programs_calculate_info_dic)
            else:
                # 如果队列没有更新 且 最新版本不存在则用目前的队列生成最新版本
                if version not in _programs_calculate_info_dic and len(programs_info) != 0:
                    _programs_calculate_info_dic[version] = copy.deepcopy(programs_info)
                    # 写入缓存数据库
                    r.set('maintain_plans_programs_calculate_info_dic', _programs_calculate_info_dic)
        except Exception as e:
            print(e)
            write_log(str(e))
        finally:
            # print("_programs_calculate_info_last:", _programs_calculate_info_last)
            # print("_programs_calculate_info_dic:", _programs_calculate_info_dic)
            time.sleep(30)


# 执行计划分发
class DistributePlans:
    def __init__(self):
        self.ip_list = list(_programs_crawler_info.keys())
        # print(self.ip_list)
        self.valid_slaves_ip_list = list()

    @staticmethod
    def _get_online_slaves():
        sql = mysql.Mysql()
        online_slaves = sql.mysql_read(sql_codes.get_online_slaves)
        # print("online_slaves: ", online_slaves)
        slaves_info_online = list()
        for idx in online_slaves.index:
            slaves_info_online.append(online_slaves.loc[idx, ['ip', 'port']].to_dict())
        return slaves_info_online

    def _get_valid_ip_list(self):
        try:
            slaves_info = self._get_online_slaves()
            # print("slaves_info: ", slaves_info)
            for ip in self.ip_list:
                for slave_info in slaves_info:
                    slave_ip = slave_info['ip']
                    if ip == slave_ip:
                        self.valid_slaves_ip_list.append(slave_info)
        except Exception as e:
            print("DistributePlan._get_valid_ip_list Err:", e)

    def distribute(self):
        try:
            programs_info = copy.deepcopy(_programs_crawler_info)
            self._get_valid_ip_list()
            # print("self.valid_slaves_ip_list:", self.valid_slaves_ip_list)
            if len(self.valid_slaves_ip_list) == 0:
                raise ValueError('DistributePlan get a ip_list with no valid slave_ip!')
            else:
                for slave_info in self.valid_slaves_ip_list:
                    ip = slave_info['ip']
                    port = slave_info['port']
                    print(ip, port)
                    if ip in programs_info:
                        cmd_list = programs_info[ip]
                        send_cmd = SendCmdClient(ip, port, cmd_list)
                        send_cmd.send()
                        log_string = "更新服务器: %s 的执行计划成功" % ip
                        write_log(log_string)
                        break
        except Exception as e:
            write_log('DistributePlan get err:', str(e))
            print('DistributePlans.distribute  Err:', str(e))


def run():
    global _programs_crawler_info
    global _programs_calculate_info_dic
    global _programs_calculate_info_last

    info = r.get('maintain_plans_programs_crawler_info')
    if info:
        try:
            _programs_crawler_info = json.loads(info)
        except json.decoder.JSONDecodeError:
            _programs_crawler_info = {}
    info = r.get('maintain_plans_programs_calculate_info_dic')
    if info:
        try:
            _programs_calculate_info_dic = json.loads(info)
        except json.decoder.JSONDecodeError:
            _programs_calculate_info_dic = {}
    info = r.get('maintain_plans_programs_calculate_info_last')
    if info:
        try:
            _programs_calculate_info_last = json.loads(info)
        except json.decoder.JSONDecodeError:
            _programs_calculate_info_last = {}

    threads = list()
    threads.append(threading.Thread(target=loop_flush_crawler_info))
    threads.append(threading.Thread(target=loop_flush_calculate_info))
    for thread in threads:
        thread.start()


if __name__ == '__main__':
    run()
