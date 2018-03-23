from Rcrontab.master.MasterPackages.sql_codes import get_start_flag, get_old_version, update_flag, \
    get_end_flag, get_execute_success_programs, update_version, get_execute_failed_programs, get_program_base_info, \
    get_owner_info_from_sid
from datetime import timedelta
from Packages import mysql
from Rcrontab.master.maintain_plans import SetProgramsCalculateInfo
from Rcrontab.master.MasterPackages.send_to_slave import SendProgram
import threading
import time
import json
from Rcrontab.master.MasterPackages.send_mail import create_mail
from Rcrontab.master.MasterPackages.redis_conn import r
# from Rcrontab.master.subversion_queue.maintain_sub_plan import ControlQualityResult
import datetime


class LoopCheckProgramResult:
    def __init__(self):
        self.program_rerun_dict = {}
        rerun_dict = r.get('program_rerun_dict')
        if rerun_dict:
            rerun_dict = json.loads(rerun_dict)
            try:
                for k, v in rerun_dict.items():
                    v_dick = {}
                    for k1, v1 in v.items():
                        k1 = datetime.date(*map(int, k1.split('-')))
                        v_dick[k1] = int(v1)
                    self.program_rerun_dict[int(k)] = v_dick
            except Exception as e:
                print("LoopCheckProgramResult Err: "+str(e))
                self.program_rerun_dict = {}
        else:
            self.program_rerun_dict = {}

    def run(self):
        while True:
            try:
                self.loop_check_program_result()
            except Exception as e:
                print(e)
            finally:
                time.sleep(20)

    def loop_check_program_result(self):

        start_id, end_id = self._get_max_flag()
        if start_id != end_id:
            execute_success_programs = self._get_execute_success_programs(start_id, end_id)
            execute_failed_programs = self._get_execute_failed_programs(start_id, end_id)
            # print("execute_success_programs:", execute_success_programs)
            # print("execute_failed_programs:", execute_failed_programs)
            threads = []
            if len(execute_success_programs) != 0:
                threads.append(threading.Thread(target=self.success_procedure, args=(execute_success_programs, )))
            if len(execute_failed_programs) != 0:
                threads.append(threading.Thread(target=self.failed_procedure, args=(execute_failed_programs,)))
            if threads:
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
            try:
                if self.program_rerun_dict:
                    rerun_dict = {}
                    v_dict = {}
                    print("self.program_rerun_dict:", self.program_rerun_dict)
                    for k, v in self.program_rerun_dict.items():
                        if len(v) != 0:
                            for k1, v1 in v.items():
                                v_dict[str(k1)] = v1
                            rerun_dict[str(k)] = v_dict
                    rerun_dict = json.dumps(rerun_dict)
                    print("rerun_dict:", rerun_dict)
                    r.set('program_rerun_dict', rerun_dict)
            except Exception as e:
                print("loop_check_program_result.loop_check_program_result Err: "
                      "[program_rerun_dict] {err}".format(err=str(e)))
            finally:
                sql = mysql.Mysql()
                sql.mysql_write(update_flag.format(id=end_id))

    def success_procedure(self, execute_success_programs):
        for program in execute_success_programs:
            sid = program['sid']
            version = program['version']
            subversion = program['subversion']
            extra_info = program['extra_info']
            send_mail = False
            if extra_info:
                extra_info_dict = json.loads(extra_info)
                for key in extra_info_dict:
                    if len(extra_info_dict[key]) != 0:
                        send_mail = True
                        break
            if send_mail:
                if not subversion:
                    subversion_str = "main"
                else:
                    subversion_str = subversion
                sql = mysql.Mysql()
                df = sql.mysql_read(get_owner_info_from_sid.format(sid=sid))
                if len(df) != 0:
                    program_api, owner_name, mail = (df.loc[0, 'name'], df.loc[0, 'owner'], df.loc[0, 'mail'])
                    msg = "{program_api}[{version}-{subversion}]:{info}".\
                        format(program_api=program_api, version=version, info=extra_info, subversion=subversion_str)
                    create_mail(msg, str(mail), to=owner_name,
                                subject='程序{program_api}执行成功【告警】信息'.format(program_api=program_api))
                else:
                    print("找不到邮件发送人！")

            """质控流程
            # quality_result = ControlQualityResult(sid, version, subversion)
            # quality_result.run()
            """
            if subversion:
                pass
                """重跑流程"""
            else:
                SetProgramsCalculateInfo(sid, version).program_running_success()
                day = timedelta(days=1)
                new_version = (version + day)
                self._update_version(sid, new_version)
                if sid in self.program_rerun_dict:
                    if version in self.program_rerun_dict[sid]:
                        del(self.program_rerun_dict[sid][version])

    def failed_procedure(self, execute_failed_programs):
        for program in execute_failed_programs:
            program_id = program['id']
            sid = program['sid']
            version = program['version']
            subversion = program['subversion']
            extra_info = program['extra_info']
            sql = mysql.Mysql()
            df = sql.mysql_read(get_program_base_info.format(sid=sid))
            if len(df) == 0:
                print("LoopCheckProgramResult.failed_procedure Err: sid({sid}) 不存在!".format(sid=sid))
                continue
            program_info = df.loc[0, ['sid', 'path', 'run_type', 'deploy_server', 'port', 'times']].to_dict()
            program_info['version'] = version
            program_info['id'] = program_id
            slave_ip = program_info['deploy_server']
            slave_port = program_info['port']
            times = program_info['times']
            if subversion:
                pass
                """重跑流程"""
            else:
                if program_info['times'] == 0:
                    re_execute = 0
                else:
                    if sid in self.program_rerun_dict:
                        if version in self.program_rerun_dict[sid]:
                            if self.program_rerun_dict[sid][version] != 0:
                                re_execute = 1
                            else:
                                # 计数器为0 删除计数器
                                del(self.program_rerun_dict[sid][version])
                                if len(self.program_rerun_dict[sid]) == 0:
                                    del (self.program_rerun_dict[sid])
                                re_execute = 0
                        else:
                            self.program_rerun_dict[sid][version] = times
                            re_execute = 1
                    else:
                        self.program_rerun_dict[sid] = {}
                        self.program_rerun_dict[sid][version] = times
                        re_execute = 1
                if re_execute == 1:
                    send_program = SendProgram(slave_ip, slave_port, program_info)
                    send_program.send()
                    self.program_rerun_dict[sid][version] -= 1
                else:
                    SetProgramsCalculateInfo(sid, version).program_running_failed()
                    # 发送邮件
                    subversion_str = "main"
                    sql = mysql.Mysql()
                    df = sql.mysql_read(get_owner_info_from_sid.format(sid=sid))
                    program_api, owner_name, mail = (df.loc[0, 'name'], df.loc[0, 'owner'], df.loc[0, 'mail'])
                    msg = "{program_api}[{version}-{subversion}]:{info}". \
                        format(program_api=program_api, version=version, info=extra_info, subversion=subversion_str)
                    create_mail(msg, str(mail), to=owner_name,
                                subject='程序{program_api}执行错误！【错误】信息'.format(program_api=program_api))
            # print("program_rerun_dict:", self.program_rerun_dict)

#    def quality_success_procedure(self, quality_success_programs):
#        for program in quality_success_programs:
#            sid = program['sid']
#            version = program['version']
#            subversion = program['subversion']
#            SetProgramsCalculateInfo(sid, version).program_running_success()
#            day = timedelta(days=1)
#            new_version = (version + day)
#            self._update_version(sid, new_version)

    @staticmethod
    def _get_max_flag():
        sql1 = mysql.Mysql()
        df1 = sql1.mysql_read(get_start_flag)
        sql2 = mysql.Mysql()
        df2 = sql2.mysql_read(get_end_flag)
        try:
            start_id = int(df1.loc[0, 'id'])
        except Exception as e:
            start_id = 0
        finally:
            try:
                end_id = int(df2.loc[0, 'id'])
            except Exception as e:
                end_id = 0
            finally:
                return (start_id, end_id)

    @staticmethod
    def _get_execute_success_programs(start_id, end_id):
        execute_success_programs = list()
        if start_id == end_id:
            pass
        else:
            sql = mysql.Mysql()
            df = sql.mysql_read(get_execute_success_programs.format(start_id=start_id, end_id=end_id))
            if len(df) == 0:
                pass
            else:
                for idx in df.index:
                    info = df.loc[idx, ['sid', 'version', 'subversion', 'extra_info']].to_dict()
                    execute_success_programs.append(info)
        return execute_success_programs

    @staticmethod
    def _update_version(sid, new_version):
        sql = mysql.Mysql()
        df = sql.mysql_read(get_old_version.format(sid=sid))
        if len(df) == 0:
            print("_update_version ERR: {sid} can't find!".format(sid=sid))
            return
        old_version = df.loc[0, 'version']
        if new_version > old_version or old_version is None:
            sql = mysql.Mysql()
            sql.mysql_write(update_version.format(version=str(new_version), sid=sid))

    @staticmethod
    def _get_execute_failed_programs(start_id, end_id):
        execute_failed_programs = list()
        if start_id == end_id:
            pass
        else:
            sql = mysql.Mysql()
            df = sql.mysql_read(get_execute_failed_programs.format(start_id=start_id, end_id=end_id))
            if len(df) == 0:
                pass
            else:
                for idx in df.index:
                    info = df.loc[idx, ['id', 'sid', 'version', 'subversion', 'extra_info']].to_dict()
                    execute_failed_programs.append(info)
        return execute_failed_programs

#    @staticmethod
#    def _get_quality_success_programs(start_id, end_id):
#        execute_failed_programs = list()
#        if start_id == end_id:
#            pass
#        else:
#            sql = mysql.Mysql()
#            df = sql.mysql_read(get_quality_success_programs.format(start_id=start_id, end_id=end_id))
#            if len(df) == 0:
#                pass
#            else:
#                for idx in df.index:
#                    info = df.loc[idx, ['id', 'sid', 'version', 'subversion', 'extra_info']].to_dict()
#                    execute_failed_programs.append(info)
#        return execute_failed_programs


if __name__ == "__main__":
    LoopCheckProgramResult().loop_check_program_result()
