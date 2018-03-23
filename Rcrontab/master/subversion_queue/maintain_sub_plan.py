import queue
from Packages.mysql import Mysql
from Rcrontab.master.MasterPackages.sql_codes import get_result_tables, program_result_log
from Rcrontab.master.MasterPackages import get_parents_sons
from Rcrontab.master.MasterPackages.redis_conn import r
import time
import json
quality_programs = queue.Queue(1000)
_quality_result = {}


class ControlQualityResult:

    def __init__(self, sid, version, subversion=None):
        self.subversion_dict = {}
        self.sid = sid
        self.version = str(version)
        self.subversion = subversion
        sql_code = '''select program_type from  py_script_base_info_v2 where sid={sid}'''.format(sid=self.sid)
        mysql = Mysql()
        df = mysql.mysql_read(sql_code)
        self.program_type = int(df.loc[0, ['program_type',]])
        print('program_type:', self.program_type)

    def get_quality_result(self):  # 获取质控结果
        sql = Mysql()
        df = sql.mysql_read(get_result_tables.format(sid=self.sid))
        table_quality_result = list()
        if len(df) != 0:
            for idx in df.index:
                table_info = df.loc[idx, ['id', 'db_server', 'db_name', 'table_name']].to_dict()

                # 调用质控
                is_err = quality(table_info['id'], table_info['table_name'],
                                 self.version, self.subversion)

                if is_err == 1:
                    table_quality_result.append(id)
        info = table_quality_result
        return info

    def get_descendants(self):
        sid = int(self.sid)
        descendants = set()
        new_descendants = set()
        descendants.add(sid)
        new_descendants.add(sid)
        while True:
            tmp_list = list()
            for p_dict in new_descendants:
                sid = p_dict['sid']
                tmp_list.append(get_parents_sons.get_sons(sid))
            if len(tmp_list) == 0:
                return descendants
            else:
                for program_info in tmp_list:
                    sid = int(program_info['sid'])
                    new_descendants.add(sid)
                    descendants.add(sid)

    def run(self):
        global _quality_result
        err_tables_list = self.get_quality_result()
        if self.program_type == 0:  # 抓取程序
            key = self.version
            if key in _quality_result:
                _quality_result[key].append({str(self.sid): err_tables_list})
            else:
                _quality_result[key] = [{str(self.sid): err_tables_list}, ]
            r.set('quality_result', json.dumps(_quality_result))


def quality(tid, table_name, version, subversion):
    print(tid, table_name, version, subversion)
    info = 0
    return info


