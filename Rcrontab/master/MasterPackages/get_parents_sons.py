from Rcrontab.master.MasterPackages.sql_codes import get_parents,get_sons
from Packages.mysql import Mysql
from Packages.write_log import write_log


class ParentsSons:
    def __init__(self, sid):
        if isinstance(sid, str):
            self.sid = int(sid)
        elif isinstance(sid, int):
            self.sid = sid
        else:
            err_str = "ParentsSons Err: program sid type should be Int!"
            write_log(err_str)
            raise ValueError(err_str)

    @property
    def parents(self):
        programs_list = list()
        try:
            sql = Mysql()
            df = sql.mysql_read(get_parents.format(sid=self.sid))
            if len(df) != 0:
                for idx in df.index:
                    program_info = df.loc[idx, ['sid', 'version', 'path',
                                                'deploy_server', 'port', 'run_type']].to_dict()
                    programs_list.append(program_info)
        except Exception as e:
            err_str = "ParentsSons.parents Err :" + str(e)
            write_log(err_str)
            print(err_str)
        finally:
            return programs_list

    @property
    def sons(self):
        programs_list = list()
        try:
            sql = Mysql()
            df = sql.mysql_read(get_sons.format(sid=self.sid))
            programs_list = list()
            if len(df) != 0:
                for idx in df.index:
                    program_info = df.loc[idx, ['sid', 'version', 'path',
                                                'deploy_server', 'port', 'run_type']].to_dict()
                    programs_list.append(program_info)
        except Exception as e:
            err_str = "ParentsSons.sons Err :" + str(e)
            write_log(err_str)
            print(err_str)
        return programs_list


if __name__ == '__main__':
    sons = ParentsSons(2)
    print(sons.sons)
