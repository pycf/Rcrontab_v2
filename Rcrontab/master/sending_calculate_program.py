from Rcrontab.master.MasterPackages.send_to_slave import SendProgram
from Rcrontab.master import maintain_plans
from Rcrontab.master.MasterPackages.get_parents_sons import ParentsSons
import copy
import time
from Packages.write_log import write_log
import threading


class SendingCalculateProgram:

    @staticmethod
    def check_parents(program_info, version):
        sid = program_info['sid']
        parents_list = ParentsSons(sid).parents
        if len(parents_list) == 0:
            return False
        else:
            for parent_info in parents_list:
                p_version = parent_info['version']
                if p_version <= version:
                    # parents 的版本号小于等于 子程序的版本 ，子程序不执行
                    return False
            # 子程序执行
            return True

    def loop_check_calculate_programs(self):
        while True:
            calculate_programs = copy.deepcopy(maintain_plans.GetValue().get_programs_calculate_info_dic())
            try:
                threads = []
                for version in calculate_programs:
                    programs_list = calculate_programs[version]
                    for program_info in programs_list:
                        flag = self.check_parents(program_info, version)
                        print("loop_check_calculate_programs[ program_info:%s, flag:%s]" % (program_info, flag))
                        if flag:
                            ip = program_info['deploy_server']
                            port = program_info['port']
                            sid = program_info['sid']
                            program_info['version'] = version
                            maintain_plans.SetProgramsCalculateInfo(sid, version).program_start_running()
                            send_program = SendProgram(ip, port, program_info)
                            threads.append(threading.Thread(target=send_program.send))
                    for thread in threads:
                        thread.start()
            except Exception as e:
                err_str = "loop_check_calculate_programs Err:", str(e)
                print(err_str)
                write_log(err_str)
            finally:
                time.sleep(10)

