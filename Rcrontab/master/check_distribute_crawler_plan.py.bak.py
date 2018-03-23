import hashlib
import json
import time
import copy
from Rcrontab.master import maintain_plans
from Rcrontab.master.MasterPackages.send_to_slave import DistributePlans
from Packages.write_log import write_log


# 循环检查 distribute_crawler_info 更新 则更新计划任务
def loop_check_distribute_crawler_info():
    _programs_crawler_info = maintain_plans.GetValue().get_programs_crawler_info()
    programs_info = copy.deepcopy(_programs_crawler_info)
    while True:
        try:
            _programs_crawler_info = maintain_plans.GetValue().get_programs_crawler_info()
            programs_info_new = copy.deepcopy(_programs_crawler_info)
            hash_old = hashlib.md5(json.dumps(programs_info).encode('utf-8')).hexdigest()
            # 转换成dict
            # 与执行计划比对
            hash_new = hashlib.md5(json.dumps(programs_info_new).encode('utf-8')).hexdigest()
        except Exception as e:
            print(e)
            write_log(str(e))
        else:
            if hash_new != hash_old:
                programs_info = copy.deepcopy(programs_info_new)
                distribute = DistributePlans(_programs_crawler_info.keys())
                distribute.distribute()
        finally:
            time_waite = 20
            time.sleep(time_waite)

