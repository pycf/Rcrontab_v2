from datetime import datetime


class GetProgramList:

    def __init__(self):

        self.crawler_sql_str = 'select  ' \
                      'cast(sid as char) as sid, ' \
                      'run_type, ' \
                      'c.ip as deploy_server, ' \
                      ' a.run_type, '\
                      ' CONCAT(b.path,a.`name`)  as path , ' \
                      'crontab ' \
                      'from py_script_base_info_v2 a ' \
                      'LEFT JOIN ' \
                      'py_script_programs_path b ' \
                      'on a.path_id = b.id ' \
                      'LEFT JOIN py_script_server_info_v2 c ' \
                      'ON b.server_id = c.id ' \
                      'where is_stop=0  and crontab  is not NULL' \


        self.calculate_sql_str = 'select  ' \
                                 'cast(sid as char) as sid, ' \
                                 'run_type, ' \
                                 'c.ip as deploy_server, ' \
                                 'c.port, ' \
                                 ' a.run_type, '\
                                 ' CONCAT(b.path,a.`name`)  as path , ' \
                                 'crontab ' \
                                 'from py_script_base_info_v2 a ' \
                                 'LEFT JOIN ' \
                                 'py_script_programs_path b ' \
                                 'on a.path_id = b.id ' \
                                 'LEFT JOIN py_script_server_info_v2 c ' \
                                 'ON b.server_id = c.id ' \
                                 'where is_stop=0 and program_type!=0' \



def get_crawler_sql_codes():
    get_program_list = GetProgramList()
    return get_program_list.crawler_sql_str


def get_calculate_sql_codes():
    get_program_list = GetProgramList()
    return get_program_list.calculate_sql_str


get_online_slaves = '''SELECT id,ip,port 
from 
py_script_server_info_v2
where status=1'''

get_all_slaves = '''SELECT id,ip,port 
from 
py_script_server_info_v2
'''

update_slave_status = '''UPDATE py_script_server_info_v2
SET status = {status},
uptime = \"{uptime}\"
where id = {id}
'''


get_parents = '''
select DISTINCT cast(d.sid as char) as sid,
d.version,
CONCAT(e.path,d.`name`)  as path ,
f.ip as deploy_server, 
f.`port` as port, 
d.run_type 
from py_script_base_info_v2 as a 
LEFT JOIN 
py_script_base_info_v2_pre_tables as b
on a.sid = b.pyscriptbaseinfov2_id
LEFT JOIN py_script_base_info_v2_result_tables as c
on b.tablesinfo_id = c.tablesinfo_id
LEFT JOIN py_script_base_info_v2 as d
on c.pyscriptbaseinfov2_id = d.sid
LEFT JOIN py_script_programs_path  as e 
on d.path_id = e.id 
LEFT JOIN py_script_server_info_v2 as f
ON e.server_id = f.id
where a.sid = {sid} and d.sid is not NULL 
and d.exec_plan = 1 '''

get_sons = '''
select DISTINCT cast(d.sid as char) as sid,
d.version,
CONCAT(e.path,d.`name`)  as path ,
f.ip as deploy_server, 
f.`port` as port, 
d.run_type 
FROM
py_script_base_info_v2 as a 
LEFT JOIN 
py_script_base_info_v2_result_tables b
on a.sid = b.pyscriptbaseinfov2_id
LEFT JOIN py_script_base_info_v2_pre_tables  as c
on b.tablesinfo_id = c.tablesinfo_id
LEFT JOIN py_script_base_info_v2 as d
on c.pyscriptbaseinfov2_id = d.sid
LEFT JOIN py_script_programs_path  as e 
on d.path_id = e.id 
LEFT JOIN py_script_server_info_v2 as f
ON e.server_id = f.id
where a.sid = {sid}  and d.sid is not NULL
and d.exec_plan = 1 '''

program_result_log = '''insert into py_script_result_log(id,version,event_time,sid,event_type,extra_info,subversion) 
values(0, \"{version}\",\"{event_time}\",{sid},{event_type},"{extra_info}","{subversion}")'''

get_start_flag = 'select max(id) as id from py_script_result_log where flag = 1'

get_end_flag = 'select max(id) as id from py_script_result_log'

update_flag = 'update py_script_result_log set flag = 1 where id = {id}'

get_execute_crawling_programs_result = '''
select a.id,a.sid,a.version,a.subversion,a.extra_info,a.event_type,b.program_type, b.name, c.owner,c.mail
from py_script_result_log a
inner join py_script_base_info_v2 b on  a.sid = b.sid
LEFT JOIN py_script_owners_info_v2 c on b.owner_id = c.`owner`
where  id>{a.start_id} and id <={a.end_id} and b.program_type=0
'''

get_execute_calculate_programs_result = '''
select a.id,a.sid,a.version,a.subversion,a.extra_info,a.event_type,b.program_type, b.name, c.owner,c.mail
from py_script_result_log a
inner join py_script_base_info_v2 b on  a.sid = b.sid
LEFT JOIN py_script_owners_info_v2 c on b.owner_id = c.`owner`
where  id>{start_id} and id <={end_id} and b.program_type=1
'''

# get_equality_success_programs = '''
# select DISTINCT id,sid,version,subversion,extra_info
# from py_script_result_log where event_type = 4 and id>{start_id} and id <={end_id}
# '''

get_old_version = 'select version from py_script_base_info_v2 where sid={sid}'

update_version = 'update py_script_base_info_v2 set version="{version}" where sid={sid}'

get_program_base_info = ''' select  cast(sid as char) as sid, run_type, c.ip as deploy_server, 
c.`port` as port, 
a.run_type,
CONCAT(b.path,a.`name`)  as path ,  
 exec_time ,
 a.times 
 from py_script_base_info_v2 a  
 LEFT JOIN  py_script_programs_path b  on a.path_id = b.id  
 LEFT JOIN py_script_server_info_v2 c  ON b.server_id = c.id
 where a.sid={sid}'''


get_owner_info_from_sid = '''
select a.name,a.owner_id,b.`owner`,b.mail
from py_script_base_info_v2 a
LEFT JOIN py_script_owners_info_v2 b
on a.owner_id = b.`owner`
where a.sid = {sid}'''


get_result_tables = '''select 
c.id,
c.db_server,
c.db_name,
c.table_name
from 
py_script_base_info_v2 as a 
LEFT JOIN 
py_script_base_info_v2_result_tables b
on a.sid = b.pyscriptbaseinfov2_id
LEFT JOIN py_script_tables_info c
on b.tablesinfo_id = c.id
where a.sid = {sid}'''
