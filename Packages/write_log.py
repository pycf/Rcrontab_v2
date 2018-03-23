import os.path
import datetime
import configparser

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
service_path = os.path.join("/", base_path, "config/service.conf")
conf = configparser.ConfigParser()
conf.read(service_path)
log_file_path = conf.get("master", "MASTER_LOG_FILE")
log_file = os.path.join("/", base_path, log_file_path)


def write_log(string, name=None):
    if name is None:
        name = log_file
    try:
        if not (os.path.exists(name) and os.path.isfile(name)):
            pass
        else:
            log_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(name, 'a') as f:
                file_str = "[%s]  %s \n" % (log_time, string)
                f.write(file_str)
    except Exception as e:
        print("write_log Err:", str(e))
