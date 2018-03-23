
#  MySQL 数据库连接操作
import pymysql
import pandas as pd
import configparser
import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
service_path = os.path.join("/", base_path, "config/service.conf")


conf = configparser.ConfigParser()
conf.read(service_path)

mysql_host = conf.get("master", "MYSQL_IP")
mysql_user = conf.get("master", "MYSQL_USER")
mysql_pwd = conf.get("master", "MYSQL_PWD")
db_name = conf.get("master", "DATABASE_NAME")


class Mysql:

    def __init__(self, host=mysql_host, user=mysql_user, password=mysql_pwd, db=db_name):
        self.host = host
        self.user = user
        self.pwd = password
        self.db = db
        self.status = "Fail"
        try:
            self.db = pymysql.connect(self.host, self.user, self.pwd, self.db, charset="utf8")
            self.status = "ok"
        except Exception as e:
            print("MySQL connect err:", e)
            print()
            self.status = "Fail"
        finally:
            self.status = "Fail"

    def mysql_read(self, sql):
        df = []
        try:
            df = pd.read_sql(sql, self.db)
            self.db.close()
        except pymysql.err.InternalError as err:
            print("Mysql.mysql_read error:", err)
        except Exception as e:
            print("Unexpected error:", e)
            raise
        finally:
            return df

    def mysql_write(self, sql):
        cur = self.db.cursor()
        try:
            cur.execute(sql)
            self.db.commit()
            self.db.close()
        except pymysql.err.InternalError as err:
            print("Mysql.mysql_write error:", err)
        except Exception as e:
            print("Unexpected error:", e)
            raise
