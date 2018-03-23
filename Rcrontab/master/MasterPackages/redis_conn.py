import os
import configparser
import redis
import json
base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
service_path = os.path.join("/", base_path, "config/service.conf")


conf = configparser.ConfigParser()
conf.read(service_path)

redis_server = conf.get("master", "REDIS_SERVER")
redis_port = conf.get("master", "REDIS_PORT")
redis_password = conf.get("master", "REDIS_PASSWORD")


class RedisConn:

    def __init__(self):
        pool = redis.ConnectionPool(host=redis_server, port=int(redis_port),
                                    password=redis_password, decode_responses=True)
        self.r = redis.Redis(connection_pool=pool)


r = RedisConn().r

if __name__ == '__main__':
    a = {'a': 'aa', 'a1': {'bb': 'bb', 'b1': 'b1', 'b3': {'c1': 'c2'}}}
    a_s = json.dumps(a)
    r.set('_aa', a_s)
    a_r = r.get('_aa')
    b = json.loads(a_r)
    print(b)

