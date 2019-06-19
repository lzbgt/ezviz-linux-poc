#!/bin/python3
import os, redis
if __name__ == '__main__':
    env = {}
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.132")#"172.16.20.4")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))

    redisConn = redis.Redis(host=env["redisAddr"], port=env["redisPort"], db=0)
    keys = redisConn.keys('ezv*')
    for k in keys:
        redisConn.delete(k)

    print("\ncleaned.\n")
