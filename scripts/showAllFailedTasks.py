#!/usr/bin/env python3
__author__ = "Bruce.Lu"
__copyright__ = "Copyright 2019, iLabService"
__credits__ = ["G.Xu", "david.Feng", "lk.Liao"]
__license__ = "PRIV"
__version__ = "0.0.1"
__maintainer__ = "Bruce.Lu"
__email__ = "lzbgt@icloud.com"
__status__ = "ALPHA"

import os, sys, time, datetime, re, json, shutil, threading, random, logging
from  multiprocessing import Pool, Process
from multiprocessing.pool import ThreadPool
from subprocess import Popen, PIPE, DEVNULL
import requests, redis, zlib

logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s: %(message)s')
log = logging.getLogger(__name__)

class TasksMgr(object):
    def __init__(self, env):
        self.redisConn = redis.Redis(host=env["redisAddr"], port=env["redisPort"], db=0)
        pass
    def run(self):
        pass

if __name__ == "__main__":
    env = {}
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.131")#"172.16.20.4")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))

    app = TasksMgr(env)
    failedTasks = app.redisConn.keys("ezvts:failed:*")
    for ft in failedTasks:
        fs = app.redisConn.smembers(ft)
        for fk in fs:
            fv = app.redisConn.get(fk)
            log.info("ft: {}, k: {}, v: {}".format(ft.decode('utf-8'),fk.decode('utf-8'), fv.decode('utf-8')))

    pass