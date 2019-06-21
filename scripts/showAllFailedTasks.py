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
import requests, redis, zlib, click
import numpy as np
import pandas as pd


logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s: %(message)s')
log = logging.getLogger(__name__)

#ft: ezvts:failed:1560902400000:1560988799000:C90842542, k: ezvt:C90842542:1560962767000:1560962769000:2, v: 3.0.ezvdl:app:682:140662334924608.1561013069555
class TasksMgr(object):
    def __init__(self, env):
        self.redisConn = redis.Redis(host=env["redisAddr"], port=env["redisPort"], db=0)
        pass

    def run(self, status=None, retries=None, devsn=None, start=None, appId=None):
        self.getFailedTasks(status, retries, devsn, start, appId)

    def printFull(self,x):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 2000)
        pd.set_option('display.float_format', '{:20,.2f}'.format)
        pd.set_option('display.max_colwidth', -1)
        print(x)
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
        pd.reset_option('display.width')
        pd.reset_option('display.float_format')
        pd.reset_option('display.max_colwidth')


    def getFailedTasks(self, status = None, retries=None, devsn=None, start=None, appId=None):
        failedTasks = self.redisConn.keys("ezvts:failed:*")
        records = []
        for ft in failedTasks:  
            v1 = ft.decode('utf-8').split(':')
            fs = self.redisConn.smembers(ft)
            for fk in fs:
                fv = self.redisConn.get(fk)
                v2 = fk.decode('utf-8').split(':')
                v3 = fv.decode('utf-8').split('.')
                app = v3[2].split(':')[3]
                line = (v2[2], v2[3], v2[4], v3[0], v3[1], v2[1], v1[2], v1[3], v3[3],app)
                records.append(line)
                #log.info("ft: {}, k: {}, v: {}".format(ft.decode('utf-8'),fk.decode('utf-8'), fv.decode('utf-8')))
        # schema: vs, ve, vt, status, retries, sn, ps, pe, last, app
        label = ('StartTime', 'EndTime', 'RecType', 'Status', 'Retries', 'DevSn', 'PeriodStart', 'PeriodEnd', 'LastSched', 'InstanceId')
        df = pd.DataFrame.from_records(records, columns=label)

        filter  = (df['RecType'] != None)
        if status is not None and status != "none":
            filter = (df['Status'] == status)
        if retries is not None and retries != "none":
            filter = filter & (df['Retries'] == retries)
        if devsn is not None and devsn != "none":
            filter = filter & (df['DevSn'] == devsn)
        if start is not None and start != "none":
            filter = filter & (df['PeroidStart'] == start)
        if appId is not None and appId != "none":
            filter = filter & (df['InstanceId'] == appId)

        df = df.loc[filter, :].sort_values(by=['PeriodStart'], ascending=False).reset_index()
        self.printFull(df)

    def getDevices(self):
        devicesKey = app.redisConn.keys("ezvadevices:*")
        for dk in devicesKey:
            devComp = self.redisConn.get(dk)
            dev = json.loads(zlib.decompress(devComp).decode('utf-8'))
            log.info("devices key: {}, len: {}".format(dk, len(dev)))
            log.info("\tdevs: {}".format(dev))

def usage():
    usage = '''PARAM Sequence: <status> <retries> <devsn> <period_start> <instance_id>
    status: 3 - failed; 2 - suceessed; 1 - processing; None - any
    retries: N - number; None - any
    devsn: Cxxxx; None - any
    period_start: None - any
examples:
1. check all failed job on specified instance
    3, None, None, None, 140571204925248
2. check all failed job on specified device
    3, None, C90840812
    '''
    print(usage)

if __name__ == "__main__":
    env = {}
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.132")#"172.16.20.4")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))
    app = TasksMgr(env)
    if sys.argv[1] and (sys.argv[1] == "-h" or sys.argv[1] == "--help"):
        usage()
        exit(0)

    app.run(*sys.argv[1:])


