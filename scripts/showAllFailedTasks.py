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
        self.csv = False
        self.appKey = 'a287e05ace374c3587e051db8cd4be82'
        self.appSecret = 'f01b61048a1170c4d158da3752e4378d'
        self.getToken()
        pass

    def run(self, status=None, retries=None, devsn=None, start=None, appId=None):
        self.getFailedTasks(status, retries, devsn, start, appId)

    def getToken(self):
        data = {"appKey": self.appKey, "appSecret": self.appSecret}
        url = "https://open.ys7.com/api/lapp/token/get"
        r = requests.post(url, data=data)
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed request yscloud token. " + r.text)
            exit(1)

        if r.json().get("data") is None:
            log.error("failed get token")
            exit(1)
        self.token = r.json()["data"]["accessToken"]

    def checkOnline(self, devSn):
        data = {'accessToken': self.token, 'deviceSerial': devSn}
        url = 'https://open.ys7.com/api/lapp/device/info'
        r = requests.post(url, data=data)
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed request yscloud token. " + r.text)
            exit(1)

        if r.json().get("data") is None:
            log.error("failed get token")
            exit(1)

        online = True if r.json()['data']['status'] == 1 else False
        encry = True if r.json()['data']['isEncrypt'] == 1 else False
        return online, encry

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
        rawDf = df.copy()

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

        df = df.loc[filter, :].sort_values(by=['PeriodStart', 'DevSn', 'LastSched'], ascending=False).reset_index()
        self.printFull(df)
        if self.csv:
            df.to_csv(r'failed_veidos.csv')
        
        if status == '3':
            # check device online
            onlines = []
            df = rawDf.loc[(rawDf['Status'] == '3') & (rawDf['Retries'] == '5404'), :]
            failedDevs = df['DevSn'].unique()
            log.info("checking offile devices: {}".format(failedDevs))
            for dev in failedDevs:
                onLine,encry = self.checkOnline(dev)
                onlines.append((dev, onLine, encry))
            
            devOnline_df = pd.DataFrame.from_records(onlines, columns=('DevSn', 'Online', 'Encryption')).sort_values(by=['Online']).reset_index()
            self.printFull(devOnline_df)

        
        totalKeys = self.redisConn.keys('ezvtotal*')
        totals = []
        for k in totalKeys:
            v = self.redisConn.get(k)
            totals.append((k, v))
        
        df = pd.DataFrame.from_records(totals, columns=('TotalKey', 'Count'))
        df = df.sort_values(by=['TotalKey'])
        self.printFull(df)

    def getDevices(self):
        devicesKey = app.redisConn.keys("ezvadevices:*")
        for dk in devicesKey:
            devComp = self.redisConn.get(dk)
            dev = json.loads(zlib.decompress(devComp).decode('utf-8'))
            log.info("devices key: {}, len: {}".format(dk, len(dev)))
            log.info("\tdevs: {}".format(dev))

def usage():
    usage = '''Usage: <status> <retries> <devsn> <period_start> <instance_id>
    status: 3 - failed; 2 - suceessed; 1 - processing; none - any
    retries: N - number; none - any
    devsn: Cxxxx; none - any
    period_start: none - any
examples:
1. check all failed job on specified instance
    3, none, none, none, 140571204925248
2. check all failed job on specified device
    3, none, C90840812
    '''
    print(usage)

if __name__ == "__main__":
    env = {}
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.132")#"172.16.20.4")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))
    app = TasksMgr(env)
    args = len(sys.argv)
    skip = 0
    if args > 1 and 'csv' in sys.argv[args-1]:
        skip = 1
        app.csv = True
    for arg in sys.argv:
        if arg == "-h" or arg == "--help":
            usage()
            exit(0)

    app.run(*sys.argv[1:(args - skip)])


