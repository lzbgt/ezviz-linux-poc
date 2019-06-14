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
                     format='%(asctime)s %(name)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

redisConn = None

class VideoDownloader(object):
    TFSTR = "%Y-%m-%d %H:%M:%S"
    @staticmethod
    def makeVTaskKey(devSn, startTime):
        return 'ezvt:'  + devSn + ':' + str(startTime)

    @staticmethod
    def makeFailedVTasksKey(devSn):
        return 'ezvts:failed:' + devSn

    @staticmethod
    def makeVTasksKey(devSn):
        return 'ezvts:' + devSn

    @staticmethod
    def makeVTaskValue(appId, status=1, retries=0):
        '''
        status: 1 - in_processing; 2 - done; 3 - failed
        retries: 
        '''
        return "{}.{}.{}.{}".format(status, retries, int(datetime.datetime.now().timestamp()*1000), appId)

    @staticmethod
    def makeVADataKey(startTimeTs, endTimeTs):
        return 'ezvadata:' + str(startTimeTs) + ':' + str(endTimeTs)

    @staticmethod
    def makeVADevicesKey(startTimeTs, endTimeTs):
        return 'ezvadevices:' + str(startTimeTs) + ':' + str(endTimeTs)

    @staticmethod
    def timeStrToTsInt(timeStr):
        tm = datetime.datetime.strptime(timeStr, VideoDownloader.TFSTR)
        return int(tm.timestamp()*1000)
    
    @staticmethod
    def tsIntToTimeStr(tsInt):
        tm = datetime.datetime.fromtimestamp(tsInt/1000.0)
        return tm.strftime(VideoDownloader.TFSTR)
    
    def taskNeedRetry(self, taskVal):
        '''
        return (needRetry, status, retries, ts, appId)
        '''
        #log.info("typeof taskVal = {}".format(type(taskVal)))
        if type(taskVal) is not str:
            taskVal = taskVal.decode("utf-8")
        taskVals = taskVal.split('.')
        status = int(taskVals[0])
        retries = int(taskVals[1])
        ts = int(taskVals[2])
        appId = taskVals[3]

        log.info("status: {}, retries: {}, ts: {}, appId: {}, self: {}".format(status, retries, ts, appId, self.appId))
        if status == 3: # failed task
            # it was a failed task with retries: {}
            if retries < env["maxRetries"]:
                return True, status, retries, ts, appId
            else:
                return False, status, retries, ts, appId
        elif status == 1: # in processing
            # check liveness of app
            if appId == self.appId:
                # should run again
                log.info("[BUG] had run on this instance(1), but need rerun")
                return True, status, retries, ts, appId
            else:
                lastHeartBeatTs = redisConn.get(appId)
                now = int(datetime.datetime.now().timestamp())
                # over 30s no heartbeat
                # liveness check
                delta = now - lastHeartBeatTs
                log.info("delta: {}, thisApp: {}, other:{}".format(delta, self.appId, appId))
                if  delta > 30:
                    # 30s no heartbeat, rerun
                    return True, status, retries, ts, appId
                else:
                    # received heartbeat, skip
                    return False, status, retries, ts, appId
        elif status == 2: # done
            # this task was done before
            return False, status, retries, ts, appId
        else:
            log.info("[BUG] unkown status: {}".format(status))
            return False, status, retries, ts, appId

    def __init__(self, env):
        self.env = env
        if any([env["appKey"], env["appSecret"], env["redisAddr"]]) is None:
            exit(1)
        # get token
        data = {"appKey": self.env["appKey"], "appSecret": self.env["appSecret"]}
        url = "https://open.ys7.com/api/lapp/token/get"
        r = requests.post(url, data=data)
        log.info(r.status_code)
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed request yscloud token. " + r.text)
            exit(1)

        if r.json().get("data") is None:
            log.error("failed get token")
            exit(1)

        self.makeAppId()
        
        self.env["token"] = r.json()["data"]["accessToken"]
        log.info("token is: " + self.env["token"])

    def makeAppId(self):
        log.info("1")
        if not getattr(self, 'appId', None):
            random.seed()
            log.info("2")
            rnd = int(random.random()*1000)
            _tid = threading.get_ident()
            self.appId= 'ezvdl:app:' + str(rnd) +':' + str(_tid)

        return self.appId
    
    def refreshLiveness(self):
        secs = int(datetime.datetime.now().timestamp())
        redisConn.set(self.makeAppId(), secs)

    def getDeviceList(self):
        devices = []
        url = "https://open.ys7.com/api/lapp/device/list"
        data = {"accessToken": self.env["token"], "pageStart": 0, "pageSize": 50}
        r = requests.post(url, data=data)
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed request devices list. " + r.text)
            exit(1)
        
        rj = r.json()
        rp = rj["page"]
        #log.info("\n\n{}".format(rp))
        total = rp["total"]
        currPage = rp["page"]
        currNum = rp["size"]
        devices = devices + rj["data"]

        # has more?
        while currNum < total:
            data["pageStart"] = data["pageStart"] + 1
            r = requests.post(url, data=data)
            if r.status_code != 200 and r.json().get("code") != "200":
                log.error("failed request devices list. " + r.text)
                exit(1)
            # log.info(r.text)
            rj = r.json()
            if rj.get("data") is None:
                break;
            #log.info("\n\n{}".format(rj["page"]))
            currNum = currNum + rj["page"]["size"]
            devices = devices + rj["data"]

        #log.info("devices: {}\nlen: {}".format(devices, len(devices)))
        return devices

    def getAlarms(self, sn, startTime, endTime, status="2"):
        # devsn: [video]
        # video: {info, [alarmTs]}
        # info: {sts, ets, recType}

        startTime = datetime.datetime.strptime(startTime, VideoDownloader.TFSTR)
        startTime = int(startTime.timestamp()*1000)
        endTime = datetime.datetime.strptime(endTime, VideoDownloader.TFSTR)
        endTime = int(endTime.timestamp()*1000)
        total = 10000
        currNum = 0
        ret = []
        url = "https://open.ys7.com/api/lapp/alarm/device/list"
        data = {"accessToken":"at.957mvxyr5jb83w9056myl66fcu8kyhl3-4n5k20fl3x-1pg85jt-ony0xd8xb",
        "deviceSerial":sn,"startTime":startTime,"endTime":endTime,"status":status,"pageSize":"50", "pageStart": 0}
        while currNum < total:
            r = requests.post(url, data=data)
            log.info(json.dumps(data))
            if r.status_code != 200 and r.json().get("code") != "200":
                log.error("failed to get alarm for device {}. {}".format(sn, r.text))
                exit(1)

            if r.json().get("data") is None:
                return ret
            else:
                rj = r.json()
                rp = rj["page"]
                data["pageStart"] = data["pageStart"] + 1
                currNum = currNum + rp["size"]
                total = rp["total"]
                log.info("total:{}, curr:{}".format(total, currNum))
                ret = ret +  rj["data"]

        return sorted(ret, key=lambda k: k["alarmTime"])

    def getAlarmsPar(self, dev):
        self.alarms[dev["deviceSerial"]] =  self.getAlarms(dev["deviceSerial"], self.env["startTime"], self.env["endTime"])

    def getVideoList(self, devSn, startTime, endTime):
        videos = []
        # ./ezviz-cmd records (list|get) <chanId> <startTime> <endTime> <qualityLvl> <devSn> <devCode> <appKey> <token>
        # escape whitespaces
        #startTime = startTime.replace(" ", "\ ")
        #endTime = endTime.replace(" ", "\ ")
        proc = Popen(["./ezviz-cmd", "records", "list", "1", startTime, endTime, "3", devSn, "123456", self.env["appKey"], self.env["token"], "0"],
            bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)
        
        for line in proc.stdout:
            #index 1: start: 2019-06-10 23:25:27, endTime: 2019-06-11 00:00:01, type: 2
            m = re.search(r'index \d+: start: (.*?), endTime: (.*?), type: (\d)', line.decode('utf-8'))
            if m is not None:
                #log.info("matched: {}, {}, {}".format(m.group(1), m.group(2), m.group(3)))
                videos.append({"startTime": self.timeStrToTsInt(m.group(1)), "endTime": self.timeStrToTsInt(m.group(2)), "recType": int(m.group(3))})
                #log.info("parsed: {}".format(videos))
                #sys.stdout.buffer.write(line)
                #sys.stdout.buffer.flush()
                pass
        proc.stdout.close()
        proc.wait()

        return videos

    def getVideoListPar(self, dev):
        self.videos[dev["deviceSerial"]] = self.getVideoList(dev["deviceSerial"], self.env["startTime"], self.env["endTime"])
    
    def videoDownload(self, videos):
        app = self
        devSn = videos["deviceSerial"]
        vss = videos["videos"]
        #log.info("videos: \n{}\n\n\n\n vs:\n{}".format(videos, vss))
        for vs in vss[:1]:
            v = vs["video"]
            alarmPic = v['alarms'][0]['alarmPicUrl']
            startTime = app.tsIntToTimeStr(v["startTime"])
            endTime = app.tsIntToTimeStr(v["endTime"])
            appKey = app.env["appKey"]
            token = app.env["token"]
            recType = "{}".format(v["recType"])

            taskKey = app.makeVTaskKey(devSn, v["startTime"])
            tasksKey = app.makeVTasksKey(devSn)
            log.info("task: dev {}, start {}, end {}, type {}".format(devSn, v["startTime"], v["startTime"], recType))

            # track this task in redis
            # key := devSn + ':' + videoStartTimeStamp
            # value := status + ":" + retried + ":" +timestamp; 
            #       status := 1 - in_processing, 2 - done, 3 - failed; 
            #       retried := num of retried downloads after failed. (maybe by other processes, not this one)
            #       timestamp := ts_in_millisecs - should update every minute. (for health checking)

            # check first if this task is already on going
            again = True
            status = 0
            retries = 0
            ts = 0
            appId = None
            delta = 0

            numRunning = redisConn.scard(tasksKey)
            log.info("running sessions for {}: {}".format(devSn, numRunning))
            if numRunning >= 3:
                # clean tasks
                tasksVal = redisConn.smembers(tasksKey)
                for tk in tasksVal:
                    tv = redisConn.get(tk)
                    a, s, r, t, i = self.taskNeedRetry(tv)
                    if a:
                        redisConn.srem(tasksKey, tv)
            
            numRunning = redisConn.scard(tasksKey)
            if numRunning >= 3:
                log.error("[SKIP] running seesion for {} is more than 3, may result in 0 sized file.".format(devSn))
                return

            
            taskVal = redisConn.get(taskKey)
            log.info("redis taskval:{}, thisAppId: {}".format(taskVal, self.appId))
            if taskVal is not None:
                # had run before
                # check status
                taskVal = taskVal.decode('utf-8')
                #log.info("taskVal: {}".format(taskVal))
                again, status, retries, ts, appId = self.taskNeedRetry(taskVal)

                #log.info("check status2. type appId:{}".format(type(appId)))
                if again == False:
                    if status == 2:
                        log.info("[SKIP] task was done: {},{},{},{}".format(devSn, startTime, endTime, recType))
                    elif status == 1:
                        log.info("[SKIP] task running on other instance alive: {},{},{},{}".format(devSn, startTime, endTime, recType))
                    elif retries >= env['maxRetries']:
                        log.info("[SKIP] EZ_MAX_RETRIES: {} reached: {}".format(env['maxRetries'], retries))
                    else:
                        delta = int(datetime.datetime.now().timestamp()) - ts
                        log.info("[UNKOWN] taskAppId: {}, instanceId:{}, delta-secs:{}".format(appId, self.appId, delta))
                    return
            
            #log.info("prepare")
            if appId == None:
                log.info("new task. AppId:{}".format(self.appId))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId))
            else:
                redisConn.set(taskKey, app.makeVTaskValue(self.appId, 1, retries + 1))

            redisConn.sadd(tasksKey, taskKey)

            log.info("start cmd with params: {} {} {} {} {} {}".format(startTime, endTime, devSn, appKey, token, recType))
            proc = Popen(["./ezviz-cmd", "records", "get", "1", startTime, endTime, "3", devSn, "123456", appKey, token, recType],
                bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)
            
            msgCode = 0
            evType = 0
            
            fileName = None
            for line in proc.stdout:
                # filename: videos/20190612083439_C90843689_23.mpg 
                if fileName is None:
                    f = re.search(r'^filename: (.*?).mpg', line.decode('utf-8'))
                    if f is not None:
                        fileName = f.group(0)
                        log.info("\n\n\nFileName: {}\n alarmPic: {}\n\n".format(fileName, alarmPicUrl))
                #sys.stdout.buffer.write(line)
                #sys.stdout.buffer.flush()
                m = re.search(r'code: (\d+) evt: (\d+)', line.decode('utf-8'))
                if m is not None:
                    msgCode = int(m.group(1))
                    evType = int(m.group(2))
                    if evType == 1 and msgCode != 6701 and msgCode != 5000:
                        sys.stdout.buffer.write("\n\n[DL_FAILED] msgCode: {}, evType: {}, {}, {}, {}, {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                    else:
                        if evType == 0:
                            sys.stdout.buffer.write("\n\n[DL_START] msgCode: {}, evType: {}, {}, {}, {}, {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                        else:
                            sys.stdout.buffer.write("\n\n[DL_SUCCEDDED] msgCode: {}, evType: {}, {}, {}, {}, {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                    sys.stdout.buffer.flush()
            proc.wait()
            proc.stdout.close()
            
            if (evType == 1 and msgCode != 6701 and msgCode != 5000) or (msgCode == 0 and evType == 0):
                # failed download, register in redis
                log.info("\n\n\ndownload failed:{},{} {}, {}, {}, {}\n\n\n".format(msgCode, evType, devSn, startTime, endTime, recType))
                failedTasksKey = app.makeFailedVTasksKey(devSn)
                redisConn.sadd(failedTasksKey, taskKey)
                redisConn.set(taskKey, app.makeVTaskValue(self.appId, 3, retries))
                # device offline & file not found
                if msgCode == 5404:
                    redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, 5404))
                elif msgCode == 5402:
                    redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, 5404))
                else:
                    self.allDone = False
            else:
                log.info("\n\n\ndownload success: {}, {}, {}, {}\n\n\n".format(devSn, startTime, endTime, recType))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId,2, retries))
                redisConn.srem(tasksKey, taskKey)
                redisConn.srem(failedTasksKey, taskKey)
                # move to downloaded
                shutil.move(fileName, env["downloaded"] + '/')

    def run(self, redisConn):
        os.makedirs(env["downloaded"], exist_ok=True)
        log.info("try to load redis stored data first")
        vadataKey = self.makeVADataKey(self.timeStrToTsInt(self.env["startTime"]), self.timeStrToTsInt(self.env["endTime"]))
        vadevicesKey = self.makeVADevicesKey(self.timeStrToTsInt(self.env["startTime"]), self.timeStrToTsInt(self.env["endTime"]))

        if env["startOver"] == "true":
            redisConn.delete(vadataKey)
            redisConn.delete(vadevicesKey)
            
        vadata = redisConn.get(vadataKey)
        vadevices = redisConn.get(vadevicesKey)

        devices = None
        alarmVideos = None
        loadedFromRedis = False

        if env["devicesList"] is not None:
            devices = []
            devL = env["devicesList"].split(',')
            for d in devL:
                devices.append({"deviceSerial": d.strip()})

        if vadata is not None and vadevices is not None:
            vadataunzip = zlib.decompress(vadata)
            del vadata
            vadevicesunzip = zlib.decompress(vadevices)
            del vadevices
            # parse json
            alarmVideos = json.loads(vadataunzip.decode('utf-8'))
            del vadataunzip
            devices = json.loads(vadevicesunzip.decode('utf-8'))
            del vadevicesunzip
            loadedFromRedis = True

        if devices is None or alarmVideos is None:
            log.info("no data stored in redis, fetching from yscloud")
            # get devices
            devices = self.getDeviceList()

            self.alarms = dict()
            self.videos = dict()

            with ThreadPool(env["numConcurrent"]) as tp:
                tp.map(self.getAlarmsPar, devices)
            
            with ThreadPool(env["numConcurrent"]) as tp:
                tp.map(self.getVideoListPar, devices)

            alarmVideos = dict()
            for dev in devices[:]:
                thisAlarms = self.alarms.get(dev["deviceSerial"])
                thisVideos = self.videos.get(dev["deviceSerial"])

                # next dev
                if thisAlarms is None or thisVideos is None or len(thisAlarms) == 0 or len(thisVideos) == 0:
                    continue
                idx = 0
                end = len(thisAlarms)
                alarmVideos[dev["deviceSerial"]] = []

                # for each video
                iv = 0
                for v in thisVideos:
                    matchedAlarms = []
                    # for each alarm
                    for i in range(idx, end):
                        #log.info("matching {}-{}:{} -> {}".format(iv, i, v, thisAlarms[i]["alarmTime"]))
                        # the first alarm is new than this video, next video
                        if thisAlarms[i]["alarmTime"] > v["endTime"]:
                            break
                        # next alarm, change idx for later iter
                        if thisAlarms[i]["alarmTime"] < v["startTime"]:
                            idx = i + 1
                            continue
                        # matched
                        #log.info("matched {}-{}:{} -> {}".format(iv, i, v, thisAlarms[i]["alarmTime"]))
                        matchedAlarms.append({'alarmTime': thisAlarms[i]["alarmTime"], 'alarmPicUrl': thisAlarms[i]["alarmPicUrl"]})
                    pass # alarm
                    alarmVideos[dev["deviceSerial"]].append({"video": v, "alarms": matchedAlarms})
                    iv = iv + 1
                pass # video
            pass # device

            # release memory
            del self.alarms
            del self.videos

            # clear dirty data in redis
            # clear tasks & task
            for dev in devices:
                vas = alarmVideos.get(dev["deviceSerial"])
                tasksKey = self.makeVTasksKey(dev["deviceSerial"])
                failedTasksKey = self.makeFailedVTasksKey(dev["deviceSerial"])
                if failedTasksKey is not None:
                    redisConn.delete(failedTasksKey)
                if tasksKey is not None:
                    redisConn.delete(tasksKey)
                if vas is not None:
                    for v in vas:
                        startTime = v["video"]["startTime"]
                        taskKey = self.makeVTaskKey(dev["deviceSerial"], startTime)
                        redisConn.delete(taskKey)

                pass
            # store videoAlarm data to redis
            textAlarmVides = json.dumps(alarmVideos)
            zippedAlarmVides = zlib.compress(textAlarmVides.encode('utf-8'), -1)
            redisConn.set(vadataKey, zippedAlarmVides)
            del textAlarmVides
            del zippedAlarmVides
            log.info("saved compressed video-alarms data to redis, key: " + vadataKey)

            # store devices to redis
            textDevices = json.dumps(devices)
            zippedDevices = zlib.compress(textDevices.encode('utf-8'), -1)
            redisConn.set(vadevicesKey, zippedDevices)
            del textDevices
            del zippedDevices
            log.info("saved compressed devices data to redis, key: " + vadevicesKey)
        pass # end getting devices and alarmVideos


        # convert to parallel array for multiple threading
        matchedDevVideos = []
        for dev in devices:
            avs = alarmVideos.get(dev["deviceSerial"])
            if avs is not None and len(avs) != 0:
                # filter avs
                avs = [v for v in avs if len(v["alarms"])!=0 and (v["video"]["endTime"] - v["video"]["startTime"]) <= self.env["maxMinutes"] * 60 * 1000 ]
                if avs is not None and len(avs) != 0:
                    matchedDevVideos.append({"deviceSerial": dev["deviceSerial"], "videos": avs})

        del alarmVideos
        
        log.info("matching result: {}".format(matchedDevVideos))

        # store appId
        self.refreshLiveness()
        self.allDone = False
        while self.allDone == False:
            self.allDone = True
            tp = ThreadPool(1)#env["numConcurrent"])
            tph = tp.map_async(self.videoDownload, matchedDevVideos[:])
            log.info("pooling..")
            time.sleep(4)
            self.refreshLiveness()
            while True:
                # 20s
                tph.wait(20)
                # TODO: heartbeat
                self.refreshLiveness()
                log.info("refreshed")
                if tph.ready():
                    break # next round
                else:
                    continue



if __name__ == "__main__":
    env = dict()
    env["appKey"] = os.getenv("EZ_APPKEY", "a287e05ace374c3587e051db8cd4be82")
    env["appSecret"] = os.getenv("EZ_APPSECRET", "f01b61048a1170c4d158da3752e4378d")
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.148")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))
    env["numConcurrent"] = int(os.getenv("EZ_CONCURENT", "20"))
    env["maxMinutes"] = int(os.getenv("EZ_MAX_MINUTES", "15"))
    env['maxRetries'] = int(os.getenv("EZ_MAX_RETRIES", "10"))
    env["devicesList"] = os.getenv("EZ_DEVICES_LIST", None)
    env["startOver"] = os.getenv("EZ_START_OVER", "false")
    env["downloaded"] = "downloaded"

    # last day
    startTime = datetime.datetime.fromordinal((datetime.datetime.now()- datetime.timedelta(days=1)).toordinal())
    env["startTimeTs"] = int(startTime.timestamp())
    endTime = startTime + datetime.timedelta(days=0, hours=23, minutes=59, seconds=59)
    env["endTimeTs"] = int(endTime.timestamp())
    startTime = startTime.strftime(VideoDownloader.TFSTR)
    endTime = endTime.strftime(VideoDownloader.TFSTR)
    env["startTime"] = os.getenv("EZ_START_TIME", startTime)
    env["endTime"] = os.getenv("EZ_END_TIME", endTime)


    redisConn = redis.Redis(host=env["redisAddr"], port=env["redisPort"], db=0)
    if redisConn is None:
        log.error("failed connect to redis: {}:{}".format(env["redisAddr"], env["redisPort"]))
        exit(1)

    app = VideoDownloader(env)
    app.run(redisConn)