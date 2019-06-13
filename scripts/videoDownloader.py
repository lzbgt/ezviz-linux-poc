#!/usr/bin/env python3
__author__ = "Bruce.Lu"
__copyright__ = "Copyright 2019, iLabService"
__credits__ = ["G.Xu", "david.Feng", "lk.Liao"]
__license__ = "PRIV"
__version__ = "0.0.1"
__maintainer__ = "Bruce.Lu"
__email__ = "lzbgt@icloud.com"
__status__ = "ALPHA"

import os, sys, time, datetime, re, json, shutil, logging
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
        return 'vt:'  + devSn + str(startTime)

    @staticmethod
    def makeFailedVTasksKey(devSn):
        return 'vts:failed:' + devSn

    @staticmethod
    def makeVTasksKey(devSn):
        return 'vts:' + devSn

    @staticmethod
    def makeVTaskValue(status=1, retries=0):
        '''
        status: 1 - in_processing; 2 - done; 3 - failed
        retries: 
        '''
        return "{}.{}.{}".format(status, retries, int(datetime.datetime.now().timestamp()*1000))

    @staticmethod
    def makeVADataKey(startTimeTs, endTimeTs):
        return 'vadata:' + str(startTimeTs) + ':' + str(endTimeTs)

    @staticmethod
    def makeVADevicesKey(startTimeTs, endTimeTs):
        return 'vadevices:' + str(startTimeTs) + ':' + str(endTimeTs)

    @staticmethod
    def timeStrToTsInt(timeStr):
        tm = datetime.datetime.strptime(timeStr, VideoDownloader.TFSTR)
        return int(tm.timestamp()*1000)
    
    @staticmethod
    def tsIntToTimeStr(tsInt):
        tm = datetime.datetime.fromtimestamp(tsInt/1000.0)
        return tm.strftime(VideoDownloader.TFSTR)

    @staticmethod
    def makeVideoFileName(devSn, startTimeTs, endTimeTs):
        delta = int(endTimeTs/1000 - startTimeTs/1000)
        tm = datetime.datetime.fromtimestamp(startTimeTs/1000.0)
        fileName = 'videos/'+tm.strftime('%Y%m%d%H%M%S') + '_' + devSn + '_' + str(delta) + '.mpg'
        log.info("moving file {}".format(fileName))
        return fileName

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
        
        self.env["token"] = r.json()["data"]["accessToken"]
        log.info("token is: " + self.env["token"])

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

        url = "https://open.ys7.com/api/lapp/alarm/device/list"
        data = {"accessToken":"at.957mvxyr5jb83w9056myl66fcu8kyhl3-4n5k20fl3x-1pg85jt-ony0xd8xb",
        "deviceSerial":sn,"startTime":startTime,"endTime":endTime,"status":status,"pageSize":"1000"}
        r = requests.post(url, data=data)
        log.info(json.dumps(data))
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed to get alarm for device {}. {}".format(sn, r.text))
            exit(1)
        
        if r.json().get("data") is None:
            return []

        return sorted(r.json()["data"], key=lambda k: k["alarmTime"])

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
        for vs in vss:
            v = vs["video"]
            #log.info("type: {}, value: {}".format(type(v["startTime"]), v["startTime"]))

            startTime = app.tsIntToTimeStr(v["startTime"])
            endTime = app.tsIntToTimeStr(v["endTime"])
            appKey = app.env["appKey"]
            token = app.env["token"]
            recType = "{}".format(v["recType"])

            taskKey = app.makeVTaskKey(devSn, v["startTime"])
            tasksKey = app.makeVTasksKey(devSn)

            # track this task in redis
            # key := devSn + ':' + videoStartTimeStamp
            # value := status + ":" + retried + ":" +timestamp; 
            #       status := 1 - in_processing, 2 - done, 3 - failed; 
            #       retried := num of retried downloads after failed. (maybe by other processes, not this one)
            #       timestamp := ts_in_millisecs - should update every minute. (for health checking)

            # check first if this task is already on going
            again = True
            taskVal = redisConn.get(taskKey)
            if taskVal is not None:
                taskVal = taskVal.decode('utf-8')
                numRunning = redisConn.scard(tasksKey)
                if numRunning is not None:
                    # only 3 connections to yscloud for one device
                    # clean dirty tasks
                    if numRunning >= 3:
                        # check if there was expire task
                        tasksVal = redisConn.smembers(tasksKey)
                        if tasksVal is not None:
                            #log.info("tasksVal: {}".format(tasksVal))
                            for tk in tasksVal:
                                tv = redisConn.get(tk)
                                if tv is not None:
                                    again, num = self.taskNeedRetry(tv)
                        # check again
                        numRunning = redisConn.scard(tasksKey)
                        if numRunning >= 3:
                            log.warning("running seesion for {} is more than 3, may result in 0 sized file.".format(devSn))

            status = 0
            retries = 0
            ts = 0
            
            if again is True:
                redisConn.set(taskKey, app.makeVTaskValue(1,num + 1))  
            else:
                continue  

            # do task
            redisConn.sadd(tasksKey, taskKey)

            if taskVal is None:
                redisConn.set(taskKey, app.makeVTaskValue())

            log.info("start cmd with params: {} {} {} {} {} {}".format(startTime, endTime, devSn, appKey, token, recType))
            proc = Popen(["./ezviz-cmd", "records", "get", "1", startTime, endTime, "3", devSn, "123456", appKey, token, recType],
                bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)
            
            msgCode = 0
            evType = 0
            
            for line in proc.stdout:
                # code: 5557 evt: 1
                # code:6701, eventType:1
                m = re.search(r'code: (\d+) evt: (\d+)', line.decode('utf-8'))
                if m is not None:
                    #m.group(1), m.group(2)
                    msgCode = int(m.group(1))
                    evType = int(m.group(2))
                    #sys.stdout.buffer.write(line)
                    #sys.stdout.buffer.flush()
                    if evType == 1 and msgCode != 6701 and msgCode != 5000:
                        sys.stdout.buffer.write("\n\n[DL_FAILED] msgCode: {}, evType: {}, device: {}, startTime: {}, endTime: {}, recType: {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                    else:
                        if evType == 0:
                            sys.stdout.buffer.write("\n\n[DL_START] msgCode: {}, evType: {}, device: {}, startTime: {}, endTime: {}, recType: {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                        else:
                            sys.stdout.buffer.write("\n\n[DL_SUCCEDDED] msgCode: {}, evType: {}, device: {}, startTime: {}, endTime: {}, recType: {}".format(
                            msgCode, evType, devSn, startTime, endTime, recType).encode('utf-8'))
                    sys.stdout.buffer.flush()
            proc.stdout.close()
            proc.wait()
            
            if (evType == 1 and msgCode != 6701 and msgCode != 5000) or (msgCode == 0 and evType == 0):
                # failed download, register in redis
                failedTasksKey = app.makeFailedVTasksKey(devSn)
                redisConn.sadd(failedTasksKey, taskKey)
                redisConn.set(taskKey, app.makeVTaskValue(3,0))
            else:
                redisConn.set(taskKey, app.makeVTaskValue(2,0))
                redisConn.srem(tasksKey, taskKey)
                redisConn.srem(failedTasksKey, taskKey)
                # move to downloaded
                shutil.move(env["downloaded"], app.makeVideoFileName(devSn, v["startTime"], v["endTime"]))
    
    def taskNeedRetry(self, taskVal):
        '''
        return (needRetry, retries)
        '''
        #log.info("typeof taskVal = {}".format(type(taskVal)))
        if type(taskVal) is not str:
            taskVal = taskVal.decode("utf-8")
        taskVals = taskVal.split('.')
        status = int(taskVals[0])
        retries = int(taskVals[1])
        ts = int(taskVals[2])
        if status == 3: # failed task
            log.info("it was a failed task with retries: {}".format(retries))
            return True, retries
        elif status == 1: # in processing
            # is running too long?
            now = int(datetime.datetime.now().timestamp())
            delta = now - ts/1000
            if delta > 60 * 30: # over 30 minutes
                # retry
                log.info("this task was over 30 minutes with retires: {}".format(retries))
                return True, retries
            else:
                # just skip it
                log.info("task is still running. skip")
                return False, 0
        elif status == 2: # done
            log.info("this task was done before. skip")
            return False, 0
        else:
            log.info("unkown status {}".format(status))
            return False, 0

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

            # task: get all alarms and videos
            # alarms: {devSn: [{alarm}]}
            # alarm: {alarmTime, }
            # videos: {devSn: [{video}]}
            # video: {startTime, endTime, recType}
            self.alarms = dict()
            self.videos = dict()

            with ThreadPool(env["numConcurrent"]) as tp:
                tp.map(self.getAlarmsPar, devices)
            
            with ThreadPool(env["numConcurrent"]) as tp:
                tp.map(self.getVideoListPar, devices)

            #log.info("alarms: {}".format(self.alarms))
            #log.info("\n\n\n\nvideos: {}".format(self.videos))
            # task: match alarm & video
            # alarmVideos: {devSn: [alarmVideo]}
            # alarmVide: {video, [alarm]}
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
                        matchedAlarms.append(thisAlarms[i]["alarmTime"])
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
                redisConn.delete(failedTasksKey)
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

        log.info("matching result: {}".format(matchedDevVideos))

        with ThreadPool(env["numConcurrent"]) as tp:
                tp.map(self.videoDownload, matchedDevVideos)        

if __name__ == "__main__":
    env = dict()
    env["appKey"] = os.getenv("EZ_APPKEY", "a287e05ace374c3587e051db8cd4be82")
    env["appSecret"] = os.getenv("EZ_APPSECRET", "f01b61048a1170c4d158da3752e4378d")
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "192.168.0.148")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))
    env["numConcurrent"] = int(os.getenv("EZ_CONCURENT", "20"))
    env["maxMinutes"] = int(os.getenv("EZ_MAX_MINUTES", "15"))
    env["startOver"] = os.getenv("EZ_START_OVER", "false")
    env["downloaded"] = "downloaded"

    # last day
    startTime = datetime.datetime.fromordinal((datetime.datetime.now()- datetime.timedelta(days=1)).toordinal())
    endTime = startTime + datetime.timedelta(days=0, hours=23, minutes=59, seconds=59)
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