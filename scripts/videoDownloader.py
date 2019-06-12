#!/usr/bin/env python3
__author__ = "Bruce.Lu"
__copyright__ = "Copyright 2019, iLabService"
__credits__ = ["G.Xu", "david.Feng", "lk.Liao"]
__license__ = "PRIV"
__version__ = "0.0.1"
__maintainer__ = "Bruce.Lu"
__email__ = "lzbgt@icloud.com"
__status__ = "ALPHA"

import os, sys, time, datetime, re, json, logging
from  multiprocessing import Pool, Process
from subprocess import Popen, PIPE, DEVNULL
import requests, redis
 
logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s %(name)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)


class VideoDownloader(object):
    TFSTR = "%Y-%m-%d %H:%M:%S"
    @staticmethod
    def timeStrToTsInt(timeStr):
        tm = datetime.datetime.strptime(timeStr, VideoDownloader.TFSTR)
        return int(tm.timestamp()*1000)
    
    @staticmethod
    def tsIntToTimeStr(tsInt):
        tm = datetime.datetime.fromtimestamp(tsInt/1000.0)
        return tm.strftime(VideoDownloader.TFSTR)

    def __init__(self, env):
        self.env = env
        if any([env["appKey"], env["appSecret"], env["redisAddr"]]) is None:
            exit(1)
        # self.redisConn = redis.Redis(host=env["redisAddr"], port=env["redisPort"], db=0)
        # if self.redisConn is None:
        #     log.error("failed connect to redis: {}:{}".format(env["redisAddr"], env["redisPort"]))
        #     exit(1)
        # get token
        data = {"appKey": self.env["appKey"], "appSecret": self.env["appSecret"]}
        url = "https://open.ys7.com/api/lapp/token/get"
        r = requests.post(url, data=data)
        log.info(r.status_code)
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed request yscloud token. " + r.text)
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
        if r.status_code != 200 and r.json().get("code") != "200":
            log.error("failed to get alarm for device {}. {}".format(sn, r.text))
            exit(1)

        return sorted(r.json()["data"], key=lambda k: k["alarmTime"])

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
    
    @staticmethod
    def videoDownload(video):
        inst = video["video"]["instance"]
        v = video["video"]
        #log.info("type: {}, value: {}".format(type(v["startTime"]), v["startTime"]))
        #return
        startTime = inst.tsIntToTimeStr(v["startTime"])
        endTime = inst.tsIntToTimeStr(v["endTime"])
        devSn = v["deviceSerial"]
        appKey = inst.env["appKey"]
        token = inst.env["token"]
        recType = "{}".format(v["recType"])

        # track this task in redis
        # key := devSn + ':' + videoStartTimeStamp
        # value := status + ":" + retried + ":" +timestamp; 
        #       status := 1 - in_processing, 0 - done, 2 - failed; 
        #       retried := num of retried downloads after failed. (maybe by other processes, not this one)
        #       timestamp := ts_in_millisecs - should update every minute. (for health checking)
        # inst.redisConn.set(devSn + ':' + startTime, )

        proc = Popen(["./ezviz-cmd", "records", "get", "1", startTime, endTime, "3", devSn, "123456", appKey, token, recType],
            bufsize=1, shell=False, stdout=PIPE) #, stderr=DEVNULL)
        
        for line in proc.stdout:
            # #index 1: start: 2019-06-10 23:25:27, endTime: 2019-06-11 00:00:01, type: 2
            # m = re.search(r'index \d+: start: (.*?), endTime: (.*?), type: (\d)', line.decode('utf-8'))
            # if m is not None:
            #     #log.info("matched: {}, {}, {}".format(m.group(1), m.group(2), m.group(3)))
            #     videos.append({"startTime": self.timeStrToTsInt(m.group(1)), "endTime": self.timeStrToTsInt(m.group(2)), "recType": int(m.group(3))})
            #     #log.info("parsed: {}".format(videos))
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
            pass
        proc.stdout.close()
        proc.wait()
        if proc.returncode == 0:
            pass
        else:
            pass
        pass

    def run(self):
        devices = self.getDeviceList()

        # task: get all alarms and videos
        # alarms: {devSn: [{alarm}]}
        # alarm: {alarmTime, }
        # videos: {devSn: [{video}]}
        # video: {startTime, endTime, recType}
        alarms = dict()
        videos = dict()
        for dev in devices[:1]:
            alarms[dev["deviceSerial"]] = self.getAlarms(dev["deviceSerial"], self.env["startTime"], self.env["endTime"])
            #log.info("{}".format(alarms[dev["deviceSerial"]]))
            videos[dev["deviceSerial"]] = self.getVideoList(dev["deviceSerial"], self.env["startTime"], self.env["endTime"])
        
        # task: match alarm & video
        # alarmVideos: {devSn: [alarmVideo]}
        # alarmVide: {video, [alarm]}
        alarmVideos = dict()
        for dev in devices[:]:
            thisAlarms = alarms.get(dev["deviceSerial"])
            thisVideos = videos.get(dev["deviceSerial"])
            # invalid return
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
                #log.info("matchedAlarms: {}-{}".format(iv, matchedAlarms))
                # augment with devSn, and this instance
                v["deviceSerial"] = dev["deviceSerial"]
                v["instance"] = self
                alarmVideos[dev["deviceSerial"]].append({"video": v, "alarms": matchedAlarms})
                iv = iv + 1
            pass # video
        pass # device

        #log.info("matching result: {}".format(alarmVideos))
        # examing one device
        for dev in devices[:]:
            pureMatched = [v for v in alarmVideos[dev["deviceSerial"]] if len(v["alarms"])!=0]
            log.info("matched: {}".format(pureMatched))
            # filter videos length longer than env["maxMinutes"]
            downloadVideos = [v for v in alarmVideos[dev["deviceSerial"]] if len(v["alarms"])!=0 and (v["video"]["endTime"] - v["video"]["startTime"]) <= self.env["maxMinutes"] * 60 * 1000 ]
            with Pool(processes=self.env["numConcurrent"]) as pool:
                pool.map(self.videoDownload, downloadVideos)

if __name__ == "__main__":
    env = dict()
    env["appKey"] = os.getenv("EZ_APPKEY", "a287e05ace374c3587e051db8cd4be82")
    env["appSecret"] = os.getenv("EZ_APPSECRET", "f01b61048a1170c4d158da3752e4378d")
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "127.0.0.1")
    env["redisPort"] = int(os.getenv("EZ_REDIS_PORT", "6379"))
    env["numConcurrent"] = int(os.getenv("EZ_CONCURENT", "3"))
    env["maxMinutes"] = int(os.getenv("EZ_MAX_MINUTES", "5"))

    # last day
    startTime = datetime.datetime.fromordinal((datetime.datetime.now()- datetime.timedelta(days=1)).toordinal())
    endTime = startTime + datetime.timedelta(days=0, hours=23, minutes=59, seconds=59)
    startTime = startTime.strftime(VideoDownloader.TFSTR)
    endTime = endTime.strftime(VideoDownloader.TFSTR)
    env["startTime"] = os.getenv("EZ_START_TIME", startTime)
    env["endTime"] = os.getenv("EZ_END_TIME", endTime)

    app = VideoDownloader(env)
    app.run()