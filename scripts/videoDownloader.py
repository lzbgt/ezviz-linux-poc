## [overall process]
## get all alarms from ys cloud
## for each device
##    get video list
##    match alarm with video  => device: matched videos
##
## for m in machedVides:
##     create container to download video

import os, time, requests, json
import logging, sys, datetime
 
logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s %(name)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)


class VideoDownloader(object):
    TFSTR = "%Y-%m-%d %H:%M:%S"
    def __init__(self, env):
        self.env = env
        if any([env["appKey"], env["appSecret"], env["redisAddr"]]) is None:
            exit(1)
        
        # get token
        data = {"appKey": self.env["appKey"], "appSecret": self.env["appSecret"]}
        url = "https://open.ys7.com/api/lapp/token/get"
        r = requests.post(url, data=data)
        log.info(r.status_code)
        if r.status_code != 200:
            log.error("failed request yscloud token")
            exit(1)
        
        env["token"] = r.json()["data"]["accessToken"]
        log.info("token is: " + env["token"])

    def getDeviceList(self):
        devices = []
        url = "https://open.ys7.com/api/lapp/device/list"
        data = {"accessToken": self.env["token"], "pageStart": 0, "pageSize": 50}
        r = requests.post(url, data=data)
        if r.status_code != 200 and r.json()["code"] != "200":
            log.error("failed request devices list")
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
            if r.status_code != 200:
                log.error("failed request devices list")
                exit(1)
            # log.info(r.text)
            rj = r.json()
            #log.info("\n\n{}".format(rj["page"]))
            currNum = currNum + rj["page"]["size"]
            devices = devices + rj["data"]

        log.info("devices: {}\nlen: {}".format(devices, len(devices)))
        return devices

    def getAlarms(self, sn, startTime, endTime, status="2"):
        #startTime = "1559923200000"
        #endTime = "1560268799000"
        ##1355563265.81
        #sn = "C90843475"

        url = "https://open.ys7.com/api/lapp/alarm/device/list"
        data = {"accessToken":"at.957mvxyr5jb83w9056myl66fcu8kyhl3-4n5k20fl3x-1pg85jt-ony0xd8xb",
        "deviceSerial":sn,"startTime":startTime,"endTime":endTime,"status":status,"pageSize":"1000"}
        r = requests.post(url, data=data)
        log.info(r.status_code)
        log.info(r.text)

        pass
    def getVideoList(self, devSn):
        pass
    def matchAlarmVideos(self, devSn, alarms, videos):
        pass
    def downloadDevAlarmVideos(self,alarmVideos):
        # create seprate process to download
        pass

    def run(self):
        devices = self.getDeviceList()
        i = 0
        for dev in devices:
            i = i + 1
            if i > 3:
                break
            self.getAlarms(dev["deviceSerial"], self.env["startTime"], self.env["endTime"])


if __name__ == "__main__":
    env = dict()
    env["appKey"] = os.getenv("EZ_APPKEY", "a287e05ace374c3587e051db8cd4be82")
    env["appSecret"] = os.getenv("EZ_APPSECRET", "f01b61048a1170c4d158da3752e4378d")
    env["redisAddr"] = os.getenv("EZ_REDIS_ADDR", "127.0.0.1")
    env["redisPort"] = os.getenv("EZ_REDIS_PORT", 6379)
    # last day
    startTime = datetime.datetime.fromordinal((datetime.datetime.now()- datetime.timedelta(days=1)).toordinal())
    endTime = startTime + datetime.timedelta(days=0, hours=23, minutes=59, seconds=59)
    # startTime = startTime.strftime(VideoDownloader.TFSTR)
    # endTime = endTime.strftime(VideoDownloader.TFSTR)
    # startTime = datetime.datetime.strptime(startTime, VideoDownloader.TFSTR)
    startTime = int(startTime.timestamp()*1000)
    # endTime = datetime.datetime.strptime(endTime, VideoDownloader.TFSTR)
    endTime = int(endTime.timestamp()*1000)
    env["startTime"] = os.getenv("EZ_START_TIME", startTime)
    env["endTime"] = os.getenv("EZ_END_TIME", endTime)

    app = VideoDownloader(env)
    app.run()