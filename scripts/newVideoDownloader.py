#!/usr/bin/env python3
__author__ = 'Bruce.Lu'
__copyright__ = 'Copyright 2019, iLabService'
__credits__ = ['G.Xu', 'david.Feng', 'lk.Liao']
__license__ = 'PRIV'
__version__ = '0.0.1'
__maintainer__ = 'Bruce.Lu'
__email__ = 'lzbgt@icloud.com'
__status__ = 'ALPHA'

import os, sys, time, datetime, re, json, queue, shutil, threading, random, concurrent.futures, logging
from  multiprocessing import Pool, Process
from multiprocessing.pool import ThreadPool
from subprocess import Popen, PIPE, DEVNULL
from threading import Timer
import requests, redis, zlib
 
logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s %(module)s:%(lineno)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

redisConn = None

class VideoDownloader(object):
    TFSTR = '%Y-%m-%d %H:%M:%S'

    @staticmethod
    def makeAppLockKey(startTimeTs, endTimeTs):
        return 'ezvt:lock:' + str(startTimeTs) + ':' + str(endTimeTs) + ':' + str(int(datetime.datetime.now().timestamp()))

    @staticmethod
    def makeVTaskKey(devSn, startTimeTs, endTimeTs, recType):
        return 'ezvt:'  + devSn + ':' + str(startTimeTs) + ':' + str(endTimeTs) + ':' + recType

    def makeFailedVTasksKey(self, devSn):
        return 'ezvts:failed:' + str(env['startTimeTs']) + ':' + str(env['endTimeTs']) + ':' + devSn

    @staticmethod
    def makeVTasksKey(devSn):
        return 'ezvts:' + devSn

    @staticmethod
    def makeTotalVKey(startTs, endTs):
        return 'ezvtotal:' + str(startTs) + ':' + str(endTs)

    @staticmethod
    def makeTotalFilteredVKey(startTs, endTs):
        return 'ezvtotalflt:' + str(startTs) + ':' + str(endTs)

    @staticmethod
    def makeVTaskValue(appId, status=1, retries=0):
        '''
        status: 1 - in_processing; 2 - done; 3 - failed
        retries: 
        appId:
        ts:
        '''
        return '{}.{}.{}.{}'.format(status, retries, appId, int(datetime.datetime.now().timestamp()*1000))

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
        #log.info('typeof taskVal = {}'.format(type(taskVal)))
        if type(taskVal) is not str:
            taskVal = taskVal.decode('utf-8')
        taskVals = taskVal.split('.')
        status = int(taskVals[0])
        retries = int(taskVals[1])
        appId = taskVals[2]
        ts = int(taskVals[3])

        #log.info('status: {}, retries: {}, ts: {}, appId: {}, self: {}'.format(status, retries, ts, appId, self.appId))
        if status == 3: # failed task
            # it was a failed task with retries: {}
            if retries < env['maxRetries']:
                return True, status, retries, ts, appId
            else:
                return False, status, retries, ts, appId
        elif status == 1: # in processing
            # check liveness of app
            #log.info('appId: {}, thisId:{}'.format(appId, self.appId))
            if appId == self.appId:
                # should run again
                log.info('[BUG?] had run on this instance, is still running')
                return False, status, retries, ts, appId
            else:
                lastHeartBeatTs = int(redisConn.get(appId))
                now = int(datetime.datetime.now().timestamp())
                # over 30s no heartbeat
                # liveness check
                delta = now - lastHeartBeatTs
                log.info('checkpoint delta: {}, thisApp: {}, other:{}'.format(delta, self.appId, appId))
                if  delta > env['heartBeatSecs'] + 10:
                    # 30s no heartbeat, rerun
                    return True, status, retries, ts, appId
                else:
                    # received heartbeat, skip
                    return False, status, retries, ts, appId
        elif status == 2: # done
            # this task was done before
            return False, status, retries, ts, appId
        else:
            log.info('[BUG] unkown status: {}'.format(status))
            return False, status, retries, ts, appId

    def __init__(self, env):
        self.env = env
        self.threadResults = []
        self.threadResultsNum = 0
        self.failedDevices = []
        if any([env['appKey'], env['appSecret'], env['redisAddr']]) is None:
            exit(1)
        # get token
        data = {'appKey': self.env['appKey'], 'appSecret': self.env['appSecret']}
        url = 'https://open.ys7.com/api/lapp/token/get'
        r = requests.post(url, data=data)
        log.info(r.status_code)
        if r.status_code != 200 and r.json().get('code') != '200':
            log.error('failed request yscloud token. ' + r.text)
            exit(1)

        if r.json().get('data') is None:
            log.error('failed get token')
            exit(1)

        self.makeAppId()
        
        self.env['token'] = r.json()['data']['accessToken']
        log.info('token is: ' + self.env['token'])

    def makeAppId(self):
        if not getattr(self, 'appId', None):
            random.seed()
            rnd = int(random.random()*1000)
            _tid = threading.get_ident()
            self.appId= 'ezvdl:app:' + str(rnd) +':' + str(_tid)

        return self.appId
    
    def refreshLiveness(self):
        log.info('refersh liveness')
        secs = int(datetime.datetime.now().timestamp())
        redisConn.set(self.makeAppId(), secs)

    def getErrorCode(self, rj):
        log.info('{}'.format(rj))
        if rj['code'] == '20007':
            # device offline (sd)
            return 1
        elif rj['code'] == '5000':
            # server error
            return 2
        elif rj['code'] == '49999':
            # server data exception
            return 3
        elif rj['data'] is None:
            # no data
            return -2
        else:
            return -1

    def getVideosMobile(self, devSn, startTimeTs, endTimeTs):
        url = 'https://open.ys7.com/api/lapp/video/by/time'
        data = {'accessToken': self.env['token'], 'deviceSerial': devSn, 'channelNo': 1, 
        'startTime': startTimeTs, 'endTime': endTimeTs, 'recType': 1}

        oneDev = {}
        oneDev['deviceSerial'] = devSn
        oneDev['videos'] = []

        rj = []
        # get cloud files
        r = requests.post(url, data=data)
        if r.status_code != 200 or r.json() is None or r.json()['code'] != '200' or r.json()['data'] is None:
            log.error('failed to get cloud videos of IPCamera: {}, {}'.format(devSn, r.json()))
            faildDevice = {'deviceSerial': devSn, 'recType': 1, 'code': self.getErrorCode(r.json())}
            self.failedDevices.append(faildDevice)
        else:  
            rj = rj + r.json()['data']
        # get sd files
        data['recType'] = 2
        r = requests.post(url, data=data)
        if r.status_code != 200 or r.json() is None or r.json()['code'] != '200' or r.json()['data'] is None:
            log.error('failed to get sd videos of IPCamera: {}, {}'.format(devSn, r.json()))
            faildDevice = {'deviceSerial': devSn, 'recType': 2, 'code': self.getErrorCode(r.json())}
            self.failedDevices.append(faildDevice)
        else:
            rj = rj + r.json()['data']

        videos = []
        for elem in rj:
            video = {}
            video['video'] = {}
            video['video']['startTime'] = elem['startTime']
            video['video']['endTime'] = elem['endTime']
            video['video']['recType'] = elem['recType']
            videos.append(video)

        oneDev['videos'] = videos

        return oneDev

    def getDeviceList(self):
        devices = []
        url = 'https://open.ys7.com/api/lapp/device/list'
        data = {'accessToken': self.env['token'], 'pageStart': 0, 'pageSize': 50}
        log.info('{}'.format(data))
        r = requests.post(url, data=data)
        if r.status_code != 200 and r.json().get('code') != '200':
            log.error('failed request devices list. ' + r.text)
            exit(1)
        
        rj = r.json()
        rp = rj['page']
        log.info('\n\n{}'.format(rp))
        total = rp['total']
        currPage = rp['page']
        currNum = rp['size']
        devices = devices + rj['data']

        # has more?
        while currNum < total:
            data['pageStart'] = data['pageStart'] + 1
            #log.info('getting devices: {}'.format(data))
            r = requests.post(url, data=data)
            if r.status_code != 200 and r.json().get('code') != '200':
                log.error('failed request devices list. ' + r.text)
                exit(1)
            # log.info(r.text)
            rj = r.json()
            if rj.get('data') is None:
                break
            #log.info('\n\n{}'.format(rj['page']))
            currNum = currNum + rj['page']['size']
            devices = devices + rj['data']

        #log.info('devices: {}\nlen: {}'.format(devices, len(devices)))
        return devices

    def getAlarms(self, sn, startTime, endTime, status='2'):
        # devsn: [video]
        # video: {info, [alarmTs]}
        # info: {sts, ets, recType}
        # to utc
        startTime = self.env['startTimeTs'] - 8 * 60 * 60 * 1000
        endTime = self.env['endTimeTs'] - 8 * 60 * 60 * 1000
        total = 10000
        currNum = 0
        ret = []
        url = 'https://open.ys7.com/api/lapp/alarm/device/list'
        data = {'accessToken': self.env['token'],
        'deviceSerial':sn, 'startTime':startTime, 'endTime':endTime, 'status':status, 'pageSize':'50', 'pageStart': 0}
        while currNum < total:
            r = requests.post(url, data=data)
            #log.info('get alarms: {}'.format(data))
            if r.status_code != 200 and r.json().get('code') != '200':
                log.error('failed to get alarm for device {}. {}'.format(sn, r.text))
                exit(1)

            if r.json().get('data') is None:
                return ret
            else:
                rj = r.json()
                rp = rj['page']
                data['pageStart'] = data['pageStart'] + 1
                currNum = currNum + rp['size']
                total = rp['total']
                #log.info('total:{}, curr:{}'.format(total, currNum))
                ret = ret +  rj['data']

        return sorted(ret, key=lambda k: k['alarmTime'])
    
    def getVideosMobilePar(self, dev):
        self.videos.append(self.getVideosMobile(dev['deviceSerial'], self.env['startTimeTs'], self.env['endTimeTs']))

    def getAlarmsPar(self, dev):
        self.alarms[dev['deviceSerial']] =  self.getAlarms(dev['deviceSerial'], self.env['startTime'], self.env['endTime'])

    def getVideoList(self, devSn, startTime, endTime):
        videos = []
        # ./ezviz-cmd records (list|get) <chanId> <startTime> <endTime> <qualityLvl> <devSn> <devCode> <appKey> <token>
        # escape whitespaces
        #startTime = startTime.replace(' ', '\ ')
        #endTime = endTime.replace(' ', '\ ')
        proc = Popen(['./ezviz-cmd', 'records', 'list', '1', startTime, endTime, '3', devSn, '123456', self.env['appKey'], self.env['token'], '0'],
            bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)
        
        for line in proc.stdout:
            #index 1: start: 2019-06-10 23:25:27, endTime: 2019-06-11 00:00:01, type: 2
            m = re.search(r'index \d+: start: (.*?), endTime: (.*?), type: (\d)', line.decode('utf-8'))
            if m is not None:
                #log.info('matched: {}, {}, {}'.format(m.group(1), m.group(2), m.group(3)))
                videos.append({'startTime': self.timeStrToTsInt(m.group(1)), 'endTime': self.timeStrToTsInt(m.group(2)), 'recType': int(m.group(3))})
                #log.info('parsed: {}'.format(videos))
                #sys.stdout.buffer.write(line)
                #sys.stdout.buffer.flush()
                pass
        proc.stdout.close()
        proc.wait()

        return videos

    def getVideoListPar(self, dev):
        self.videos[dev['deviceSerial']] = self.getVideoList(dev['deviceSerial'], self.env['startTime'], self.env['endTime'])
    
    def videoDownload(self, videos):
        app = self
        devSn = videos['deviceSerial']
        vss = videos['videos']
        failedTasksKey = app.makeFailedVTasksKey(devSn)
        #log.info('videos: \n{}\n\n\n\n vs:\n{}'.format(videos, vss))
        hasFailedTask = False
        for vs in vss[:]:
            v = vs['video']
            log.info('\n\n\nv:{}'.format(v))
            alarmPic = None #vs['alarms'][0]['alarmPicUrl']
            startTime = app.tsIntToTimeStr(v['startTime'])
            endTime = app.tsIntToTimeStr(v['endTime'])
            appKey = app.env['appKey']
            token = app.env['token']
            recType = '0' #'{}'.format(v['recType'])

            taskKey = app.makeVTaskKey(devSn, v['startTime'], v['endTime'], recType)
            tasksKey = app.makeVTasksKey(devSn)
            log.info('task: dev {}, start {}, end {}, type {}'.format(devSn, v['startTime'], v['endTime'], recType))
            
            again = True
            status = 0
            retries = 0
            ts = 0
            appId = None
            delta = 0

            numRunning = redisConn.scard(tasksKey)
            log.info('running sessions for {}: {}, tasksKey:{}'.format(devSn, numRunning, taskKey))
            if numRunning >= 3:
                # clean tasks
                tasksVal = redisConn.smembers(tasksKey)
                for tk in tasksVal:
                    tv = redisConn.get(tk)
                    a, s, r, t, i = self.taskNeedRetry(tv)
                    if a:
                        redisConn.srem(tasksKey, tk)
            
            numRunning = redisConn.scard(tasksKey)
            if numRunning >= 3:
               log.warning('[SKIP-DEVICE] running seesion for {} is more than 3, may result in 0 sized file.'.format(devSn))
               break

            taskVal = redisConn.get(taskKey)
            #log.info('redis taskval:{}, thisAppId: {}'.format(taskVal, self.appId))
            if taskVal is not None:
                # had run before
                taskVal = taskVal.decode('utf-8')
                again, status, retries, ts, appId = self.taskNeedRetry(taskVal)
                log.info('existing tasks. key: {}, status: {}, retries: {}, ts: {}, appId: {}, self: {}'.format(taskKey, status, retries, ts, appId, self.appId))

                #log.info('check status2. type appId:{}'.format(type(appId)))
                if again == False:
                    forceRetry = False;
                    if status == 2:
                        log.info('[SKIP] task was done: {}'.format(taskKey))
                    elif status == 1:
                        log.info('[SKIP] task running on other instance alive: {}'.format(taskKey))
                        # indicate this task need to be rechecked
                        #hasFailedTask = True
                    elif retries >= env['maxRetries']:
                        if env['forceRetry'] == 'false':
                            log.info('[SKIP] task: {}, EZ_MAX_RETRIES: {} reached: {}'.format(taskKey, env['maxRetries'], retries))
                        else:
                            forceRetry = True
                            log.info('[FORCE-RETRY] task: {}, EZ_MAX_RETRIES: {} reached: {}'.format(taskKey, env['maxRetries'], retries))
                    else:
                        delta = int(datetime.datetime.now().timestamp()) - ts
                        log.info('[UNKOWN] task: {}, taskAppId: {}, instanceId:{}, delta-secs:{}'.format(taskKey, appId, self.appId, delta))
                    if forceRetry:
                        pass
                    else:
                        continue
                else:
                    if status == 1:
                        log.info('[RETAKE] other instance dead before task done, retake: {}'.format(taskKey))
                    elif status == 3:
                        log.info('[RETAKE] failed before, but retries {} < {}: {}'.format(retries, env['maxRetries'], taskKey))
            
            #log.info('prepare')
            if appId == None:
                log.info('new task: {}'.format(taskKey))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId))
            else:
                retries = retries + 1
                log.info('existing task: {}, retr: {}'.format(taskKey,retries))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId, 1, retries))

            redisConn.sadd(tasksKey, taskKey)

            log.info('start cmd with params: {} {} {} {} {} {}'.format(startTime, endTime, devSn, appKey, token, recType))
            proc = Popen(['./ezviz-cmd', 'records', 'get', '1', startTime, endTime, '3', devSn, '123456', appKey, token, recType],
                bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)

            thisTimer = Timer(env['maxTimeOutMinutes'] * 60, proc.kill)
            msgCode = 0
            evType = 0    
            fileName = None
            try:
                thisTimer.start()
                for line in proc.stdout:
                    # filename: videos/20190612083439_C90843689_23.mpg 
                    if fileName is None:
                        f = re.search(r'^filename: (.*?).mpg', line.decode('utf-8'))
                        if f is not None:
                            fileName = f.group(0)
                            log.info('\n\n\nFileName: {}\n alarmPic: {}'.format(fileName, alarmPic))
                    sys.stdout.buffer.write(line)
                    sys.stdout.buffer.flush()
                    m = re.search(r'code: (\d+) evt: (\d+)', line.decode('utf-8'))
                    if m is not None:
                        msgCode = int(m.group(1))
                        evType = int(m.group(2))
                        if evType == 1 and msgCode != 6701 and msgCode != 5000:
                            sys.stdout.buffer.write('\n\n[DL_FAILED] msgCode: {}, evType: {}. {}'.format(
                                msgCode, evType, taskKey).encode('utf-8'))
                        else:
                            if evType == 0:
                                sys.stdout.buffer.write('\n\n[DL_START] msgCode: {}, evType: {}. {}'.format(
                                msgCode, evType, taskKey).encode('utf-8'))
                            else:
                                sys.stdout.buffer.write('\n\n[DL_SUCCEDDED] msgCode: {}, evType: {}. {}'.format(
                                msgCode, evType, taskKey).encode('utf-8'))
                        sys.stdout.buffer.flush()
                proc.wait()
                proc.stdout.close()
                thisTimer.cancel()
            except Exception as e:
                log.error('[EXCEPTION] {},{}'.format(fileName, e))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, 10000))
                hasFailedTask = True
                continue
  
            if (evType == 1 and msgCode != 6701 and msgCode != 5000) or (msgCode == 0 and evType == 0):
                # failed download, register in redis
                log.info('\n\n\ndownload failed:{},{} {}, {}, {}, {}\n\n\n'.format(msgCode, evType, devSn, startTime, endTime, recType))
                
                redisConn.sadd(failedTasksKey, taskKey)

                if env['forceRetry'] != 'true':
                    # device offline, file not found & max connection
                    if msgCode == 5404:
                        redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, 5404))
                        # skip this device, no retry
                        continue
                    elif msgCode == 5402:
                        # skip this video
                        redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, 5402))
                        continue
                    elif msgCode == 5416:
                        # skip this device and add to retry later
                        redisConn.set(taskKey, app.makeVTaskValue(self.appId,3, retries))
                        hasFailedTask = True
                        break
                    else:
                        # need retry for other msgCode
                        if msgCode == 0:
                            msgCode = 9999
                        if retries >= env['maxRetries']:
                            retries = msgCode
                        redisConn.set(taskKey, app.makeVTaskValue(self.appId, 3, retries))
                        hasFailedTask = True
                else:
                    if msgCode == 0:
                            msgCode = 9999
                    redisConn.set(taskKey, app.makeVTaskValue(self.appId, 3, retries))
                    hasFailedTask = True
            else:
                log.info('\n\ndownload success: {}, {}, {}, {}\n\n'.format(devSn, startTime, endTime, recType))
                redisConn.set(taskKey, app.makeVTaskValue(self.appId, 2, retries))
                redisConn.srem(tasksKey, taskKey)

        if hasFailedTask:
            self.allTasksStatus[devSn] = 2
        else:
            self.allTasksStatus[devSn] = 3

        return hasFailedTask
    def threadCb(self, result):
        self.threadResults.append(result)
        log.info('threadResults: {}'.format(self.threadResults))

    def threadErrCb(self, result):
        self.threadErrResults.append(result)
        log.info('threadErrResults: {}'.format(self.threadErrResults))

    def run(self, redisConn):
        devices = None
        alarmVideos = None
        loadedFromRedis = False
        numCalc = 0

        appLock = redisConn.get(self.makeAppLockKey(env['startTimeTs'], env['endTimeTs']))
        if appLock:
            while True:
                appLock = int(appLock.decode('utf-8').split(':')[-1])
                # check locker lifetime
                if appLock == 0:
                    break
                now = int(datetime.datetime.now().timestamp())
                delta = now - int(appLock)
                if delta > 30 * 60:
                    redisConn.set(self.makeAppLockKey(env['startTimeTs'], env['endTimeTs']), str(now))
                    break
                time.sleep(30)
        else:
            now = int(datetime.datetime.now().timestamp())
            redisConn.set(self.makeAppLockKey(env['startTimeTs'], env['endTimeTs']), str(now))

        vadataKey = self.makeVADataKey(self.timeStrToTsInt(self.env['startTime']), self.timeStrToTsInt(self.env['endTime']))
        vadevicesKey = self.makeVADevicesKey(self.timeStrToTsInt(self.env['startTime']), self.timeStrToTsInt(self.env['endTime']))

        if env['startOver'] == 'true':
            redisConn.delete(vadataKey)
            redisConn.delete(vadevicesKey)
            
        if env['devicesList'] is not None:
            log.info('load devices from environment variable: {}'.format(env['devicesList'] ))
            devices = []
            devL = env['devicesList'].split(',')
            for d in devL:
                devices.append({'deviceSerial': d.strip()})

        log.info('try to load redis stored data first')
        vadata = redisConn.get(vadataKey)
        vadevices = redisConn.get(vadevicesKey)
        if vadata is not None and vadevices is not None:
            log.info('processing stored redis data')
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

        log.info('try to get devices list')
        if devices is None:
            devices = self.getDeviceList()
            if devices is None or len(devices) == 0:
                log.error('no devices')
                exit(1)

        if alarmVideos is None:
            log.info('no alarmVideos stored in redis, fetching from yscloud')

            self.videos = []
            with ThreadPool(env['numConcurrent']) as tp:
                tp.map(self.getVideosMobilePar, devices[:])

            size = 0
            cloudSize = 0
            for el in self.videos:
                size = size + len(el['videos'])
                for v in el['videos']:
                    #log.info('\n\n\n\nv: {}\n\n\n'.format(v))
                    if v['video']['recType'] == 1:
                        cloudSize = cloudSize + 1
            
            log.info('\n\n\ntotoal videos {}, cloud {}, failed devices: {}\n\nvideos: {}'.format(size, cloudSize, self.failedDevices, self.videos))
            
            # filter length 0
            alarmVideos = [v for v in self.videos if len(v['videos']) > 0]
            del self.videos

            # store videoAlarm data to redis
            textAlarmVides = json.dumps(alarmVideos)
            zippedAlarmVides = zlib.compress(textAlarmVides.encode('utf-8'), -1)
            redisConn.set(vadataKey, zippedAlarmVides)
            del textAlarmVides
            del zippedAlarmVides
            log.info('saved compressed video-alarms data to redis, key: ' + vadataKey)

            redisConn.set(self.makeAppLockKey(env['startTimeTs'], env['endTimeTs']), '0')

            # store devices to redis
            textDevices = json.dumps(devices)
            zippedDevices = zlib.compress(textDevices.encode('utf-8'), -1)
            redisConn.set(vadevicesKey, zippedDevices)
            del textDevices
            del zippedDevices
            log.info('saved compressed devices data to redis, key: ' + vadevicesKey)
        pass # end getting devices and alarmVideos

        matchedDevVideos = alarmVideos
        workQueue = queue.Queue()
        allTasks = dict()
        for t in matchedDevVideos[:]:
            workQueue.put(t)
            allTasks[t['deviceSerial']] = t
        self.allTasksStatus = {v['deviceSerial']:0 for _,v in allTasks.items()}
        with concurrent.futures.ThreadPoolExecutor(max_workers=env['numConcurrent']) as executor:
            done = False
            self.refreshLiveness()
            numSubmitted = 0
            while not done:
                while not workQueue.empty():
                    dv = workQueue.get()
                    executor.submit(self.videoDownload, dv)
                    # update status to running
                    self.allTasksStatus[dv['deviceSerial']] = 1
                    numSubmitted = numSubmitted + 1
                    
                done = True
                time.sleep(env['heartBeatSecs']/2)
                # info
                self.refreshLiveness()
                # check status
                downLoading = 0
                total = 0
                downloadingDetails = []
                for k, v in self.allTasksStatus.items():
                    total = total + 1
                    if v == 0:
                        downLoading = downLoading + 1
                        downloadingDetails.append((k, v))
                    if v == 1:
                        downLoading = downLoading + 1
                        downloadingDetails.append((k, v))
                        if downLoading <= 5:
                            log.info('running appId: {}, dev: {}, status: {}'.format(self.appId, k, v))
                    if v == 2: # failed
                        workQueue.put(allTasks[k])
                        self.allTasksStatus[k] = 0
                        log.info('requeue dev: {}, status: {}'.format(k, v))
                    if v != 3: # success
                        done = False
                log.info('total jobs: {}, downloading: {}, workq: {}, submitted: {}'.format(total, downLoading, workQueue.qsize(), numSubmitted))
                if len(downloadingDetails) <= 5:
                    log.info('downloading details: {}'.format(downloadingDetails))

if __name__ == '__main__':
    env = dict()
    env['appKey'] = os.getenv('EZ_APPKEY', 'a287e05ace374c3587e051db8cd4be82')
    env['appSecret'] = os.getenv('EZ_APPSECRET', 'f01b61048a1170c4d158da3752e4378d')
    env['redisAddr'] = os.getenv('EZ_REDIS_ADDR', '172.31.0.96')
    env['redisPort'] = int(os.getenv('EZ_REDIS_PORT', '6379'))
    env['numConcurrent'] = int(os.getenv('EZ_CONCURRENT', '20'))
    env['maxMinutes'] = int(os.getenv('EZ_MAX_MINUTES', '15'))
    env['maxRetries'] = int(os.getenv('EZ_MAX_RETRIES', '10'))
    env['heartBeatSecs'] = int(os.getenv('EZ_HEATBEAT_SECS', '20'))
    env['devicesList'] = os.getenv('EZ_DEVICES_LIST', None)
    env['startOver'] = os.getenv('EZ_START_OVER', 'false')
    env['forceRetry'] = os.getenv('EZ_FORCE_RETRY', 'false')
    env['downloaded'] = 'downloaded'
    env['maxTimeOutMinutes'] = int(os.getenv('EZ_MAX_TIMEOUT', 30))

    # last day
    lastDate = (datetime.datetime.now() - datetime.timedelta(days=1) + datetime.timedelta(hours=8)).toordinal()
    startTime = datetime.datetime.fromordinal(lastDate) + datetime.timedelta(hours=0)
    endTime = startTime + datetime.timedelta(days=0, hours=23, minutes=59, seconds=59)
    startTime = startTime.strftime(VideoDownloader.TFSTR)
    endTime = endTime.strftime(VideoDownloader.TFSTR)
    env['startTime'] = os.getenv('EZ_START_TIME', startTime)
    env['endTime'] = os.getenv('EZ_END_TIME', endTime)
    env['startTimeTs'] = VideoDownloader.timeStrToTsInt(env['startTime'])
    env['endTimeTs'] = VideoDownloader.timeStrToTsInt(env['endTime'])
    log.info('startTime: {}, {}. endTime: {}, {}'.format(env['startTime'], env['startTimeTs'], env['endTime'], env['endTimeTs']))
    log.info('connecting redis')
    redisConn = redis.Redis(host=env['redisAddr'], port=env['redisPort'], db=0)
    if redisConn is None:
        log.error('failed connect to redis: {}:{}'.format(env['redisAddr'], env['redisPort']))
        exit(1)

    log.info('run app')
    app = VideoDownloader(env)
    app.run(redisConn)