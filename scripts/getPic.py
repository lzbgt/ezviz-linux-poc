# -*- coding: utf-8 -*-
import requests,json,time,datetime
import os,sys, logging
from intelabInfluxdb import InfluxdbClient
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
logging.basicConfig(level=logging.INFO,
                    filename='new.log',
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )



mystoragename = "ilsqzwjvideo"
mystoragekey = "wta7rpW6HeTzok1AwhYohbj+Ws6cFN7u102vkTlSjrOGBllUtKMuezwuenipjnaBng4hXHtS53341liEVz1MKA=="
blob_service = BlockBlobService(account_name=mystoragename, account_key=mystoragekey,endpoint_suffix='core.chinacloudapi.cn')

def getToken():
    # data={"appKey":"102574957ca44d8493e6c1df6aaa1b14",
    #       "appSecret":"2fd3f7e17d5a9590c182e6f2ee64bdfc"}
    data={"appKey":"a287e05ace374c3587e051db8cd4be82",
          "appSecret":"f01b61048a1170c4d158da3752e4378d"}
    r= requests.post("https://open.ys7.com/api/lapp/token/get",data=data)
    return json.loads(r.text)['data']['accessToken']

'''查询所有设备的sn'''
def get_device_sn(token):
    try:
        list = []
        for i in range(sys.maxsize):
            data = {"accessToken": token, "pageStart": i, "pageSize": 50}
            r=requests.post("https://open.ys7.com/api/lapp/device/list",data=data)
            result = json.loads(r.text)['data']
            print(result)
            if len(result) == 0:
                break
            list += result
        deviceSN = []
        for i in list:
            deviceSN.append(i['deviceSerial'])
        return deviceSN
        logging.info("get device ok")
    except Exception as e:
        logging.info(e)

def get_alarm_picture(token,deviceSerial,startTime,endTime,url,path):
    try:
        list = []
        for i in range(sys.maxsize):
            data = {"accessToken": token, "pageStart": i, "pageSize": 50, "deviceSerial": deviceSerial,
                    "startTime": startTime*1000, "endTime": endTime*1000, "status": 2, "alarmType": -1}
            #data = {"accessToken": token, "pageStart": i, "pageSize": 50, "deviceSerial": deviceSerial,
            #        "startTime": startTime, "endTime": endTime, "status": 2, "alarmType": -1}
            r = requests.post("https://open.ys7.com/api/lapp/alarm/device/list", data=data)
            result = json.loads(r.text)['data']
            print(result)
            if len(result) == 0:
                break
            list += result
            influxdb = InfluxdbClient()
        for i in list:
            print(i['alarmPicUrl'])
            print(i['deviceSerial'])
            downLoadPhoto(path,i['alarmPicUrl'],i['deviceSerial'],i['alarmTime'])
            print(i['alarmTime'])
            a = str(url) + "/intelab-alarm-list" + "/" + str(i['deviceSerial']) + str(i['alarmTime']) + ".jpg"
            print(a)
            influxdb.insert_message(i['alarmTime'], a, str(i['deviceSerial']))
        return list
    except Exception as e:
        print(e)

def getfile(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            uploadPhoto(file,str(path)+"/"+file)

def del_file(path):
    ls = os.listdir(path)
    for i in ls:
        c_path = os.path.join(path, i)
        if os.path.isdir(c_path):
            del_file(c_path)
        else:
            os.remove(c_path)

def uploadPhoto(name,path):
    blob_service.create_blob_from_path(
        'intelab-alarm-list',
        name,
        path,
        content_settings=ContentSettings(content_type='image/jpg'))

def downLoadPhoto(path,url,deviceSerial,alarmTime):
    if not os.path.exists(path):
        os.mkdir(path)
    f = requests.get(url)
    # 下载文件
    with open(str(path)+"/"+str(deviceSerial)+"{}.jpg".format(alarmTime), "wb") as code:
        code.write(f.content)
    getfile(path)
    del_file(path)

if __name__ == "__main__":
    # 今天日期
    today = datetime.date.today()
    # 昨天时间
    yesterday = today - datetime.timedelta(days=1)
    # 昨天开始时间戳
    yesterday_start_time = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d')))
    timeArray = time.localtime(yesterday_start_time)
    otherStyleTime = time.strftime("%Y--%m--%d %H:%M:%S", timeArray)
    print(otherStyleTime)
    # 昨天结束时间戳
    yesterday_end_time = int(time.mktime(time.strptime(str(today), '%Y-%m-%d')))-1
    timeArray = time.localtime(yesterday_end_time)
    otherStyleTime = time.strftime("%Y--%m--%d %H:%M:%S", timeArray)
    list = get_device_sn(getToken())
    try:
        for i in list:
            get_alarm_picture(getToken(),i,yesterday_start_time,yesterday_end_time,"https://ilsqzwjvideo.blob.core.chinacloudapi.cn","/root/images")
            logging.info("OK")
    except Exception as e:
        logging.info(e)



