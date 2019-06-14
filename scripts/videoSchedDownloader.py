import requests,json,time
from datetime import datetime
import os,logging,argparse,shutil
import getToken


'''get image'''
def get_file(path,token,address,target):
    try:
        for root, dirs, files in os.walk(path):
            for file in files:
                fileName = file
                stat = os.stat(path + '/' + fileName)
                last = int(stat.st_mtime)
                now = int(datetime.datetime.now().timestamp())
                deltaSecs = now -last
                if deltaSecs < 60 * 2:
                    continue
                file_size = os.path.getsize(path+"/"+fileName)
                list = fileName.split("_")
                if len(list[0]) == 14 and file_size != 0:
                    date = datetime.strptime(list[0], "%Y%m%d%H%M%S")
                    timeArray = time.strptime(str(date), "%Y-%m-%d %H:%M:%S")
                    timestamp = int(time.mktime(timeArray) * 1000)
                    print(timestamp)
                    url = "http://{address}/api/v2/secure/admin/private/video/upload/camera/".format(
                        address=address) + str(list[1]) + "/start/" + str(timestamp) + "/length/" + str(
                        list[2].split(".")[0])
                    '''upload video'''
                    upload_video(url, path + "/" + file, token,"playback")
        '''delete file'''
        del_files(path)
    except Exception as e:
        logging.info("get file fail")


'''delete all file'''
def del_files(path):
    ls = os.listdir(path)
    for i in ls:
        c_path = os.path.join(path, i)
        if os.path.isdir(c_path):
            del_files(c_path)
        else:
            os.remove(c_path)

'''upload video'''
def upload_video(url,filePath,token,type):
    headers = {"X-Authorization":"Bearer "+token}
    data = {"type":type}
    files = {"file": open(filePath, "rb")}
    print(filePath)
    r=requests.post(url,headers=headers,files=files,data=data)
    if r.status_code == 200:
        if r.json()['code'] != 0:
            logging.info("upload video:" + r.text)
    else:
        logging.info("upload video:" + r.text)


if __name__ == '__main__':
    # os.system(". ./sourcefile")
    # print("upload video")
    #api_server = os.environ.get("API_SERVER")
    api_server = "172.16.20.4:8901"
    start_time = os.environ.get("START_TIME")
    end_time = os.environ.get("END_TIME")
    #file_path = os.environ.get("FILE_PATH")
    file_path = '/apps/ezviz/scripts/videos'
    target_path = os.environ.get("TARGET_PATH")
    device_serial = os.environ.get("DEVICE_LIST")
    files = os.listdir(file_path)
    for filename in files:
        portion = os.path.splitext(filename)
        if portion[1] ==".mpg":
            newname = portion[0]+".mp4"
            os.chdir(file_path)
            os.rename(filename,newname)
    get_file(file_path,getToken.get_admin_token(api_server),api_server,target_path)
