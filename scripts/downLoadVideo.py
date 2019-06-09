# !/usr/bin/python
# -*- coding: UTF-8 -*-

import requests,json,time
from datetime import datetime
import os,logging
import getToken
import argparse

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)

'''get command'''
def get_code(start_time,end_time,SN,token):
    code = "./ezviz records get 1 {startTime} {endTime} 3 {SN} WGXWZT a287e05ace374c3587e051db8cd4be82 {Token}".format(
        startTime=start_time,endTime=end_time,SN=SN,Token=token)
    return code

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

'''upload one video'''
def get_file(path,token,address):
    try:
        fileName = os.path.basename(path)
        size = os.path.getsize(path)
        if size != 0:
            list = fileName.split("_")
            if len(list[0]) == 14 and len(list) ==2:
                date = datetime.strptime(list[0], "%Y%m%d%H%M%S")
                timeArray = time.strptime(str(date), "%Y-%m-%d %H:%M:%S")
                timestamp = int(time.mktime(timeArray) * 1000)
                print(timestamp)
                url = "http://{address}/api/v2/secure/admin/private/video/upload/camera/".format(
                    address=address) + str(list[1].split(".")[0]) + "/start/" + str(timestamp) + "/length/" + str(0)
                '''upload video'''
                upload_video(url, path, token,"playback")
            else:
                logging.info("file type error"+fileName)
            '''delete file'''
            del_file(path)
    except Exception as e:
        logging.info("get file fail")

'''delete file'''
def del_file(path):
    result = os.path.exists(path)
    if result == 'False':
        logging.info("file not exist")
    else:
        os.remove(path)


def get_args():
    """
    Supports the command-line arguments listed below.
    """
    parser = argparse.ArgumentParser(description="docker image version")

    parser.add_argument('-s', '--apiserver', required=False, help='apiserver',
                        dest='apiserver', default= '', type=str)

    parser.add_argument('-i', '--videofile', required=False, help='videofile',
                        dest='videofile', default='', type=str)

    args = parser.parse_args()
    return args


input_args = get_args()


if __name__ == '__main__':
    #os.system(". ./sourcefile")
    print("upload video")
    #os.system(get_code(start_time='2019-05-30\ 00:00:00',end_time='2019-05-30\ 09:00:00',SN='C90674290',token=get_token()))
    get_file('/apps/ezviz/'+input_args.videofile,getToken.get_admin_token(input_args.apiserver),input_args.apiserver)
