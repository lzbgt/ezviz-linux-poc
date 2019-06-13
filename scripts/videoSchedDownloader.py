import requests,json,time
from datetime import datetime
import os,logging,argparse,shutil
import getToken

'''get command'''
def get_code(start_time,end_time,SN,token):
    try:
        code = "./ezviz records get 1 {startTime} {endTime} 3 {SN} WGXWZT a287e05ace374c3587e051db8cd4be82 {Token}".format(
            startTime=start_time, endTime=end_time, SN=SN, Token=token)
    except Exception as e:
        print(e)
    print(code)
    return code

'''get image'''
def get_file(path,token,address,target):
    try:
        for root, dirs, files in os.walk(path):
            for file in files:
                fileName = file
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
                    upload_video(url, path + "/" + file, token,"rtplay")
                else:
                    shutil.move(path+fileName,target+fileName)
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

'''get device sn'''
def get_device_sn(address,start_time,end_time,device_sn):
    headers = {"X-Authorization": "Bearer " + getToken.get_company_manager_token(address=address)}
    data = {"monitorTargetType":5}
    r = requests.get("http://{}/api/v2/secure/customer/monitor_target_lab_device".format(address), headers=headers,data=data)
    if r.status_code == 200:
        if r.json()['code'] != 0:
            logging.info("upload video:" + r.text)
    else:
        logging.info("upload video:" + r.text)

    result = json.loads(r.text)['data']['list']
    devices= []
    if device_sn is not  None:
    if len(device_sn) != 0:
        devices = str(device_sn).split(",")
        for j in devices:
            os.system(get_code(start_time=start_time, end_time=end_time, SN=j,
                               token=getToken.get_token()))
    else:
        for i in result:
            if i['serialNo'][0] == 'C':
                os.system(get_code(start_time=start_time, end_time=end_time, SN=i['serialNo'],
                                   token=getToken.get_token()))

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

def get_args():
    """
    Supports the command-line arguments listed below.
    """
    parser = argparse.ArgumentParser(description="docker image version")

    parser.add_argument('-s', '--apiserver', required=False, help='apiserver',
                        dest='apiserver', default= '', type=str)

    parser.add_argument('-p', '--path', required=False, help='path',
                        dest='path', default='', type=str)

    parser.add_argument('-t', '--targetpath', required=False, help='targetpath',
                        dest='targetpath', default='', type=str)

    parser.add_argument('-m', '--starttime', required=False, help='starttime',
                        dest='starttime', default='', type=str)

    parser.add_argument('-e', '--endtime', required=False, help='endtime',
                        dest='endtime', default='', type=str)

    parser.add_argument('-d', '--deviceSn', required=False, help='deviceSn',
                        dest='deviceSn', default='', type=str)

    args = parser.parse_args()
    return args

input_args = get_args()





if __name__ == '__main__':
    # os.system(". ./sourcefile")
    # print("upload video")
    api_server = os.environ.get("API_SERVER")
    start_time = os.environ.get("START_TIME")
    end_time = os.environ.get("END_TIME")
    #file_path = os.environ.get("FILE_PATH")
    file_path = '/apps/ezviz/scripts/videos'
    target_path = os.environ.get("TARGET_PATH")
    device_serial = os.environ.get("DEVICE_LIST")
    #get_device_sn(api_server,start_time,end_time,device_serial)
    get_file(file_path,getToken.get_admin_token(api_server),api_server,target_path)
