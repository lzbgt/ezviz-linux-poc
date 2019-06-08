# !/usr/bin/python
# -*- coding: UTF-8 -*-

import requests,json

'''get token'''
def get_token():
    data={"appKey":"a287e05ace374c3587e051db8cd4be82",
          "appSecret":"f01b61048a1170c4d158da3752e4378d"}
    r= requests.post("https://open.ys7.com/api/lapp/token/get",data=data)
    return json.loads(r.text)['data']['accessToken']

'''get comany manage token'''
def get_company_manager_token(address):
    data = {"username": "qz@qz", "password": "ilabservice"}
    r = requests.post("http://{}/api/v2/unsecure/login".format(address), json=data)
    token = json.loads(r.text)['data']['token']
    return token

'''get admin token'''
def get_admin_token(address):
    data = {"username": "administrator", "password": "test1234"}
    r = requests.post("http://{}/api/v2/unsecure/login".format(address), json=data)
    token = json.loads(r.text)['data']['token']
    return token

print (get_token())

