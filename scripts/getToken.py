# !/usr/bin/python
# -*- coding: UTF-8 -*-

import requests,json
import os,logging
import redis

logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
log = logging.getLogger(__name__)
env = {}
env['redisAddr'] = os.getenv('EZ_REDIS_ADDR', '139.219.136.0')
env['redisPort'] = int(os.getenv('EZ_REDIS_PORT', '6379'))

try:
    redisConn = redis.Redis(host=env['redisAddr'], port=env['redisPort'], db=0)
except Exception as e:
    log.error("exception {}", e)

'''get token'''
def get_token():
    token = 'INVALID_TOKEN'
    tok = redisConn.get('ystoken')
    if tok is not None:
        return tok

    data={"appKey":"a287e05ace374c3587e051db8cd4be82",
          "appSecret":"f01b61048a1170c4d158da3752e4378d"}
    r= requests.post("https://open.ys7.com/api/lapp/token/get",data=data)

    # bugfix
    if r.status_code != 200 and r.json().get('code') != '200':
            log.error('failed request yscloud token: {} '.format(r.text))
    elif r.json().get('data') is None:
        log.error('failed get token: {}'.format(r.text))
    else:
        token = r.json()['data']['accessToken']
    
    if token != 'INVALID_TOKEN':
        redisConn.set("ystoken", token, ex=6*24*60*60)

    return token

'''get comany manage token'''
def get_company_manager_token(address):
    token = "INVALID_TOKEN"
    data = {"username": "qz@qz", "password": "ilabservice"}
    r = requests.post("http://{}/api/v2/unsecure/login".format(address), json=data)
    # bugfix
    if r.status_code != 200 and r.json().get('code') != '200':
            log.error('failed request platform token: {} '.format(r.text))
    elif r.json().get('data') is None:
        log.error('failed get token: {}'.format(r.text))
    else:
        token = r.json()['data']['accessToken']
    
    return token

'''get admin token'''
def get_admin_token(address):
    data = {"username": "administrator", "password": "test1234"}
    r = requests.post("http://{}/api/v2/unsecure/login".format(address), json=data)
    token = json.loads(r.text)['data']['token']
    return token

print (get_token())

