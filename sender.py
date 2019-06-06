#!/usr/bin/env python
import pika
import json
from time import sleep
import sys

# ./ezviz records get 1 2019-05-30\ 00:00:00 2019-05-30\ 09:00:00 C90674290 WGXWZT a287e05ace374c3587e051db8cd4be82 at.bg2xm8xf03z5ygp01y84xxmv36z54txj-4n5jmc9bua-0iw2lll-qavzt882f
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.0.105'))
channel = connection.channel()
channel.exchange_declare(exchange="ezviz.exchange.default", exchange_type="direct")

# args = {"x-max-priority": 10}
# args["x-expires"] = 10 * 1000
channel.queue_declare(queue='ezviz.work.queue.rtplay', durable=False)

channel.queue_bind(exchange="ezviz.exchange.default",
                   queue='ezviz.work.queue.rtplay', routing_key='rtplay')

body = {}
body["cmd"] = "rtstop"
body["chanId"] = 1
body["devSn"] = "C90674290"
body["devCode"] = "bcd"
body["uuid"] = "abcd"
body["quality"] = 0

body2 = {}
body2["cmd"] = "rtstop"
body2["chanId"] = 1
body2["devSn"] = "C90674290"
body2["quality"] = 0

if sys.argv[1] == "rtplay":
    body["cmd"] = "rtplay"
else:
    body["cmd"] = "rtstop"
channel.basic_publish(exchange='ezviz.exchange.default', routing_key='rtplay', body= json.dumps(body))

# channel.basic_publish(exchange='ezviz.exchange.default', routing_key='rtplay', body= json.dumps(body2))
# sleep(5)


print(" [x] Sent " + json.dumps(body))
connection.close()