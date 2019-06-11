#!/usr/bin/env python
import pika
import json
from time import sleep
import sys

# ./ezviz records get 1 2019-05-30\ 00:00:00 2019-05-30\ 09:00:00 C90674290 WGXWZT a287e05ace374c3587e051db8cd4be82 at.bg2xm8xf03z5ygp01y84xxmv36z54txj-4n5jmc9bua-0iw2lll-qavzt882f
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='127.0.0.1'))
chanPlay = connection.channel()
chanPlay.exchange_declare(exchange="ezviz.exchange.rtplay", exchange_type="direct")

# args = {"x-max-priority": 10}
# args["x-expires"] = 10 * 1000
chanPlay.queue_declare(queue='ezviz.work.queue.rtplay', durable=False)

chanPlay.queue_bind(exchange="ezviz.exchange.rtplay",
                   queue='ezviz.work.queue.rtplay', routing_key='rtplay')

chanStop = connection.channel()
chanStop.exchange_declare(exchange="ezviz.exchange.rtplay", exchange_type="direct")

# args = {"x-max-priority": 10}
# args["x-expires"] = 10 * 1000
chanStop.queue_declare(queue='ezviz.work.queue.rtstop_', durable=False)

chanStop.queue_bind(exchange="ezviz.exchange.rtplay",
                   queue='ezviz.work.queue.rtstop_', routing_key='rtstop_')

body = {}
body["cmd"] = "rtstop"
body["chanId"] = 1
body["devSn"] = "C90842444" #"C90842467"  #"C90842444" #"C90843626" #"C90842444" #"C90843484"
body["devCode"] = "bcd"
body["uuid"] = "abcd"
body["quality"] = 0


if sys.argv[1] == "rtplay":
    body["cmd"] = "rtplay"
    chanPlay.basic_publish(exchange='ezviz.exchange.rtplay', routing_key='rtplay', body= json.dumps(body))
else:
    body["cmd"] = "rtstop"
    chanStop.basic_publish(exchange='ezviz.exchange.rtplay', routing_key='rtstop_', body= json.dumps(body))



print(" [x] Sent " + json.dumps(body))
connection.close()