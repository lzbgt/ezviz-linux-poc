#!/usr/bin/env python
import pika
import json
# ./ezviz records get 1 2019-05-30\ 00:00:00 2019-05-30\ 09:00:00 C90674290 WGXWZT a287e05ace374c3587e051db8cd4be82 at.bg2xm8xf03z5ygp01y84xxmv36z54txj-4n5jmc9bua-0iw2lll-qavzt882f
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.0.127'))
channel = connection.channel()

args = {"x-max-priority": 1}
channel.queue_declare(queue='ezviz.work.queue.playback', durable=True, arguments=args)

body = {}
body["cmd"] = "playback"
body["chanId"] = 1
body["startTime"] = "2019-05-30 00:00:00"
body["stopTime"] = "2019-05-30 09:00:00"
body["devList"] = [
    {"devSn": "C90674290"},
    {"devCode": "WGXWZT"},

];


channel.basic_publish(exchange='ezviz.exchange.default', routing_key='', body= json.dumps(body))


print(" [x] Sent " + json.dumps(body))
connection.close()