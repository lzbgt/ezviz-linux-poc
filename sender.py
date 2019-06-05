#!/usr/bin/env python
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.0.127'))
channel = connection.channel()

args = {"x-max-priority": 1}
channel.queue_declare(queue='ezviz.work.queue.playback', durable=True, arguments=args)

channel.basic_publish(exchange='ezviz.exchange.default', routing_key='', body='Hello World!')
print(" [x] Sent 'Hello World!'")
connection.close()