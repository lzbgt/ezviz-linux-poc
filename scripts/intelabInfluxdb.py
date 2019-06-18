from influxdb import InfluxDBClient
import time


class InfluxdbClient:

    def __init__(self):
        self.client = InfluxDBClient('172.100.0.4', 8086, 'ilabservice', 'AqcDoDs[i[rWW4b+aSEw', 'intelab')


    #@retry(stop_max_attempt_number=5, wait_random_min=200, wait_random_max=5000)
    def query_influxdb(self, query_string):
        return self.client.query(query_string)


    #@retry(stop_max_attempt_number=5, wait_random_min=200, wait_random_max=5000)
    def write_data(self, points):
        return self.client.write_points(points)

    def insert_message(self,alarm_time,alarm_pic,device_serial):

        point_value = {
                "measurement": "qzwj_picture",
                "time":alarm_time*1000000,
                "fields": {
                    "alarm_pic": alarm_pic,
                    "device_serial": device_serial
                }
        }
        print(point_value)

        series = [point_value]

        self.client.write_points(series,retention_policy="one_month")

