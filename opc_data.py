
import os, sys, time, datetime, re, json, queue, shutil, threading, random, concurrent.futures, logging
from  multiprocessing import Pool, Process
from multiprocessing.pool import ThreadPool
from subprocess import Popen, PIPE, DEVNULL
from threading import Timer
import requests, redis, zlib

logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                     format='%(asctime)s %(module)s:%(lineno)s %(levelname)s: %(message)s')
log = logging.getLogger(__name__)

class OpcData(object):
    def __init__(self, apiAddr):
        self.apiAddr = apiAddr
        pass

    def postData(self, data):
        log.info('{}'.format(data))
        r = requests.patch(self.apiAddr, data=data)
        if r.status_code != 204:
            log.error('{}'.format(r))
            raise Exception('Failed Patch')

    def run(self):
        while True:
            data = {}
            # opc -s KEPware.KEPServerEx.V4 -r IP100.THP.LAB_01_HT IP100.THP.LAB_01_TT IP100.THP.LAB_01_PT IP100.THP.LAB_01_HT IP100.THP.LAB_02_TT IP100.THP.LAB_02_PT
            proc = Popen(['opc', '-s', 'KEPware.KEPServerEx.V4', '-r', 'IP100.THP.LAB_01_HT', 'IP100.THP.LAB_01_TT', 'IP100.THP.LAB_01_PT', 'IP100.THP.LAB_02_HT', 'IP100.THP.LAB_02_TT', 'IP100.THP.LAB_02_PT'], bufsize=1, shell=False, stdout=PIPE, stderr=DEVNULL)
            thisTimer = Timer(3, proc.kill)
            try:
                thisTimer.start()
                for line in proc.stdout:
                    # IP100.THP.LAB_01_HT     57.4562     Good     07/04/19 06:18:19
                    m = re.search(r'IP100\.THP\.LAB_0(\d)_(\w+)\s+(-?[\d\.]+)', line.decode('utf-8'))
                    if m is not None:
                        #log.info("matched: {}, {}, {}".format(m.group(1), m.group(2), m.group(3)))
                        #sys.stdout.buffer.write(line)
                        #sys.stdout.buffer.flush()
                        data[m.group(2).lower()+m.group(1)] = float(m.group(3))
                        
                proc.stdout.close()
                proc.wait()
                log.info('{}'.format(data))
                if len(data) != 0:
                    self.postData(data)
                thisTimer.cancel()
                time.sleep(1)
            except Exception as e:
                log.error("[EXCEPTION] {},{}".format(fileName, e))

if __name__ == '__main__':
    API_ADDR = 'http://42.159.154.236:10030/opc?id=eq.1'
    opc = OpcData(API_ADDR);
    opc.run()
