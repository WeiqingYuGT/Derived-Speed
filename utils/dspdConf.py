import os

from conf import Conf
from statuslog import StatusLog

class dspdConf:
    def __init__(self):
        conf = Conf()
        conf.load('dspd.properties','../config')
        self.statuslog = StatusLog(conf,prefix = 'status_log_local')
        self.conf = conf
        self.dspd_key = conf.get('dspd_key_name')
        self.ard_key = conf.get('ard_key_name')

    def getLast(self,key):
        res = self.statuslog.get_latest(key)
        return res

    def updateLog(self,stage,country,time):
        key = self.dspd_key+country+'/'+stage+'/'
        self.statuslog.addStatus(key,time)
