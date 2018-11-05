import sys
sys.path.append('/home/xad/share/python/')

import os

from xad.common.conf import Conf
from xad.common.statuslog import StatusLog

class dspdConf:
    def __init__(self,path):
        conf = Conf()
        conf.load('dspd.properties',path+'/../config')
        self.statuslog = StatusLog(conf,prefix = 'status_log_local')
        self.conf = conf
