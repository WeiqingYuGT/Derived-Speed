from os.path import expanduser, join, abspath
import os, sys
pcd = os.path.dirname(os.getcwd())
sys.path.append(pcd+'/config')
sys.path.append(pcd+'/utils')
from dspdConf import dspdConf
import my_mail as mm
import argparse
import logging
import dspd_variables as dv
from string import Template
from datetime import datetime, timedelta

def genKey(prefix,stage,country):
    return prefix+country+'/'+stage+'/'

def getDH(dt):
    dat = dt.strftime("%Y-%m-%d")
    hr = int(dt.strftime("%H"))
    return [dat,hr]

try:
    dc = dspdConf()
    for cntry in dv.cntry_list:
        for stage in ['0','1','2','3']:
            ard_dt = dc.getLast(genKey(dv.ard_key))
            ard_dt = datetime.strptime(ard_dt,"%Y-%m-%d-%H")
            dspd_dt = dc.getLast(genKey(dv.dspd_key))
            dspd_dt = datetime.strptime(dspd_dt,"%Y-%m-%d-%H")
            n_hour = (ard_dt-dspd_dt).seconds/3600
            dhseq = [getDH(dspd_dt+timedelta(hours=x)) for x in range(1,n_hour+1)]
            for dt, hr in dhseq:
                run_temp = Template("""spark-submit --master ${master} --num-executors ${n_exe} --queue ${q} --driver-memory ${d_mem} --executor-memory ${e_mem} --executor-cores ${n_core} --conf spark.driver.maxResultSize=${max_res} --conf spark.dynamicAllocation.enabled=${dyn_allo} --conf spark.scheduler.listenerbus.eventqueue.capacity=${sche_cap} run_dspd.py -d ${d} -t ${t} -s ${s} -c ${c}""")
                run_cmd = run_temp.substitute(master = dv.master,n_exe = dv.num_executors, q = dv.queue,\
                    d_mem = dv.driver_memory, e_mem = dv.executor_memory, n_core = dv.executor_cores,\
                    max_res = dv.max_result_size, dyn_allo = dv.dynamicAllocation, sche_cap = dv.scheduler_capacity,\
                    d = dt, t = hr, s = stage, c = cntry)
                dc.updateLog(stage,cntry,dt+'-'+hr)
    os.system(run_cmd)
except Exception as e:
    errors = str(e)
    mm.send_msg("ALTER: Request Derived Speed Hourly",errors)
    spark.stop()
