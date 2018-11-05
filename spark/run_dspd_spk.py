from os.path import expanduser, join, abspath
import os, sys
pcd = os.path.dirname(os.getcwd())
sys.path.append(pcd+'/config')
import my_mail as mm
import argparse
import logging
import dspd_variables as dv
from string import Template

def p_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--date',required=True)
    parser.add_argument('-t','--hour',required=True)
    parser.add_argument('-s','--stage',required=True)
    args = parser.parse_args()
    return args

args = p_args()

try:
    run_temp = Template("""spark-submit --master ${master} --num-executors ${n_exe} --queue ${q} --driver-memory ${d_mem} --executor-memory ${e_mem} --executor-cores ${n_core} --conf spark.driver.maxResultSize=${max_res} --conf spark.dynamicAllocation.enabled=${dyn_allo} --conf spark.scheduler.listenerbus.eventqueue.capacity=${sche_cap} run_dspd.py -d ${d} -t ${t} -s ${s}""")
    run_cmd = run_temp.substitute(master = dv.master,n_exe = dv.num_executors, q = dv.queue,\
                d_mem = dv.driver_memory, e_mem = dv.executor_memory, n_core = dv.executor_cores,\
                max_res = dv.max_result_size, dyn_allo = dv.dynamicAllocation, sche_cap = dv.scheduler_capacity,\
                d = args.date, t = args.hour, s = args.stage)
    os.system(run_cmd)
except Exception as e:
    errors = str(e)
    mm.send_msg("ALTER: Request Derived Speed Hourly",errors)
    spark.stop()
