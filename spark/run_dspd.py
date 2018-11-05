from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
import os, sys
pcd = os.path.dirname(os.getcwd())
sys.path.append(pcd+'/config')
import dspd_variables as dv
import my_mail as mm
import argparse
import logging
from deriveSpeed import calcDSPD

def p_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--date',required=True)
    parser.add_argument('-t','--hour',required=True)
    parser.add_argument('-s','--stage',required=True)
    args = parser.parse_args()
    return args

args = p_args()

warehouse_location = abspath("spark-warehouse")
spark = SparkSession.\
        builder.\
        appName("Request Derived Speed Hourly").\
        config("spark.sql.warehouse.dir",warehouse_location).\
        enableHiveSupport().\
        getOrCreate()

logging.basicConfig(
            format   = '-- [%(asctime)s - %(levelname)s]\t %(message)s',
            datefmt  = '%Y%m%d %I:%M%S%p',
            filename = './logs/derived_speed.log')
logging.root.level = logging.INFO

try:
    for cntry in dv.cntry_list:
        calcDSPD(dt = args.date, hr = args.hour, stage = args.stage, country = cntry)
        logging.info("Finished hourly derived speed calculation for {} {}, {}:00".format(cntry,args.date,args.hour))
    spark.stop()
except Exception as e:
    errors = str(e)
    mm.send_msg("ALTER: Request Derived Speed Hourly",errors)
    spark.stop()
