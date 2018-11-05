from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
import os, sys
from pyspark.sql.functions import collect_set, struct, col, greatest
import pyspark.sql.functions as F
from speedCalc import SpD
pcd = os.path.dirname(os.getcwd())
sys.path.append(pcd+'/config')
import dspd_variables as dv
import my_mail as mm
import argparse
from pyspark.sql.types import *
from datetime import datetime, timedelta
import logging
import json

logging.basicConfig(
            format   = '-- [%(asctime)s - %(levelname)s]\t %(message)s',
            datefmt  = '%Y%m%d %I:%M%S%p',
            filename = './logs/hrdspd.log')
logging.root.level = logging.INFO

warehouse_location = abspath("spark-warehouse")
spark = SparkSession.\
        builder.\
        appName("Request Derived Speed Hourly").\
        config("spark.sql.warehouse.dir",warehouse_location).\
        enableHiveSupport().\
        getOrCreate()

spark.sparkContext.addFile("speedCalc.py")

class DSPD:
    def __init__(self,dt,hr,stage,country,path,output):
        self.dt = dt
        self.hr = hr
        self.cntry = country
        self.keys = ['logType','y','m','d','h','stage','isFilled','confScore']
        self.path = path + "country={}/".format(country)
        self.output = output + "country={}/".format(country)
        self.stage=stage

    def load(self):
        dat = spark.read.format('orc').option("compression","zlib").load(self.path)
        dt1 = datetime.strptime(self.dt,"%Y-%m-%d")
        dat = dat.where("y={} and m={} and d={} and h={} and stage={}".format(dt1.strftime("%Y"),\
            dt1.strftime("%m"),dt1.strftime("%d"),self.hr,self.stage))
        return dat

    def write(self,df):
        df.write.orc(self.output,compression='zlib',mode='append',partitionBy=self.keys)
        return

def dumpJSON(angle,dspd):
    return json.dumps({"angle":angle,"dspd":dspd})

def toStr(datn,coln):
    cols = datn.columns
    datn = datn.withColumn(coln+'1',F.when(col(coln)<10,F.concat(F.lit('0'),datn[coln].\
        cast(StringType()))).otherwise(datn[coln].cast(StringType()))).drop(coln).\
        withColumnRenamed(coln+'1',coln).select(cols)
    return datn

def calcDSPD(dt, hr, country, stage, upper = dv.upper, lower = dv.lower, thres = dv.thres):
    SD = SpD(upper = upper, lower = lower, thres = thres)

    dspd_helper = DSPD(dt=dt,hr=hr, stage=stage, country=country, path=dv.input_path, output=dv.output_path)
    dat = dspd_helper.load()
    datn = dat.where("(too_freq_uid!=false or r_s_info is not null) and sl_adjusted_confidence in (94,95)")
    datn = datn.select(['request_id']+dspd_helper.keys)\
            .withColumn("derived_speed",F.lit(dumpJSON(-1.0,-1.0)))
    for coln in ['m','d','h']:
        datn = toStr(datn,coln)

    logging.info("Writing data without derived speed and angle for {}, {}, {}".\
            format(dspd_helper.dt,dspd_helper.hr,dspd_helper.cntry))
    dspd_helper.write(datn)
    logging.info("Done writing")

    datc = dat.where("too_freq_uid=false and r_s_info is null and sl_adjusted_confidence in (94,95)")\
            .withColumn("sec",F.round(dat["r_timestamp"]/(1000*float(lower)),0)*float(lower))\
            .groupby(['uid','sec'])\
            .agg(collect_set(struct(*(['request_id']+dspd_helper.keys))).alias('val_info'),\
                F.avg(F.round('latitude',5)).alias('lat'),F.avg(F.round('longitude',5)).alias('long'))\
            .groupby('uid')\
            .agg(collect_set(struct('sec','val_info','lat','long')).alias('comb'))
    datc = datc\
            .repartition(1000)\
            .rdd\
            .map(lambda x:SD.getSpeed(x))\
            .flatMap(lambda x:x)
#    schema = StructType(dat.schema.fields+[StructField("derived_speed",StringType(),True)])
    datc = spark.createDataFrame(datc,schema=datn.schema)
    logging.info("Writing data with derived speed and angle for {}, {}, {}".\
        format(dspd_helper.dt,dspd_helper.hr,dspd_helper.cntry))
    dspd_helper.write(datc)
    logging.info("Done writing")


