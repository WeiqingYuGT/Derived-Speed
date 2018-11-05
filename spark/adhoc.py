from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
import os, sys
from pyspark.sql.functions import collect_set, struct, col, greatest
import pyspark.sql.functions as F
from speedCalc import SpD
ppcd = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.append(ppcd+'/common')
import my_mail as mm
from hive_utils import HiveUtils
hv = HiveUtils(app_name='derived_speed')
import argparse
from pyspark.sql.types import *
from datetime import datetime, timedelta
import itertools
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
    def __init__(self,dt,hr,stage,country='us',path='/data/science_core_ex/',output='tmp/weiqing/science_core_ex1/',offset=0):
        self.dt = dt
        self.hr = hr
        self.cntry = country
#        hier_fixed = [[country],['display','display_dr','euwest1','exchange'],['fill','nf'],['pos','rest','tll']]
#        cdh = datetime.strptime(dt+' '+hr,'%Y-%m-%d %H')
#        dh = [self._getDH(cdh+timedelta(hours=x)) for x in range(0-offset,1+offset)]
#        self.dirs = [list(x[:2])+y+list(x[2:]) for x in list(itertools.product(*hier_fixed)) for y in dh]
        self.keys = ['logType','y','m','d','h','stage','isFilled','confScore']
        self.path = path + "country={}/".format(country)
        self.output = output + "country={}/".format(country)
    def _getDH(self,dh):
        return dh.strftime("%Y/%m/%d %H").split(" ")
    def _addCols(self,df,val):
        for i in range(len(val)):
            df = df.withColumn(self.keys[i],F.lit(val[i]))
        return df
    def _changeDtFormat(self,val):
        val1 = list(val)
        for i in range(len(self.keys)):
            if self.keys[i]=='dt':
                val1[i] = self.dt
        return val1
    def _load1(self):
        dirs = self.dirs
        d = dirs[0]
        df = spark.read.format('orc').load(self.path+'/'.join(d))
        df = self._addCols(df,self._changeDtFormat(d))
        for d in dirs[1:]:
            try:
                df = df.union(self._addCols(spark.read.format('orc').\
                    load(self.path+'/'.join(d)),self._changeDtFormat(d)))
            except Exception as e:
                logging.warn("Error processing directory {}, skipped this dir...".format(self.path+'/'.join(d)))
        return df
    def _load(self):
        
    def _write(self,df):
        df.where("dt='{}' and hr='{}'".format(self.dt,self.hr)).\
            write.orc(self.output,mode='append',partitionBy=self.keys)
        return
    def _dumpJSON(self,angle,dspd):
        return json.dumps({"angle":angle,"dspd":dspd})

def adhoc(dt, hr, upper = 20, lower = 5, thres = 300):
    SD = SpD(upper = upper, lower = lower, thres = thres)
    dat = spark.sql("""select * from weiqingyu.a_request_{}_par where hour='{}'""".format(datetime.strptime(dt,"%Y-%m-%d").strftime("%y%m%d"),hr))
    datc = dat.withColumn("sec",F.round(dat["r_timestamp"]/(1000*float(lower)),0)*float(lower))\
            .groupby(['uid','sec'])\
            .agg(collect_set(struct('fp_matches', 'hour')).alias('val_info'),\
                F.avg(F.round('latitude',5)).alias('lat'),F.avg(F.round('longitude',5)).alias('long'))\
            .repartition(400)\
            .groupby('uid')\
            .agg(collect_set(struct('sec','val_info','lat','long')).alias('comb'))
    datc = datc\
            .repartition(400)\
            .rdd\
            .map(lambda x:SD.getSpeed(x))\
            .flatMap(lambda x:x)
    schema = StructType([StructField("hr",StringType(),True),\
                   StructField("fp_matches",ArrayType(StructType(\
                            [StructField("proximity_mode",IntegerType(),True),\
                            StructField("fp_brand",StringType(),True),\
                            StructField("fp_sic",StringType(),True),\
                            StructField("weight",FloatType(),True),\
                            StructField("hash_key",StringType(),True),\
                            StructField("is_poly",IntegerType(),True),\
                            StructField("block_id",IntegerType(),True),\
                            StructField("is_employee",BooleanType(),True),\
                            StructField("open_status",StringType(),True)\
                            ,StructField("ha_match",BooleanType(),True)\
                            ]),True),True),
                   StructField("dspd",DoubleType(),True),\
                   StructField("angle",DoubleType(),True),\
                   StructField("uid",StringType(),True)])
    try:
        datc = spark.createDataFrame(datc,schema=schema)
        datc.registerTempTable('temp')
        spark.sql("""insert into weiqingyu.temp_res partition(dt='{}',hr='{}') select fp_matches, dspd, angle, uid from temp""".format(dt, hr))
    except Exception as e:
        errors = str(e)
        raise Exception(errors)
        print("Upper bound: {}; Lower bound: {}".format(upper,lower))
#    def ver_1_fp(upper = 20, lower = 5, thres = 300):
#        SD = SpD(upper = upper, lower = lower, thres = thres)
#
#        dat = self._load()
#        datn = dat.where("too_freq_uid!=false or r_s_info is not null or sl_adjusted_confidence not in (94,95)")
#        datn = datn.select(['request_id']+self.keys)\
#            .withColumn("derived_speed",F.lit(self._dumpJSON(-1.0,-1.0)))
#        logging.info("Writing data without derived speed and angle for {}, {}, {}".format(self.dt,self.hr,self.cntry))
#        self._write(datn)
#        logging.info("Done writing")
#
#        datc = dat.where("too_freq_uid=false and r_s_info is null and sl_adjusted_confidence in (94,95)")\
#            .withColumn("sec",F.round(dat["r_timestamp"]/(1000*float(lower)),0)*float(lower))\
#            .groupby(['uid','sec'])\
#            .agg(collect_set(struct(*(['request_id']+self.keys))).alias('val_info'),\
#                F.avg(F.round('latitude',5)).alias('lat'),F.avg(F.round('longitude',5)).alias('long'))\
#            .groupby('uid')\
#            .agg(collect_set(struct('sec','val_info','lat','long')).alias('comb'))
#        datc = datc\
#            .repartition(2000)\
#            .rdd\
#            .map(lambda x:SD.getSpeed(x))\
#            .flatMap(lambda x:x)
#        schema = StructType(dat.schema.fields+[StructField("derived_speed",StringType(),True)])
#        try:
#            datc = spark.createDataFrame(datc,schema=schema)
#            logging.info("Writing data with derived speed and angle for {}, {}, {}".format(self.dt,self.hr,self.cntry))
#            self._write(datc)
#            logging.info("Done writing")
#        except Exception as e:
#            errors = str(e)
#            raise Exception(str(datc.take(2)+errors))
#        print("Upper bound: {}; Lower bound: {}".format(upper,lower))



#def p_args():
#    parser = argparse.ArgumentParser()
#    parser.add_argument('-u','--upper', default = 20)
#    parser.add_argument('-l','--lower', default = 5)
#    parser.add_argument('-t','--threshold', default = 1500)
#    parser.add_argument('-d','--date',required=True)
#    parser.add_argument('-h','--hour',required=True)
#    args = parser.parse_args()
#    return args
#
try:
#    args = p_args()
#    ver_1_fp(upper = args.upper,lower = args.lower, thres = args.threshold, dt = args.date)
    for dt in ['2018-10-10','2018-10-11','2018-10-12','2018-10-13','2018-10-14','2018-10-15','2018-10-16']:
        for hr in ['0'+str(x) if x<10 else str(x) for x in range(24)]:
            adhoc(dt, hr)
    spark.stop()
except Exception as e:
    errors = str(e)
    mm.send_msg("ALTER: Request Derived Speed Hourly",errors)
    raise Exception(errors)
    spark.stop()

