#----------------------------------------
#---   Path variables
#----------------------------------------
input_path = "tmp/weiqing/test/"
output_path = "tmp/weiqing/result/"

#----------------------------------------
#---   Countries
#----------------------------------------
cntry_list = ['us']

#----------------------------------------
#---   Model Parameters
#----------------------------------------
upper = 20
lower = 5
thres = 300

#----------------------------------------
#---   Spark Configuration
#----------------------------------------
master = 'yarn'
num_executors = '40'
queue = 'weiqing'
driver_memory = '12g'
executor_memory = '12g'
executor_cores = '3'
max_result_size = '4g'
dynamicAllocation = 'false'
scheduler_capacity = '40000'

#---------------------------------------
#---   Status Log Keys
#---------------------------------------
ard_key = "ard/"
dspd_key = "derived_speed/"
