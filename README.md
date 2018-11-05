Derived Speed 
=====

Derived Speed logic for speed calculation. 

Usage:
=====

To generate the derived speed logic for a specific date/hour/stage, run the following command:
```
python run_dspd_spk.py -d {date} -t {hour} -s {stage}
```

All the parameters needed can be tuned in the `config/dspd_variables.py` file.

Specifically, one may want to change the `input_path` to the location of ARD output location, `output_path` to wherever the desired output is, `cntry_list` the list of country to process and multiple Spark configuration used in `spark-submit`.
