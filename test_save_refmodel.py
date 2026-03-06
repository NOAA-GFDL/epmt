import numpy as np
import epmt
from epmt import epmt_query
# epmt_outliers contains the EPMT Outlier Detection API
from epmt import epmt_outliers as eod
# epmt_stat contains statistical functions
from epmt import epmt_stat as es
from epmt import epmt_query as eq
from epmt.orm import *
import pandas
from pandas import DataFrame

feature_list = [ 'duration', 'rchar', 'syscr', 'syscw', 'wchar', 'cstime', 'cutime', 'majflt', 'cpu_time', 'minflt', 'rssmax', 'cmajflt','cminflt', 'inblock', 'outblock', 'usertime', 'num_procs', 'starttime', 'vol_ctxsw', 'read_bytes', 'systemtime', 'time_oncpu', 'timeslices', 'invol_ctxsw', 'write_bytes', 'time_waiting', 'cancelled_write_bytes']
random_jobs =  eq.get_jobs(limit = 100, fmt = 'dict', trigger_post_process=False)

try:
    r = eq.get_refmodels("bronx_test_model")
    eq.delete_refmodels(r[0]['id'])
finally:
    print("ready for new bronx model")
r = eq.create_refmodel(random_jobs, methods=es.mvod_classifiers(), features = feature_list, name = "bronx_test_model")

eq.save_refmodel(ReferenceModel, r['jobs'], r['computed'], r['info_dict'], r['enabled'], name='test_name', tag={}, op_tags=[])
