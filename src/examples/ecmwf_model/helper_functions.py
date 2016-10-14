import os
import math
import pickle
import datetime
from sklearn.metrics import mean_squared_error

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from logreader import ingest_data


def rmse(prediction, ground_truth):

    """ Root mean square error for model evaluation """
    prediction = prediction[ground_truth.nonzero()].flatten()
    ground_truth = ground_truth[ground_truth.nonzero()].flatten()
    return math.sqrt(mean_squared_error(prediction, ground_truth))


def ingest_operational_logs(path_ingested_jobs):

    # Darshan
    with open(os.path.join(path_ingested_jobs, "ingested_darshan"), "r") as f:
        darshan_dataset = pickle.load(f)
    print "darshan log data ingested!"

    # IPM
    with open(os.path.join(path_ingested_jobs, "ingested_ipm"), "r") as f:
        ipm_dataset = pickle.load(f)
    print "ipm log data ingested!"

    # stdout
    with open(os.path.join(path_ingested_jobs, "ingested_stdout"), "r") as f:
        stdout_dataset = pickle.load(f)
    print "stdout log data ingested!"

    return darshan_dataset, ipm_dataset, stdout_dataset


def read_accounting_jobs(log_file):

    # get accounting background jobs..
    # ECMWF_acc_data = ingest_data("accounting", "/perm/ma/maab/ngio_logs/ECMWF/cca-jobs-20160201_test_0.csv")
    ecmwf_acc_data = ingest_data("accounting", log_file)
    print "accounting data ingested!"

    # filter 1day of data only..
    timescale_day = (datetime.datetime(2016, 2, 1, 0, 0, 0), datetime.datetime(2016, 2, 2, 0, 0, 0))

    new_joblist = [j for j in ecmwf_acc_data.joblist
                   if timescale_day[0] < datetime.datetime.fromtimestamp(j.time_start) < timescale_day[1]]

    # model all accounting jobs..
    ecmwf_acc_data.joblist = new_joblist
    acc_model_jobs = [j for j in ecmwf_acc_data.model_jobs()]

    return acc_model_jobs
