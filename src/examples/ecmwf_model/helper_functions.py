import os
import math
import pickle
from sklearn.metrics import mean_squared_error


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

