# import the modules needed

import os
import sys
import datetime

import math
from math import sqrt

import cPickle as pickle

import matplotlib
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist, pdist

import matplotlib.pyplot as plt
from matplotlib import dates
import matplotlib.cm as cm

from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
from sklearn import cross_validation as cv
from sklearn.metrics import mean_squared_error
from sklearn.metrics.pairwise import pairwise_distances

os.sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from logreader import ingest_data
from time_signal import TimeSignal
from synthetic_app import SyntheticApp, SyntheticWorkload
from config.config import Config

# path_ingested_jobs = "/perm/ma/maab/iows_operational"
path_ingested_jobs = '/perm/ma/maab/ngio_ingested_data/my_ingested'


def ecmwf_model():

    # Load config
    config = Config()

    # darshan
    with open( os.path.join(path_ingested_jobs,"ingested_darshan"), "r") as f:
        darshan_dataset = pickle.load(f)
    print "darshan log data ingested!"

    dsh_model_jobs = [j for j in darshan_dataset.model_jobs()]
    print "model jobs created!"

    # transform into synthetic apps..
    sa_list = []
    for cc, job in enumerate(dsh_model_jobs[:10]):
        app = SyntheticApp(
                            job_name="RS-appID-{}".format(cc),
                            time_signals=job.timesignals,
                            ncpus=2,
                            nnodes=1,
                            time_start=job.time_start
                            )

        sa_list.append(app)
# ///////////////////////////////////////////


# ///////////////////////////////////////////
if __name__ == '__main__':
    ecmwf_model()