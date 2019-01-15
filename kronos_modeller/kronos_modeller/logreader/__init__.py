# (C) Copyright 1996-2018 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from darshan import DarshanDataSet, Darshan3DataSet
from ipm import IPMDataSet
from kronos_modeller.logreader.stdout_ecmwf import StdoutECMWFDataSet
from profiler_reader import ingest_allinea_profiles
from scheduler_reader import ingest_accounting_logs, ingest_pbs_logs, ingest_epcc_csv_logs

simple_ingest_mapping = {
    'allinea': ingest_allinea_profiles,
    'pbs': ingest_pbs_logs,
    'accounting': ingest_accounting_logs,
    'epcc_csv': ingest_epcc_csv_logs,
}

class_ingest_mapping = {
    'darshan': DarshanDataSet,
    'darshan3': Darshan3DataSet,
    'ipm': IPMDataSet,
    'stdout-ecmwf': StdoutECMWFDataSet
}

ingest_types = sorted(simple_ingest_mapping.keys() + class_ingest_mapping.keys())


def ingest_data(ingest_type, ingest_path, ingest_config=None, global_config=None):
    """
    A factory for the types of data ingestion we are going to get!
    """
    print "Ingesting data from {}, of type {}".format(ingest_path, ingest_type)

    try:

        ingester = simple_ingest_mapping[ingest_type]

    except KeyError:

        # Start adding LogReader CLASSES
        # TODO: More specific configuration should be possible.

        try:

            ingester = class_ingest_mapping[ingest_type].from_logs_path

            # Did we supply any custom configuration?
            if ingest_config is None:
                ingest_config = global_config.ingestion.get(ingest_type, {}) if global_config else {}

        except KeyError:
            raise ValueError("Ingestion type unknown in config: {}".format(ingest_type))

    return ingester(ingest_path, ingest_config)
