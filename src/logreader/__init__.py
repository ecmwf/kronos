from profiler_reader import ingest_allinea_profiles
from scheduler_reader import ingest_accounting_logs, ingest_pbs_logs, ingest_epcc_csv_logs
from darshan import DarshanDataSet
from ipm import IPMDataSet


def ingest_data(ingest_type, ingest_path, global_config=None):
    """
    A factory for the types of data ingestion we are going to get!
    """
    print "Ingesting data from {}, of type {}".format(ingest_path, ingest_type)

    try:

        return {
            'allinea': ingest_allinea_profiles,
            'pbs': ingest_pbs_logs,
            'accounting': ingest_accounting_logs,
            'epcc_csv': ingest_epcc_csv_logs,
        }[ingest_type](ingest_path)

    except KeyError as e:

        # Start adding LogReader CLASSES
        # TODO: More specific configuration should be possible.

        try:

            dataset_class = {
                'darshan': DarshanDataSet,
                'ipm': IPMDataSet
            }[ingest_type]

            # Did we supply any custom configuration?
            cfg = global_config.ingestion.get(ingest_type, {}) if global_config else {}

            return dataset_class.from_logs_path(ingest_path, cfg)

        except KeyError as e:
            raise ValueError("Ingestion type unknown in config: {}".format(ingest_type))
