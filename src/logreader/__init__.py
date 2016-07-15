from profiler_reader import ingest_allinea_profiles
from scheduler_reader import ingest_accounting_logs, ingest_pbs_logs


def ingest_data(ingest_type, ingest_path):
    """
    A factory for the types of data ingestion we are going to get!
    """
    print "Ingesting data from {}, of type {}".format(ingest_path, ingest_type)

    try:

        return {
            'allinea': ingest_allinea_profiles,
            'pbs': ingest_pbs_logs,
            'accounting': ingest_accountincg_logs
        }[ingest_type](ingest_path)

    except KeyError as e:
        raise ValueError("Ingestion type unknown in config: {}".format(ingest_type))
