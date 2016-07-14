from profiler_reader import ingest_allinea_profiles


def ingest_data(ingest_type, ingest_path):
    """
    A factory for the types of data ingestion we are going to get!
    """
    print "Ingesting data from {}, of type {}".format(ingest_path, ingest_type)

    try:

        return {
            'allinea': ingest_allinea_profiles
        }[ingest_type](ingest_path)

    except KeyError as e:
        raise ValueError("Ingestion type unknown in config: {}".format(ingest_type))
