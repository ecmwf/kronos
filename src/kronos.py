import os
import sys
import argparse

from exceptions_iows import ConfigurationError
from kpf_handler import KPFFileHandler
from config.config import Config
from kronos_model import IOWSModel
from workload_data import WorkloadData


class Kronos(object):
    """
    Kronos main class
    """

    def __init__(self, config):

        self.config = config
        self.workloads = []
        self.model_jobs = None
        self.synthetic_apps = None

        if self.config.verbose:
            print "VERBOSE logging enabled"

    def ingest_data(self):
        """
        Depending on the supplied config, ingest data of various types.
        """

        # load job data sets from a kpf file..
        self.workloads = KPFFileHandler().load_kpf(os.path.join(self.config.dir_input, self.config.kpf_file))

        # collect all the ingested data into a workload
        loaded_workloads = WorkloadData()
        for wl in self.workloads:
            loaded_workloads.append_workload(
                                            model_jobs=wl['jobs'],
                                            set_tag=wl['tag']
                                            )

        # try to export the workload
        loaded_workloads.export_workloads(kpf_filename=os.path.join(self.config.dir_output, 'test_1.kpf'))

    def scale_workload(self):
        model = IOWSModel(self.config, self.model_jobs)
        synapps = model.create_scaled_workload("time_plane", "Kmeans", self.config.unit_sc_dict)
        self.synthetic_apps = synapps

        if self.config.verbose:
            print synapps.verbose_description()

    def export(self):
        self.synthetic_apps.export_synth_apps(self.config.IOWSMODEL_TOTAL_METRICS_NBINS)

    def run(self):
        """
        Main execution routine (default if no specific plugin is requested)
        """
        print "\nBegining data ingestion...\n----------------------------------"
        self.ingest_data()
        print "\nIngested data sets: [\n" + ",\n".join(["    {}".format(d['tag']) for d in self.workloads]) + "\n]"

        # print "\nScaling model workload...\n----------------------------------"
        # self.scale_workload()
        # print "Scaled workload: {}".format(self.synthetic_apps)
        #
        # print "\nOutputting synthetic app input...\n----------------------------------"
        # self.export()


if __name__ == "__main__":

    # read other arguments if present..
    parser = argparse.ArgumentParser(description='Kronos software')
    parser.add_argument('input_file', type=str)
    parser.add_argument('-m', "--model", help="generate model", action='store_true')
    parser.add_argument('-r', "--run", help="run the model on HPC", action='store_true')
    parser.add_argument('-p', "--postprocess", help="postprocess run results", action='store_true')
    parser.add_argument('-i', "--input", help="postprocess sa workload", action='store_true')
    parser.add_argument('-o', "--output", help="postprocess sa run results", action='store_true')
    args = parser.parse_args()

    # command line keys checks..
    postprocess_flag = None
    if args.postprocess and not (args.input or args.output):
        raise ConfigurationError(" specify either 'input' or 'output'")
    else:
        if args.input:
            postprocess_flag = 'input'
        elif args.output:
            postprocess_flag = 'output'

    try:
        try:

            config = Config(config_path=args.input_file)

        except (OSError, IOError) as e:
            print "Error opening input file: {}".format(e)
            print __doc__
            sys.exit(-1)

        except ValueError as e:
            print "Error parsing the supplied input file: {}".format(e)
            sys.exit(-1)

        # And get going!!!
        app = Kronos(config)

        # if set s input, use a specific plugin
        if config.plugin:

            pass
            # model = plugins.factory(config.plugin['name'], config)
            #
            # if args.model:
            #     model.generate_model()
            # elif args.run:
            #     model.run()
            # elif args.postprocess:
            #     model.postprocess(postprocess_flag)
            # else:
            #     print "command line parsing error.."
            #     sys.exit(-1)

        else:
            app.run()

    except ConfigurationError as e:
        print "Error in Kronos configuration: {}".format(e)
