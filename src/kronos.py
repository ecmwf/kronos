import os
import sys
import argparse

import runner
from exceptions_iows import ConfigurationError
from kpf_handler import KPFFileHandler
from config.config import Config
from model import KronosModel
from postprocess.run_plotter import RunPlotter


class Kronos(object):
    """
    Kronos main class
    """

    def __init__(self, config):

        self.config = config
        self.workloads = None
        self.workload_model = None
        self.model_jobs = None
        self.synthetic_apps = None

        if self.config.verbose:
            print "VERBOSE logging enabled"

    def model(self):
        """
        Depending on the supplied config, ingest data of various types.
        """
        print "\nBegining data ingestion...\n----------------------------------"
        self.workloads = KPFFileHandler().load_kpf(os.path.join(self.config.dir_input, self.config.kpf_file))
        print "\nIngested workloads: [\n" + ",\n".join(["    {}".format(d.tag) for d in self.workloads]) + "\n]"

        print "\nGenerating model workload...\n----------------------------------"
        self.workload_model = KronosModel(self.workloads, self.config)
        self.workload_model.generate_model()
        self.workload_model.export_synthetic_workload()

        # run_data = RunData(self.config.post_process['dir_sa_run_output'])
        # run_data.print_schedule_summary()

        # if self.config.verbose:
        #     print self.synthetic_apps.verbose_description()

    def export(self):
        print "\nOutputting synthetic app input...\n----------------------------------"
        self.workload_model.export_synthetic_workload()

    def run(self):
        """
        Main execution routine (default if no specific plugin is requested)
        """
        print "\nRun model...\n----------------------------------"
        kronos_runner = runner.factory(self.config.runner['type'], self.config)
        kronos_runner.run()

    def postprocess(self):
        """
        Postprocess the results of the run
        :return:
        """
        print "\nPostprocess run..\n----------------------------------"
        plotter = RunPlotter(self.config.post_process['dir_sa_run_output'])
        plotter.plot_run()


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

        if args.model:
            app.model()
        elif args.run:
            app.run()
        elif args.postprocess:
            app.postprocess()
        else:
            print "command line parsing error.."
            sys.exit(-1)

    except ConfigurationError as e:
        print "Error in Kronos configuration: {}".format(e)
