# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import csv
import os

from kronos.core.post_process.exporter_base import ExporterBase

from kronos.core.post_process.krf_data import sorted_krf_stats_names
from kronos.core.post_process.krf_data import krf_stats_info

from kronos.core.post_process.definitions import fig_name_from_class


class ExporterTable(ExporterBase):

    class_export_type = "rates_table"
    default_export_format = "csv"
    optional_configs = []

    def export_function_map(self, key):

        function_map = {
            "csv": self._export_csv,
        }

        return function_map.get(key, None)

    def _export_csv(self, output_path, job_classes, **kwargs):
        """
        export data into csv format
        :return:
        """

        print "Exporting CSV tables in {}".format(output_path)

        # export run-time tables
        with open(os.path.join(output_path, 'run_times.csv'), 'wb') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for sim_name, sim in self.sim_set.ordered_sims().iteritems():
                csvwriter.writerow([sim_name, sim.runtime()])

        # ------- write a stat file for each job class --------
        for class_name in job_classes.keys():

            csv_name = os.path.join(output_path, 'rates_class_{}.csv'.format(fig_name_from_class(class_name)))
            csvfile = open(csv_name, 'wb')
            csvwriter = csv.writer(csvfile, delimiter=',')
            csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

            # Loop over the sorted metrics
            for stat_name in sorted_krf_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "-1" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name].get(stat_name)
                                    else None for sim_name in self.sim_set.ordered_sims().keys()]

                # write the metrics only if actually present for this class..
                if all(metric_rate_list):
                    csvwriter.writerow([krf_stats_info[stat_name]["label_rate"]] + ["{:.3f}".format(v) for v in metric_rate_list])

            csvfile.close()

        # ------- write a stat file for all-class statistics --------
        csvfile = open(os.path.join(output_path, 'rates_class_all.csv'), 'wb')
        csvwriter = csv.writer(csvfile, delimiter=',')
        csvwriter.writerow(["metric"] + self.sim_set.ordered_sims().keys())

        # Print the metrics
        for stat_name in sorted_krf_stats_names:

            # take list of rates if the metric is defined for this class otherwise get "-1" flag
            metric_rate_list = [self.sim_set.class_stats_sums[sim_name]["all_classes"][stat_name]["rate"]
                                if self.sim_set.class_stats_sums[sim_name]["all_classes"].get(stat_name)
                                else None for sim_name in self.sim_set.ordered_sims().keys()]

            if all(metric_rate_list):
                csvwriter.writerow([krf_stats_info[stat_name]["label_rate"]] + ["{:.3f}".format(v) for v in metric_rate_list])
        csvfile.close()