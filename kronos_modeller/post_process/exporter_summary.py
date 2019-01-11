# (C) Copyright 1996-2018 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os

from kronos.kronos_executor.io.definitions import sorted_kresults_stats_names, kresults_stats_info
from kronos.kronos_modeller.post_process.exportable_types import ExportableSignalGroup, \
    ExportableSignalFrame
from kronos.kronos_modeller.post_process.result_signals import ResultSignal

from kronos_modeller.post_process.exporter_base import ExporterBase


class ExporterSummaryRates(ExporterBase):

    export_type = "normalised_rates"

    def do_export(self, export_config, output_path, job_classes, **kwargs):

        print "Exporting summary rates data to {}".format(output_path)

        # ------------ find max and min rates for scaling all the plots accordingly ------------
        max_norm_value = None
        min_norm_value = None
        for class_name in job_classes.keys():

            for stat_name in sorted_kresults_stats_names:

                # take list of rates if the metric is defined for this class otherwise get "None" flag
                metric_rate_list = [self.sim_set.class_stats_sums[sim_name][class_name][stat_name]["rate"]
                                    if self.sim_set.class_stats_sums[sim_name][class_name].get(stat_name)
                                    else None for sim_name in self.sim_set.ordered_sims().keys()]

                # write the metrics only if actually present for this class..
                if all(metric_rate_list):
                    try:
                        normalized_rates = [v/float(metric_rate_list[0]) for v in metric_rate_list if metric_rate_list[0]]
                    except ZeroDivisionError:
                        print "zero value encountered for metric: {} of class: {}".format(stat_name, class_name)
                    max_norm_value = max(max_norm_value, max(normalized_rates)) if max_norm_value else max(normalized_rates)
                    min_norm_value = min(min_norm_value, min(normalized_rates)) if min_norm_value else min(normalized_rates)

        # --------------------------- assemble frame for export.. ---------------------------------
        group_signals = []
        for stat_name in sorted_kresults_stats_names:

            # take list of rates if the metric is defined for this class otherwise get "-1" flag
            metric_rate_list = [self.sim_set.class_stats_sums[sim_name]["all_classes"][stat_name]["rate"]
                                if self.sim_set.class_stats_sums[sim_name]["all_classes"].get(stat_name)
                                else None for sim_name in self.sim_set.ordered_sims().keys()]

            # write the metrics only if actually present for this class..
            if all(metric_rate_list):
                # NB: values are normalized against 1st simulation (of the ordered list of sims)
                _times = range(len(metric_rate_list))
                _values = [v / float(metric_rate_list[0]) for v in metric_rate_list]
                label = kresults_stats_info[stat_name]["label_sum"]
                group_signals.append(ResultSignal(label, _times, _values).get_exportable(metadata={"norm_factor": float(metric_rate_list[0])}))

        # if y_lims are not specified by the user, take the max/min encountered during the per-class plots
        # (if job-classes have been passes at all -> and therefore min_norm_value and max_norm_value are not None
        plot_ylim = self.export_config.get("y_lims")
        if not plot_ylim and min_norm_value and max_norm_value:
            ylims = [0.9 * min_norm_value, max_norm_value * 1.1]
        else:
            ylims = plot_ylim

        # Aggregate all the signals in a subplot
        all_groups = [ExportableSignalGroup(group_signals,
                                            name="Normalized Rates",
                                            ylims=ylims,
                                            y_label="Normalized Rates",
                                            x_ticks=range(len(self.sim_set.ordered_sims())),
                                            x_tick_labels=self.sim_set.ordered_sims().keys(),
                                            legend=True)]

        # Aggregate all the groups in the Exportable data structure
        output_file = os.path.join(output_path, self.export_type)+'_'+self.export_config["tag"]
        export_frame = ExportableSignalFrame(all_groups,
                                             title="Normalised rates",
                                             save_filename=output_file
                                             )

        export_frame.export(export_config["format"])
