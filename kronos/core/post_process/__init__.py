# (C) Copyright 1996-2017 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
from kronos.core.post_process.exporters_tables import ExporterTable

from kronos.core.post_process.exporters_plots import ExporterPlot
from kronos.core.post_process.exporters_plots import ExporterTimeSeriesMetrics
from kronos.core.post_process.exporters_plots import ExporterTimeSeriesJNP
from kronos.core.post_process.exporters_plots import ExporterScatteredData


writer_map = {
    "rates_table": ExporterTable,
    "rates_plot": ExporterPlot,
    "time_series_metrics": ExporterTimeSeriesMetrics,
    "time_series_jobs_nodes_procs": ExporterTimeSeriesJNP,
    "scattered_plots": ExporterScatteredData,
}