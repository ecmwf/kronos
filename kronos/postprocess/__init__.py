from kronos.postprocess.exporters import ExporterTable
from kronos.postprocess.exporters import ExporterPlot
# from kronos.postprocess.exporters import ExporterTimeSeries

writer_map = {
    "table": ExporterTable,
    "plot": ExporterPlot,
    # "_series_tvr": ExporterTimeSeries,
}