import logging

from kronos_modeller.workload_editing.workload_split import WorkloadSplit

logger = logging.getLogger(__name__)


workload_editing_types = {
    "split": WorkloadSplit
}
