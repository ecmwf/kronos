import logging

from kronos_modeller.kronos_exceptions import ConfigurationError
from kronos_modeller.tools.shared_utils import progress_percentage
from kronos_modeller.workload import Workload
from difflib import SequenceMatcher

from kronos_modeller.workload_filling.filling_strategy import FillingStrategy

logger = logging.getLogger(__name__)


class StrategyMatchKeyword(FillingStrategy):

    """
    Apply job metrics from a name matching job
    """

    required_config_fields = FillingStrategy.required_config_fields + \
        [
            'keywords',
            'similarity_threshold',
            'source_workloads',
            'apply_to'
        ]

    def _apply(self, config):

        # Apply each source workload into each destination workload
        n_job_matched = 0
        n_destination_jobs = 0

        for wl_source_tag in config['source_workloads']:

            try:
                wl_source = next(wl for wl in self.workloads if wl.tag == wl_source_tag)
            except StopIteration:
                raise ConfigurationError("Source Workload {} not found".format(wl_source_tag))

            for wl_dest_tag in config['apply_to']:

                try:
                    wl_dest = next(wl for wl in self.workloads if wl.tag == wl_dest_tag)
                except StopIteration:
                    raise ConfigurationError("Destination Workload {} not found".format(wl_dest_tag))

                n_destination_jobs += len(wl_dest.jobs)

                n_job_matched += self.apply_lookup_table(wl_source,
                                                         wl_dest,
                                                         config['similarity_threshold'],
                                                         config['priority'],
                                                         config['keywords'],
                                                        )

        logger.info("jobs matched/destination jobs = [{}/{}]".format(n_job_matched, n_destination_jobs))

    def apply_lookup_table(self, look_up_wl, wl_dest,
                           threshold, priority, match_keywords):

        """
        Uses another workload as lookup table to fill missing job information
        :param look_up_wl:
        :param wl_dest:
        :param threshold:
        :param priority:
        :param match_keywords:
        :return:
        """

        logger.info('Applying look up from workload: {} onto workload: {}'.format(look_up_wl.tag, wl_dest.tag))

        assert isinstance(look_up_wl, Workload)
        assert isinstance(threshold, float)
        assert isinstance(priority, int)
        assert isinstance(match_keywords, list)

        n_jobs_replaced = 0

        # apply matching logic (if threshold < 1.0 - so not an exact matching is sought)
        n_print = 10
        if threshold < 1.0:
            for jj, job in enumerate(wl_dest.jobs):

                pc_scanned = progress_percentage(jj, len(wl_dest.jobs), n_print)
                if pc_scanned > 0:
                    print "Scanned {}% of source jobs".format(pc_scanned)

                for lu_job in look_up_wl.jobs:

                    # in case of multiple keys considers tha average matching ratio
                    current_match = 0
                    for kw in match_keywords:
                        if getattr(job, kw) and getattr(lu_job, kw):
                            current_match += SequenceMatcher(lambda x: x in "-_",
                                                             str(getattr(job, kw)),
                                                             str(getattr(lu_job, kw))
                                                             ).ratio()
                    current_match /= float(len(match_keywords))
                    # ---------------------------------------------------------------

                    if current_match >= threshold:
                        n_jobs_replaced += 1
                        for tsk in job.timesignals.keys():
                            if job.timesignals[tsk].priority <= priority and lu_job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]

        # compare directly (much faster..)
        elif threshold == 1:
            for jj, job in enumerate(wl_dest.jobs):

                pc_scanned = progress_percentage(jj, len(wl_dest.jobs), n_print)
                if pc_scanned > 0:
                    print "Scanned {}% of source jobs".format(pc_scanned)

                for lu_job in look_up_wl.jobs:

                    if all(getattr(job, kw) == getattr(lu_job, kw) for kw in match_keywords):
                        n_jobs_replaced += 1
                        for tsk in job.timesignals.keys():

                            if not job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]
                            elif job.timesignals[tsk].priority <= priority and lu_job.timesignals[tsk]:
                                job.timesignals[tsk] = lu_job.timesignals[tsk]
                            else:
                                pass
        else:
            raise ConfigurationError("matching threshold should be in [0,1], provided {} instead".format(threshold))

        return n_jobs_replaced


