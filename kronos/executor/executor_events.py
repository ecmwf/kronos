#!/usr/bin/env python

import multiprocessing as mp
import Queue
from kronos.executor.event_dispatcher import dispatcher_callback
from kronos.executor.executor import Executor


class ExecutorDepsEvents(Executor):
    """
    An ExecutorDepsScheduler passes a schedule of jobs to the real scheduler to be executed.

    """

    def __init__(self, config, schedule, kschedule_file=None):
        """
        Initialisation. Passed a dictionary of configurations
        """

        super(ExecutorDepsEvents, self).__init__(config, schedule, kschedule_file=kschedule_file)

    def run(self):
        """
        Specific run function for this type of execution
        :return:
        """

        jobs = self.generate_job_internals()

        # loop over jobs and unlock jobs for which all dependencies are satisfied..
        event_queue = mp.Queue()
        events_handler_proc = mp.Process(target=dispatcher_callback,
                                         args=(event_queue, self.host, self.port))
        events_handler_proc.start()

        _simulation_events = []
        _submitted_jobs = []

        # ### pseudo code
        # while len(finished_jobs) < len(jobs):
        #
        #     event = queue.pop_events()
        #     if(event)
        #         dispatch(event) # reads the event type and update the status of jobs, data structures, etc
        #
        #     dispatch(time_event)
        #
        #     submit_elegible_jobs() # can be almost asynchronous by forking for submission

        while len(_simulation_events) < len(jobs):

            # update events from the dispatcher
            try:
                _simulation_events = event_queue.get_nowait()
            except Queue.Empty:
                pass

            # TODO: add time events..
            # _simulation_events.append(KronosEvent())

            # submit jobs (possibly with several workers..)
            for j in jobs:

                # print "job ", j.id

                # get finished jobs from event_queue
                finished_jobs = [e.job_num for e in _simulation_events if e.event == "complete"]

                if j.id not in finished_jobs:

                    # check if all its parent jobs have finished
                    if (j.depends == [] or all([p in finished_jobs for p in j.depends])) \
                            and (j.id not in _submitted_jobs):

                        # append this job to the list of "already submitted jobs"
                        _submitted_jobs.append(j.id)

                        # print "_submitted_jobs ", _submitted_jobs

                        # run the job now (with empty dependencies)..
                        j.run([])

            print "_simulation_events ", [e.job_num for e in _simulation_events]

        # Finally, terminate the event_dispatcher no more events expected..
        events_handler_proc.terminate()
