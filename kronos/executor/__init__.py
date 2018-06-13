from kronos.executor.executor_events import ExecutorDepsEvents
from kronos.executor.executor_schedule import ExecutorDepsScheduler
from kronos.executor.executor_events_multiproc import ExecutorDepsEventsMultiProc

# type of executors from config..
executor_types = {
    "scheduler": ExecutorDepsScheduler,
    "events": ExecutorDepsEvents,
    "events_multiproc": ExecutorDepsEventsMultiProc,
}

