
from kronos.executor.executor_schedule import ExecutorDepsScheduler
from kronos.executor.executor_events_par import ExecutorDepsEventsMultiProc

# type of executors from config..
executor_types = {
    "scheduler": ExecutorDepsScheduler,
    "events": ExecutorDepsEventsMultiProc
}

