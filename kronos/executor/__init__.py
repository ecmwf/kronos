from kronos.executor.executor_events import ExecutorDepsEvents
from kronos.executor.executor_schedule import ExecutorDepsScheduler

# type of executors from config..
executor_types = {
    "scheduler": ExecutorDepsScheduler,
    "events": ExecutorDepsEvents
}

