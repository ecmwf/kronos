
from __future__ import print_function

import copy
import json
import strict_rfc3339


def add_creation_timestamp(schedule):
    schedule["created"] = strict_rfc3339.now_to_rfc3339_utcoffset()


def add_magic(schedule):
    schedule.update({
        "tag": "KRONOS-KSCHEDULE-MAGIC",
        "version": 3,
        })


def save_schedule(path, schedule):
    if "tag" not in schedule or "version" not in schedule:
        add_magic(schedule)

    if "created" not in schedule:
        add_creation_timestamp(schedule)

    with open(path, "w") as f:
        json.dump(schedule, f, indent=2)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Generate an example KSchedule with a stepping producer and one consumer per step")
    parser.add_argument('steps', type=int, help="Number of steps")
    parser.add_argument('-o', '--output', default=None, help="Output file")
    parser.add_argument('-s', '--size', type=int, default=1024, help="Size of the step data (in KiB)")

    args = parser.parse_args()
    if args.output is None:
        args.output = "stepper_{}.kschedule".format(args.steps)


    schedule = {"jobs": []}

    # Stepper job (external)
    schedule["jobs"].append({
            "job_class": "stepper_job",
            "metadata": {
                "job_name": "stepper_0",
                "workload_name": "stepper",
            },
            "config_params": {
                "nsteps": args.steps,
                "size_kb": args.size,
            },
            "depends": [],
            "timed": True,
        })

    # Consumer jobs (synthetic app)
    for step in range(args.steps):
        step_deps = []
        step_deps.append({
            "info": {
                "app": "stepper",
                "job": 0
                },
            "type": "NotifyMetadata",
            "metadata": {
                "step": step + 1
                }
            })

        step_frames = []
        step_frames.append([{
            "name": "file-read",
            "kb_read": args.size,
            "mmap": False,
            "n_read": 1,
            "files": ["step_{}".format(step + 1)],
            }])

        schedule["jobs"].append({
                "metadata": {
                    "job_name": "consumer_{}".format(step),
                    "workload_name": "consumer",
                },
                "frames": step_frames,
                "num_procs": 1,
                "depends": step_deps,
                "timed": True,
            })

    save_schedule(args.output, schedule)
    print("Schedule saved at {}".format(args.output))
