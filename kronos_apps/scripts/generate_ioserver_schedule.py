#!/usr/bin/env python

"""

Generate a schedule for the kronos ioserver workflow

Example of usage:

 - Generate a schedule of 10 jobs, each with 10 IO-tasks, each writing
 files and each distributing the files periodically onto the io-servers

./generete_ioserver_schedule.py \
--njobs=10 \
--ntasks-per-jobs=10 \
--workflow="allwrite" \
--file-to-server="rotating"


"""

import sys
import copy
import json
import argparse
import datetime


# //////////////////// HIGH-LEVEL TEMPLATE /////////////////////////////
import os

param_names = [
    "kb_collective",
    "n_collective",
    "kb_write",
    "n_pairwise",
    "n_write",
    "n_read",
    "kb_read",
    "flops",
    "kb_pairwise"
]

schedule_template = {
    "unscaled_metrics_sums": {p: 1.0 for p in param_names},
    "uid": 4426,
    "jobs": [],
    "created": "",  # e.g "2018-07-09T21:30:53Z"
    "tag": "KRONOS-KSCHEDULE-MAGIC",
    "version": 3,
    "scaling_factors": {p: 1.0 for p in param_names}
}

job_template = {
    "depends": [],
    "config_params": {},
    "timed": True,
    "metadata": {
        "job_name": "",
        "workload_name": ""
    },
    "job_class": "template_ioserver"
}
# /////////////////////////////////////////////////////////////////////


# /////////////////////////// IO-TASK templates ///////////////////////////

iotask_write_template = {
    "file": "",
    "host": -1,
    "mode": "",
    "n_bytes": -1,
    "n_writes": -1,
    "name": "writer",
    "offset": -1
}

iotask_read_template = {
    "file": "",
    "host": -1,
    "n_bytes": -1,
    "n_reads": -1,
    "name": "reader",
    "offset": -1
}

# TODO tempaltes for the NVRAM iotasks..

# /////////////////////////////////////////////////////////////////////


# /////////////////////////////////////////////////////////////////////
def generate_timestep_workflow(args):
    """
    Generate a workload that models a multiple
    "time-stepping" writers and multiple readers
    :param args:
    :return: schedule
    """

    return {}


def generate_allwrite_workflow(args):
    """
    All writing jobs
    :param args:
    :return: schedule_templ:
    """

    # -------- start creating the schedule ------------
    creation_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    schedule_templ = copy.deepcopy(schedule_template)
    schedule_templ["created"] = creation_time

    io_block_size = args.io_block_size
    n_io_blocks = args.n_io_blocks
    io_dir_root = args.io_root_path
    nhosts = args.nhosts

    file_uid = 0
    for jobid in range(args.njobs):

        # print "jobid ", jobid

        # prepare job template
        job_templ = copy.deepcopy(job_template)
        job_templ["config_params"] = {"tasks": []}
        job_templ["metadata"]["job_name"] = "job-"+str(jobid)
        job_templ["metadata"]["workload_name"] = "workload-write"

        # loop over IO tasks
        for taskid in range(args.tasks_per_job):

            # print "taskid ", taskid

            # periodic host ID
            hid = taskid % nhosts

            # file name
            file_out_name = "kron_file_" + "host"+str(hid) + "_job"+str(jobid) + "_id"+str(file_uid)
            file_uid += 1

            # fill-in the job template
            task_templ = copy.deepcopy(iotask_write_template)
            task_templ["file"] = os.path.join(io_dir_root, file_out_name)
            task_templ["host"] = hid
            task_templ["mode"] = "append"
            task_templ["n_bytes"] = io_block_size
            task_templ["n_writes"] = n_io_blocks
            task_templ["name"] = "writer"
            task_templ["offset"] = 0

            # append this IO task into the job task list
            job_templ["config_params"]["tasks"].append(task_templ)

            # print "job_templ ", job_templ

        # now append the job into the schedule template
        schedule_templ.get("jobs",[]).append(job_templ)

    return schedule_templ


def generate_allread_workflow(args):
    """
    All reading jobs
    :param args:
    :return:
    """

    # -------- start creating the schedule ------------
    creation_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    schedule_templ = copy.deepcopy(schedule_template)
    schedule_templ["created"] = creation_time

    io_block_size = args.io_block_size
    n_io_blocks = args.n_io_blocks
    io_dir_root = args.io_root_path
    nhosts = args.nhosts

    file_uid = 0
    for jobid in range(args.njobs):

        # print "jobid ", jobid

        # prepare job template
        job_templ = copy.deepcopy(job_template)
        job_templ["config_params"] = {"tasks": []}
        job_templ["metadata"]["job_name"] = "job-"+str(jobid)
        job_templ["metadata"]["workload_name"] = "workload-read"

        # loop over IO tasks
        for taskid in range(args.tasks_per_job):

            # print "taskid ", taskid

            # periodic host ID
            hid = taskid % nhosts

            # file name
            file_out_name = "kron_file_" + "host"+str(hid) + "_job"+str(jobid) + "_id"+str(file_uid)
            file_uid += 1

            # fill-in the job template
            task_templ = copy.deepcopy(iotask_read_template)
            task_templ["file"] = os.path.join(io_dir_root, file_out_name)
            task_templ["host"] = hid
            task_templ["n_bytes"] = io_block_size
            task_templ["n_reads"] = n_io_blocks
            task_templ["name"] = "reader"
            task_templ["offset"] = 0

            # append this IO task into the job task list
            job_templ["config_params"]["tasks"].append(task_templ)

            # print "job_templ ", job_templ

        # now append the job into the schedule template
        schedule_templ.get("jobs",[]).append(job_templ)

    return schedule_templ    

# /////////////////////////////////////////////////////////////////////


if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("njobs", type=int, help="N of jobs in the workflow")
    parser.add_argument("nhosts", type=int, help="N of io-server hosts")

    parser.add_argument("--tasks-per-job", "-t", type=int,
                        help="N of io-tasks (per job)",
                        default=3)

    parser.add_argument("--io-block-size", "-b", type=int,
                        help="IO block size per IO-task",
                        default=102)

    parser.add_argument("--n-io-blocks", "-n", type=int,
                        help="#IO blocks per IO-task (reads or writes)",
                        default=100)

    parser.add_argument("--io-root-path", "-o", type=str,
                        help="Root path of the IO tasks",
                        default="/tmp")

    parser.add_argument("--workflow", "-w", type=str,
                        choices=["timestep", "allwrite", "allread"],
                        help="Type of workflow to be generated",
                        default="allwrite")

    parser.add_argument("--file-to-server", "-d", type=str,
                        help="Distribution file to server ID")

    # print the help if no arguments are passed
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # parse the arguments..
    args = parser.parse_args()

    # check the output directory
    if not os.path.isdir(args.io_root_path):
        print "Root path not valid!"
        exit(1)

    if args.workflow == "timestep":
        kschedule = generate_timestep_workflow(args)

    elif args.workflow == "allwrite":
        kschedule = generate_allwrite_workflow(args)

    elif args.workflow == "allread":
        kschedule = generate_allread_workflow(args)

    else:
        print "workflow option not recognised"
        exit(1)

    schedule_name = "ioserver_workflow_{}.kschedule".format(args.workflow)

    # writing file out
    with open(schedule_name, "w") as f:
        json.dump(kschedule, f, indent=2)

    print "\n * WORKFLOW SUMMARY * "
    print "N jobs:      {:10}".format( len(kschedule.get("jobs", [])) )
    print "__________________________"
    print "TOTAL N jobs:      {:10}".format(len(kschedule.get("jobs", [])))
    print "\nSchedule: {}".format(schedule_name)
