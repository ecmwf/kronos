# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import sys
import argparse

from datetime import date, datetime
from kronos.core.jobs import ModelJob
from kronos.core.time_signal.time_signal import TimeSignal
from kronos.io.profile_format import ProfileFormat
from kronos.shared_tools.shared_utils import datetime2epochs


def collect_comm_ops_from_node_file(nodefile_lines):

    comm_type_map = {

        "BROADCAST IN INMARS IN WAM": "col",
        "BROADCAST IN GETSTRESS WAM": "col",
        "BROADCAST IOSTREAM INQUIRE": "col",
        "BROADCAST IOSTREAM SPEC_IN ": "col",
        "BROADCAST IOSTREAM GRID_IN": "col",
        "ALLGATHER IN GATHERGPF": "col",
        "GATHER CLOSE IOSTREAM": "col",
        "GATHER IOSTREAM_STATS": "col",
        "TRLTOM_COMMS": "col",
        "TRMTOL_COMMS": "col",

        "SEND/RECV IN READWIND WAM": "p2p",
        "SEND/RECV IN GETSPEC WAM": "p2p",
        "TRSTOM_COMMS": "p2p",
        "TRMTOS_COMMS": "p2p",
        # "TRGTOL_COMMS": "p2p", TODO: THIS NEEDS TO BE INCLUDED..
        "TRGTOL_COMMS (GPNORM)": "p2p",
        "TRLTOG_COMMS": "p2p"
    }

    task_block = {}
    for ll in range(len(nodefile_lines)):

        # COMMUNICATIONS STATISTICS:TASK= <task ID>
        if "COMMUNICATIONS STATISTICS:TASK=" in nodefile_lines[ll] and "UNKNOWN" not in nodefile_lines[ll]:

            task_id = int(nodefile_lines[ll].split("COMMUNICATIONS STATISTICS:TASK=")[1].strip())

            block_init_idx = ll+3
            current_line = nodefile_lines[block_init_idx]
            cc = 0
            while current_line != "\n":

                try:
                    current_line = nodefile_lines[block_init_idx + cc]

                    for k, v in comm_type_map.iteritems():
                        if k in current_line:
                            values = current_line.split(k)[1].split()
                            assert len(values) == 9

                            # append tha values #send, MB_send, #recv, MB_recv
                            task_block.setdefault(task_id, []).append({
                                                                        "type": v,
                                                                        "n_send":  float(values[1]),
                                                                        "mb_send": float(values[3]),
                                                                        "n_recv":  float(values[5]),
                                                                        "mb_recv": float(values[7])
                                                                      })
                    cc += 1

                except EOFError:
                    break

        # UNKNOWN COMMUNICATIONS STATISTICS:TASK= <task ID>
        if "UNKNOWN COMMUNICATIONS STATISTICS:TASK=" in nodefile_lines[ll]:

            task_id = int(nodefile_lines[ll].split("COMMUNICATIONS STATISTICS:TASK=")[1].strip())

            block_init_idx = ll + 3
            current_line = nodefile_lines[block_init_idx]
            cc = 0
            while current_line != "\n":

                try:
                    current_line = nodefile_lines[block_init_idx + cc]

                    for k, v in comm_type_map.iteritems():
                        if k in current_line:
                            values = current_line.split(k)[1].split()
                            assert len(values) == 4

                            # append tha values #send, MB_send, #recv, MB_recv
                            task_block.setdefault(task_id, []).append({
                                                                        "type": v,
                                                                        "n_send":  float(values[0]),
                                                                        "mb_send": float(values[1]),
                                                                        "n_recv":  float(values[2]),
                                                                        "mb_recv": float(values[3])
                                                                      })
                    cc += 1

                except EOFError:
                    break

    return task_block


def get_times(nodefile_lines):
    """
    Extract runtime from the NODE file
    :param nodefile_lines:
    :return:
    """

    starttime_line=None
    for ll, line in enumerate(nodefile_lines):
        if "***   Real world time    ***" in line:
            starttime_line = nodefile_lines[ll+1]
            break

    if not starttime_line:
        raise ValueError("TIME OF START not found in NODE file")

    runtime_line=None
    for line in nodefile_lines:
        if "TOTAL WALLCLOCK TIME" in line:
            runtime_line = line
            break

    if not runtime_line:
        raise ValueError("TOTAL WALLCLOCK TIME not found in NODE file")

    starttime = datetime.strptime( starttime_line.strip(), "Date : %Y-%m-%d Time : %H:%M:%S" )
    runtime = float(runtime_line.split("TOTAL WALLCLOCK TIME")[1].strip().split()[0])

    return starttime, runtime


def get_commandline_arguments(logfile_lines):
    """
    Return a dictionary with the command line arguments of the RAPS run
    :param logfile_lines:
    :return:
    """

    raps_args = None
    for line in logfile_lines:

        if "# Job attributes:" in line:
            args_str = line.split("# Job attributes:")[1].split()
            raps_args = {arg.split("=")[0]: arg.split("=")[1] for arg in args_str}

    if not raps_args:
        print "command arguments not found in ifs.log! I stop here.."
        sys.exit(1)

    return raps_args


if __name__ == "__main__":

    # Parser for the required arguments
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("raps_output_path", type=str, help="Path of the RAPS output")
    parser.add_argument("-o", "--output", type=str, help="Name of the kprofile [output.kprofile]", default="output.kprofile")

    # print the help if no arguments are passed
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # Parse the arguments..
    args = parser.parse_args()

    node_file = os.path.join(args.raps_output_path, "NODE.001_01")
    if not os.path.exists(node_file):
        print "ERROR: file {} does not exist! check RAPS run..".format(node_file)
        sys.exit(1)

    # open ifs.log to check command line arguments (for n-procs and n-nodes)
    logfile_name = os.path.join(args.raps_output_path, "ifs.log")
    with open(logfile_name) as lf:
        lf_lines = lf.readlines()

    raps_args = get_commandline_arguments(lf_lines)

    # open file NODE.001_01
    with open(node_file) as f:
        _nodefile_lines = f.readlines()

    # analyse the COMM info
    mpi_ops = collect_comm_ops_from_node_file(_nodefile_lines)

    # get RAPS runtime
    raps_start_time, raps_runtime = get_times(_nodefile_lines)

    # form the kronos timesignals
    kb_col = sum([(e["mb_send"]+e["mb_recv"]) / 2. for itask in mpi_ops.values() for e in itask if e["type"] == "col"]) * 1024.
    n_col = sum([(e["n_send"]+e["n_recv"]) / 2. for itask in mpi_ops.values() for e in itask if e["type"] == "col"])
    kb_p2p = sum([(e["mb_send"]+e["mb_recv"]) / 2. for itask in mpi_ops.values() for e in itask if e["type"] == "p2p"]) * 1024.
    n_p2p = sum([(e["n_send"]+e["n_recv"]) / 2. for itask in mpi_ops.values() for e in itask if e["type"] == "p2p"])

    n_bins=10
    d_bin = raps_runtime/float(n_bins)
    run_times = [d_bin * v for v in range(n_bins + 1)]
    timesignals = {
        "kb_collective": TimeSignal("kb_collective").from_values("kb_collective",
                                                                 run_times,
                                                                 [kb_col / float(len(run_times))] * len(run_times),
                                                                 priority=1
                                                                 ),

        "n_collective": TimeSignal("n_collective").from_values("n_collective",
                                                               run_times,
                                                               [n_col / float(len(run_times))] * len(run_times),
                                                               priority=1),

        "kb_pairwise": TimeSignal("kb_pairwise").from_values("kb_pairwise",
                                                             run_times,
                                                             [kb_p2p / float(len(run_times))] * len(run_times),
                                                             priority=1),

        "n_pairwise": TimeSignal("n_pairwise").from_values("n_pairwise",
                                                           run_times,
                                                           [n_p2p / float(len(run_times))] * len(run_times),
                                                           priority=1),
    }

    job = ModelJob(
        time_start=datetime2epochs(raps_start_time),
        duration=raps_runtime,
        ncpus=int(raps_args["mpi"]),
        nnodes=int(raps_args["nodes"]),
        timesignals=timesignals,
        label="app_id-{}".format(0)
    )

    _kpf = ProfileFormat(model_jobs=[job], created=date.today(), uid=None, workload_tag="raps_workload")
    _kpf.write_filename(args.output)
