# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import os
import numpy as np
import copy


# # commands needed by the job scheduler
# job_sched_commands = {
#                        'pbs':
#                            {
#                             'any_jobs_running': 'qstat -u',
#                             'submit': 'qsub'
#                            },
#                        'slurm':
#                            {
#                             'any_jobs_running': '/usr/local/apps/slurm/16.05.4/bin/squeue -u',
#                             'submit': 'sbatch'
#                            }
#                      }


def mb(bytes):
    """ Mb from bytes """
    return bytes / 1024 / 1024


def multikeysort(items, columns):
    """ Sort items according to multiple keys.. """
    from operator import itemgetter
    comparers = [((itemgetter(col[1:].strip()), -1) if col.startswith('-')
                  else (itemgetter(col.strip()), 1)) for col in columns]

    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, cmp=comparer)


def isfilenotempty(fname):
    """ Safe check for empty file.. """
    if os.path.isfile(fname):
        if os.path.getsize(fname):
            return 1
        else:
            return 0
    else:
        return 0


def freq_to_time(time, freqs, ampls, phases):
    """ from frequency to time domain """
    time_signal = np.zeros(len(time))
    for iF in np.arange(0, len(freqs)):
        time_signal = time_signal + \
                      ampls[iF] * np.sin(2 * np.pi * freqs[iF] * time + phases[iF])
    return time_signal


def squarest_pair(num_in):
    """ from frequency to time domain """

    N = int(np.floor(np.sqrt(num_in)))
    M = int(num_in / N)

    while (num_in % N) != 0:
        N -= 1
        M = int(num_in / N)
    return (N, M)


def simple_cull(input_points, dominates_points):
    """ pareto front """

    inputPoints_orig = copy.deepcopy(input_points)
    paretoPoints = set()
    paretoIdxes = []
    candidateRowNr = 0
    dominatedPoints = set()

    while len(input_points):
        candidateRow = input_points[candidateRowNr]
        input_points.remove(candidateRow)
        rowNr = 0
        nonDominated = True
        while len(input_points) != 0 and rowNr < len(input_points):
            row = input_points[rowNr]
            if dominates_points(candidateRow, row):
                input_points.remove(row)
                dominatedPoints.add(tuple(row))
            elif dominates_points(row, candidateRow):
                nonDominated = False
                dominatedPoints.add(tuple(candidateRow))
                rowNr += 1
            else:
                rowNr += 1

        if nonDominated:
            # add the non-dominated point to the Pareto frontier
            paretoPoints.add(tuple(candidateRow))

    # paretoIdxes = [inputPoints_orig.index(item)
    for (iPF, pf_pt) in enumerate(paretoPoints):
        for (iROW, orig_pt) in enumerate(inputPoints_orig):
            if np.linalg.norm(np.asarray(pf_pt)- np.asarray(orig_pt)) < 1e-10:
                paretoIdxes.append(iROW)

    return paretoPoints, paretoIdxes, dominatedPoints


def dominates(row, another_row):
    """ pareto front (helper function) """
    return sum([row[x] >= another_row[x] for x in range(len(row))]) == len(row) # maximization domination


def sort_dict_list(d, sorted_keys_list):
    return [d[i] for i in sorted_keys_list]
