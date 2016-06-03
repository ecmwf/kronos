
import os
# from numpy import zeros, arange, sin, floor, pi, sqrt, linalg, asarray
import numpy as np
import copy


def mb(bytes):
    return bytes / 1024 / 1024

# sort items according to multiple keys..
def multikeysort(items, columns):
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


# safe check for empty file..
def isfilenotempty(fname):
    if os.path.isfile(fname):
        if os.path.getsize(fname):
            return 1
        else:
            return 0
    else:
        return 0


# from frequency to time domain
def freq_to_time(time, freqs, ampls, phases):
    time_signal = np.zeros(len(time))
    for iF in np.arange(0, len(freqs)):
        time_signal = time_signal + \
                      ampls[iF] * np.sin(2 * np.pi * freqs[iF] * time + phases[iF])
    return time_signal


# from frequency to time domain
def squarest_pair(num_in):

    N = int(np.floor(np.sqrt(num_in)))
    M = int(num_in / N)

    while (num_in % N) != 0:
        N -= 1
        M = int(num_in / N)
    return (N, M)

# pareto front
def simple_cull(inputPoints, dominates):

    inputPoints_orig = copy.deepcopy(inputPoints)
    paretoPoints = set()
    paretoIdxes = []
    candidateRowNr = 0
    dominatedPoints = set()

    while len(inputPoints):
        candidateRow = inputPoints[candidateRowNr]
        inputPoints.remove(candidateRow)
        rowNr = 0
        nonDominated = True
        while len(inputPoints) != 0 and rowNr < len(inputPoints):
            row = inputPoints[rowNr]
            if dominates(candidateRow, row):
                inputPoints.remove(row)
                dominatedPoints.add(tuple(row))
            elif dominates(row, candidateRow):
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


# pareto front (helper function)
def dominates(row, anotherRow):
    return sum([row[x] >= anotherRow[x] for x in range(len(row))]) == len(row) # maximization domination
