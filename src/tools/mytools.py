
from pylab import *


#================================================================
def mb(bytes):
    return bytes / 1024 / 1024

#================================================================
#--sort items according to multiple keys..


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

#================================================================
# safe check for empty file..


def isfilenotempty(fname):
    if os.path.isfile(fname):
        if os.path.getsize(fname):
            return 1
        else:
            return 0
    else:
        return 0

#================================================================
# from frequency to time domain


def freq_to_time(time, freqs, ampls, phases):
    time_signal = zeros(len(time))
    for iF in arange(0, len(freqs)):
        time_signal = time_signal + \
            ampls[iF] * sin(2 * pi * freqs[iF] * time + phases[iF])
    return time_signal


#================================================================
# from frequency to time domain
def squarest_pair(num_in):

    N = int(floor(sqrt(num_in)))
    M = int(num_in / N)

    while (num_in % N) != 0:
        N -= 1
        M = int(num_in / N)
    return (N, M)
