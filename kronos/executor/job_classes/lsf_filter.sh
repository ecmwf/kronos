#!/bin/bash

# Example of "filter" file to parse the output of the LSF scheduler upon
# job submission and extract the job ID only.
#
# NOTE: This filter is only needed when the job dependencies are delegated
# to the scheduler (it is NOT needed when Kronos events mechanism is used).

file=${@: -1}
if [ $# -gt 1 ] ; then
    cond=$2
    str=
    andstr=
    for i in $(echo $2|sed "s/:/ /g") ; do
      str=${str}$andstr"ended($i)"
      andstr=" && "
    done
    if [ "x$str" != "x" ] ; then
       echo "#BSUB -w \"$str\"" >> $file
    fi
fi
jid=$(bsub < $file | awk '{print $2}' |sed "s/<//"|sed "s/>//")
echo $jid
