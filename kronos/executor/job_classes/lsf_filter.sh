#!/bin/bash
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
