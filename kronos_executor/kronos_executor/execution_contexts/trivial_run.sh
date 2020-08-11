#!/bin/bash

outfile=$1
errfile=$2
shift 2

$@ >$outfile 2>$errfile &
echo $!
