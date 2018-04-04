#!/usr/bin/env python

from __future__ import (absolute_import, division,
                                print_function, unicode_literals)
from pprint import pprint
import json
import os.path # Check that the file exists
import argparse # Parsing of command line arguments
import datetime


def getJsonFName(args):
    # Replace the file extension given (assumed to be .map, but isn't necessarily) with
    # .json.
    # Find the index of the last . in the string
    origName = args.jsonfile

    dotInd = origName.rfind('.') 
    if dotInd < 0:
        dotInd = len(origName)
    
    assert (dotInd >= 0)
    
    # Check that a file does not already exist
    outFName = origName[:dotInd] + "_old.json"
    if os.path.isfile(outFName):
        # If a file does exist try and add a number in the filename
        # and re-check that the file doesn't exist
        count = 1
        outFName = origName[:dotInd] + "_old." + str(count) + ".json"
        while os.path.isfile(outFName):
            count = count + 1
            outFName = origName[:dotInd] + "_old." + str(count) + ".json"
     
    # Return the filename created
    return args.output if args.output else outFName


def convertMultiValue_new2old(new_data):
    return [new_data["min"],
            new_data["max"],
            new_data["mean"],
            new_data["var"]]


def convertSingleValuedMetrics(new, old):

    singleValues_new2old = {
            "number_of_processes": "targetProcs",
            "number_of_nodes": "nodes",
            "start_time": "timestamp",
            "command_line": "commandLine",
            "runtime": "runtime_ms"
            }

    multiValues_new2old = {
            "num_omp_threads_per_process" : "num_omp_threads_per_process",
            "wchar_total" : "wchar_total",
            "memory_per_node" : "memory_per_node",
            "num_cores_per_node" : "num_cores_per_node",
            "nvidia_gpus_count" : "nvidia_gpus_count",
            "rchar_total" : "rchar_total",
            "nvidia_total_memory" : "nvidia_total_memory"
            }

    pprint(new.keys())

    for key in singleValues_new2old:
        old["profile"][singleValues_new2old[key]] = new["info"][key]

    for key in multiValues_new2old:
        if new["info"]["metrics"][key]:
            old["profile"][multiValues_new2old[key]] = convertMultiValue_new2old(new["info"]["metrics"][key])


def convertSampleMetricData_new2old(new_data, old_data):

    new_metrics = new_data["samples"]["metrics"]
    old_metrics = old_data["profile"]["samples"]

    for key in new_metrics:
        sampleMins = new_metrics[key]["mins"]
        sampleMaxs = new_metrics[key]["maxs"]
        sampleMeans= new_metrics[key]["means"]
        sampleVars = new_metrics[key]["vars"]

        assert len(sampleMins) == len(sampleMaxs) == len(sampleMeans) == len(sampleVars)

        old_metrics[key] = zip(*(sampleMins, sampleMaxs, sampleMeans, sampleVars))


if __name__ == "__main__":

    # Create a parser for the option passed in
    parser = argparse.ArgumentParser(description="script that converts new format of Allinea json to old format")
    parser.add_argument("jsonfile", help="Old format JSON file to convert to new JSON format")
    parser.add_argument("--output", help="Name of the output json produced by the conversion tool")
    args = parser.parse_args()

    # Try and read in the data file. Let any error that occurs be raised
    old_data = dict()
    with open(args.jsonfile) as jsonFile:
        new_data = json.load(jsonFile)

    old_data["profile"] = {}
    convertSingleValuedMetrics(new_data, old_data)

    # Now get all of the sampled values and convert them
    old_data["profile"]["samples"] = dict()
    convertSampleMetricData_new2old(new_data, old_data)

    # Write the time that the samples were taken
    times_new = new_data["samples"]["window_start_offsets"]
    time_window_len = times_new[1]-times_new[0]
    old_data["profile"]["sample_times"] = [(t+time_window_len/2)*1000. for t in times_new]

    # add sample interval
    old_data["profile"]["sample_interval"] = time_window_len * 1000.0

    # correct the timestamp
    run_date = datetime.datetime.strptime(new_data["info"]["start_time"], '%Y-%m-%dT%H:%M:%S')
    old_data["profile"]["timestamp"] = run_date.strftime("%a %b %d %H:%M:%S %Y")

    # Get the name of the json file to write to
    jsonFName = getJsonFName(args)

    # Open the file for writing
    jsonFile = open(jsonFName, 'w')

    # Use the json module to pretty print the dictionary
    json.dump(old_data, jsonFile, indent=4, separators=(',', ':'))

    jsonFile.close()

    print("Data written to " + jsonFName)
