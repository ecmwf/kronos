#!/usr/bin/env python
# (C) Copyright 1996-2017 ECMWF.
# 
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0. 
# In applying this licence, ECMWF does not waive the privileges and immunities 
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.


import argparse # Parsing of command line arguments
import os.path # Check that the file exists
import sys # For exiting the program
from gzip import GzipFile # Decompressing of MAP file
import xml.etree.cElementTree as ET # For parsing of XML
import json # For pretty printing JSON
import numbers # For generic numeric type checking

__EtreeType__ = ET.ElementTree 
__EtreeElType__ = type(ET.Element(''))

class MAP2JSONError(Exception):
    pass

def convertValsToFloat(d):
    """ Sets the values in the dictionary as a float, if possible, otherwise
    leaves them as the type they are
    
        Args:
            d (dict): Dictionary for which to convert the values to floats

        Returns:
            No return value, as the dictionary is altred in place
    """
    assert isinstance(d, dict)
    
    for item in d:
        try:
            d[item] = float(d[item])
        except:
            pass

def map2XMLTree(filename):
    """Reads a compressed XML file and parses the data as XML

        Args:
            filename (str): The name of the MAP file to read in

        Returns:
            An element tree of the parsed XML on success. Raises an exception
            on error
    """
    assert isinstance(filename, str)
    
    if(not os.path.isfile(filename)):
        raise IOError("File " + filename + " does not exist")
        
    # Open the MAP file for reading and parse the XML
    try:
        xmlData = ET.parse(GzipFile(filename, "rb"))
    except IOError as inst:
        # Catch an IO error, and print out the filename
        # with it
        print(inst.__str__() + ": " + filename)
        raise
    except ET.ParseError:
        print("Unable to parse XML data from: " + filename)
        raise
    # We don't intercept any other exceptions, and allow
    # these to be raised by the user
    
    return xmlData
### End of function map2XMLTree

def findAndReadTagAttributes(dataRoot, tagName, attributeList, numExpected=0):
    """Reads attributes from the given tag in an XML element tree

    Reads the values of the attributes that are given in 'attributeList' in the
    tag 'tagName' and stores this in a list of dictionaries to return. If an 
    attribute is not found in the tag a warning message is printed to stdout

        Args:
            dataRoot (ET.Element): Element to read from
            tagName (str): Name of the tag to read attributes from
            attributeList (list): List of strings of name of attributes to read values of
            numExpected (int): Non-negative integer for the number of tags expected to be read.
                               The default value is zero, in which case this value is ignored.

        Returns:
            A list of dictionaries containing the values of the attributes that were found
    """
    assert isinstance(dataRoot, __EtreeElType__), \
        "The dataRoot parameter must be of type xml.etree.Element but is of type %s" % type(dataRoot)
    assert isinstance(tagName, str), "Tag name must be provided in a string format"
    assert isinstance(attributeList, list) or isinstance(attributeList, str)
    assert isinstance(numExpected, int)
    
    # If the tag name we are looking for is the tag of the current element we only consider
    # the current element
    if(dataRoot.tag == tagName):
        elements = [dataRoot]
    else:
        elements = dataRoot.iterfind(tagName)

    # If the attribute list is a single string re-cast it as a string
    if(isinstance(attributeList, str)):
        attributeList = [attributeList]
        
    # For each element read the attributes
    numTagsCounted = 0
    retVal = list()
    for element in elements:
        if(numExpected != 0 and numTagsCounted >= numExpected):
            print("Expected %d elements with name %s, but more were found" % (numExpected, tagName))
            print("Returning the %d elements read so far" % numExpected)
            break;
        retVal.append(dict((k, element.attrib[k]) for k in attributeList if k in element.attrib)) 
        numTagsCounted += 1
        
    # Print a warning if more tags were expected
    if(numExpected > numTagsCounted):
        print("Expected %d elements with name %s but only %d were found" % (numExpected, tagName, numTagsCounted))
        
    # Make sure that the tag name is found
    if(numTagsCounted == 0):
        raise MAP2JSONError("No elements with tag name " + tagName + " found in element tree")
    
    return retVal
### End of function findAndReadTagAttributes

def readGlobalMetricData(rootEl, metricNames):
    """ Reads the metric data from directly beneath the root node (i.e. the metrics that are first
    children of the <profile> tag
    
        Args:
            rootEl (xml.etree.ElementTree.Element): The root element. The tag name of this should be "profile"
            metricNames (list): List of strings of names of metrics

        Returns:
            A dictionary of elements containing the global metrics reported in a MAP file
    """
    # Make sure that the root element is of the correct type
    assert isinstance(rootEl, __EtreeElType__)
    assert isinstance(metricNames, list)
    # Make sure that the tag is correct, as otherwise we are not reading the global metric data
    if (rootEl.tag != "profile"):
        raise MAP2JSONError("Only the <profile> element should be passed to this method")
    
    globalMetrics = rootEl.iterfind("metric")
    
    # We assume that the attributes that are read can be interpreted as integers.
    # We read the min, max, mean and variance values
    tags = ["min", "max", "mean", "var"]
    metricVals = dict((metricName, tuple(float(el.attrib[tag]) for tag in tags)) for el in globalMetrics \
        for metricName in metricNames if metricName == el.attrib["name"])
    #metricVals = dict((metricName, dict((tag, float(el.attrib[tag])) for tag in tags)) for el in globalMetrics \
        #for metricName in metricNames if metricName == el.attrib["name"])
    
    # Make sure that we found some metrics
    if (len(metricVals) == 0):
        raise MAP2JSONError("No global metrics found with given names")
    
    return metricVals
### End of function readMetricData

def getJsonFName(mapFName):
    """ Returns a string containing the filename of the JSON file to write out to
    given the MAP file from which reading was performed
    
        Args:
            mapFName (str): The filename of the MAP file read in

        Returns:
            The name of the json filename to use
    """
    # Replace the file extension given (assumed to be .map, but isn't necessarily) with
    # .json.
    # Find the index of the last . in the string
    assert (isinstance(mapFName, str))
    assert (len(mapFName) > 0), "Non-empty filename must be provided"
    
    dotInd = mapFName.rfind('.') 
    if dotInd < 0:
        dotInd = len(mapFName)
    
    assert (dotInd >= 0)
    
    # Check that a file does not already exist
    outFName = mapFName[:dotInd] + ".json"
    if os.path.isfile(outFName):
        # If a file does exist try and add a number in the filename
        # and re-check that the file doesn't exist
        count = 1
        outFName = mapFName[:dotInd] + "." + str(count) + ".json"
        while os.path.isfile(outFName):
            count = count + 1
            outFName = mapFName[:dotInd] + "." + str(count) + ".json"
     
    # Return the filename created
    return outFName
### End of function getJsonFName
    
def addSampleTimesToDict(d, numSamples, duration):
    """ Adds the sample times to the dictionary passed in.
    It is assumed that the start time of the samples is zero
    
        Args:
            d (dict): The dictionary object to update
            numSamples (int): The number of samples taken
            duration (float): The duration that the samples span

        Returns:
            No return value. The value of the dictionary is altered in this
            method
    """
    assert isinstance(d, dict)
    assert isinstance(numSamples, int)
    assert isinstance(duration, numbers.Number)
    
    # Calculate the width of the samples
    sampleInterval = float(duration) / numSamples
    
    # Set the time at which the sample is taken to be in the middle of the
    # sample window
    sampleTimes = list((0.5 + i) * sampleInterval for i in range(numSamples))
    d["sample_interval"] = sampleInterval
    d["sample_times"] = sampleTimes
### End of function addSampleTimesToDict

def collectSampleInfo(xmlroot, d, numExpected):
    """ collects sample info from the samples in the xmlroot passed in
    
        args:
            xmlroot (xml.etree.elementtree.element): the root of the profiling data
            d (dict): a dictionary to populate with sample values
            numExpected (int): the number of expected samples

        returns:
            nothing. updates the dictionary d in this method
    """
    assert isinstance(xmlroot, __EtreeElType__), "xmlroot is of type " + str(type(xmlroot))
    assert isinstance(d, dict)
    assert isinstance(numExpected, int)
    
    # get the sample tag in the xml root
    sampleroot = xmlroot.find("allsamples")
    if (sampleroot is None):
        raise MAP2JSONError("No 'allsamples' tag found. Unable to interpret the MAP file")
    
    # count the number of samples found
    numfound = 0
    # Get the samples
    samples = sampleroot.iterfind("sample")
    # get the min, max, mean and variance of the samples
    attribs = ["min", "max", "mean", "var"]
    for sample in samples:
        numfound += 1
        metrics = sample.iterfind("metric")
        # Read the desired attributes from the metric
        metricVals = dict()
        for metric in metrics:
            if metric.attrib["num"] == "0":
                # There are no samples for this data. We want to record zero values in this case
                metricVals[metric.attrib["name"]] = tuple(0. for _ in range(len(attribs)))
            else:
                metricVals[metric.attrib["name"]] = tuple(float(metric.attrib[label]) for label in attribs if label in metric.attrib.keys())
        # set up the dictionary on the first pass
        if numfound == 1:
            # Loop over the dictionary here and copy the values into the dictionary
            # that is returned
            for metricVal in metricVals.items():
                d[metricVal[0]] = [metricVal[1]]
        else:
            # Append to the end of the dictionary
            for metricVal in metricVals.items():
                d[metricVal[0]].append(metricVal[1])
        
    if (numfound != numExpected):
        raise MAP2JSONError("Number of samples read does not match the expected number of samples")
### End of function collectSampleInfo

# We only want to execute something if this is run as a program, not when it is imported
# which occurs (for example) during the testing
if(__name__ == "__main__"):
    # Create a parser for the option passed in
    parser = argparse.ArgumentParser(description="Convert a MAP file to JSON format")
    # Add a positional argument which must be the MAP file to convert
    parser.add_argument("mapfile", help="MAP file to convert to JSON format")
    # Parse the arguments
    args = parser.parse_args()

    # Check that the map file that is passed in points to an existing file
    mapFName = args.mapfile
    mapData = map2XMLTree(mapFName)
    # If we return an error code, exit the script
    if (type(mapData) == int):
        sys.exit(mapData)
        
    # Get the root node of the XML tree
    rootNode = mapData.getroot()
    
    profileAttributes = findAndReadTagAttributes(rootNode, "profile", ["numSamples", "targetProcs", "nodes", "timestamp",
                                                                       "sampler_start_offset", "commandLine"])[0]
    assert isinstance(profileAttributes, dict)
    # Set numeric values in profileAttributes to be of type float instead of string
    convertValsToFloat(profileAttributes)
    numSamples = int(profileAttributes["numSamples"])
    
    # Maintain a list of metrics to read
    metricNames = ["num_omp_threads_per_process", "wchar_total", "num_physical_cores_per_node", "runtime_ms",
                   "memory_per_node", "num_cores_per_node", "nvidia_gpus_count", "rchar_total", "nvidia_total_memory"]
    
    # Read the global metric information from the parameters with names in metricNames above
    globalMetrics = readGlobalMetricData(rootNode, metricNames)
    
    # Set the run time of the profile
    tmax = globalMetrics["runtime_ms"][1]
    globalMetrics["runtime_ms"] = tmax
    profileAttributes.update(globalMetrics)
    
    # Create a dictionary to store the JSON
    jsonDict = { "profile" : profileAttributes }
    profileDict = jsonDict["profile"]
    
    # Set the sample times
    addSampleTimesToDict(profileDict, numSamples, tmax)
    
    # Add the sample information to the dictionary
    sampleDict = dict()
    collectSampleInfo(rootNode, sampleDict, numSamples)
    
    profileDict["samples"] = sampleDict
    
    # Get the name of the json file to write to
    jsonFName = getJsonFName(mapFName)
    
    # Open the file for writing
    jsonFile = open(jsonFName, 'w')
    
    # No need to check that the file is open, as an exception is raised
    # if it isn't
    
    # Use the json module to pretty print the dictionary 
    json.dump(jsonDict, jsonFile, indent=4, separators=(',', ':'))
    
    # Close the file
    jsonFile.close()
### End of main function 
