#! /usr/bin/env python

import networkx as nx

import os
import glob
import sys

#import numpy as np

import pkg_resources
import csv
import re
import optparse

import pprint as pp

from geocoder import Geocoder

#toDO; expand regex to capture LinkLabel "OC-192c"

#TODO: Move options and args parsing out of body of code, and make them pass
#params to main body of code which is a function - so can call programatically
#not just on command link And then can unit test the graphk
#TODO:  Also allow note on map like legend: rather than needig full csv
#TODO: could also store the SVN revision of the class.csv file

#TODO: add option to mark outliers

#TODO: write up a  test network containing differnt nodes and external nodes
# and hyperedges and edges labelled to check the output after the merger
# and converter (use this in documentation to explain the steps)

#TODO add comment to graph metadata if nodes skipped - similar to filename asdmn
# renaming base on args

#tODO: handle 2011-01-31 13:45:06,402 DEBUG AttributeError 'NoneType'
# object has no attribute 'encode'

#TODO: make merge by default only do classified nodes

#TODO: make consistent filename eg garr and kentman with dates

#ToDO: also allow country to be set in {{}} format
# and handle appropriately in cleanup

#TODO check case of nodes being left yellow and external nodes also being yellow
#being incorrectly tagged with the yellow internal node tag

#TODO: kill thread if get max exceeded geonames error
#TODO: auto warn about disconnected nodes
#TODO: add option to geocode external networks, only if {{}} included,
#otherwise skip if triangle
#TODO: Finish cleaning up merge script to remove sql
#TODO: allow option to skip cache - when manually add entries...
#and then append to cache/update?
#TODO: look at licence/credit for geonames
#don't geocode without it
#TODO: fix up iteration through places for region... eg each state seperately
#TODO: check tgat 2.5G and 500M get converted to edge data
#TODO: work out why #123456 fails with encode error first time
#TODO: make + behave with >


"""
Documentation:
make note in doc to not use capitals for names - as capitalized
and less than 5 characters are skipped
make note that merge and plot use cache, based on file modified,
so if rename files then won't be picked up... solution is to manually
delete cache file
write about how can use the map view of geonames to find places that match
Do examples of using zoo data in yed, gephi, matlab, matlab bgl, r library,
and networkx
tip : can often use state names to disambiguate - find well known place,
and then states from that
"""



import logging
import logging.handlers



#TODO fix bug wher eneed to append / to dir name for convert and merge
#TODO be careful not to apply colour legend classification to stop sign nodes
# which are used to represent hyperedges - but is ok for stop sign nodes
# with labels, which may just have doubted labels
#TODO catch errors in converter script, print, continue on to next network


opt = optparse.OptionParser()
opt.add_option('--file', '-f',
            help="Load data from FILE")

opt.add_option('--directory', '-d',
            help="process directory")


opt.add_option('--output_dir', '-o',
            help="process directory")

opt.add_option('--skip_cache',  action="store_true",
                default=False,
                help="Don't use cache for geocoding")

opt.add_option('--debug',  action="store_true",
                default=False,
                help="Print debugging information to terminal")

opt.add_option('--geocode', '-u', 
                default=False,
                help="Username for Geonames web service")

options = opt.parse_args()[0]

#TODO: work out best way to put in function but make globally accessible
logger = logging.getLogger("yed2zoo")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)-6s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
if options.debug:
    ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logging.getLogger('').addHandler(ch)
log_dir = "."
LOG_FILENAME =  os.path.join(log_dir, "yed2zoo.log")
LOG_SIZE = 2097152 # 2 MB
fh = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_SIZE, backupCount=5)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh.setFormatter(formatter)
logging.getLogger('').addHandler(fh)

def convert_to_undirected(graph):
    if graph.is_multigraph():
        """
        When NetworkX converts from MultiDiGraph to MultiGraph, it will discard
        an edge if it exists in both directions, eg (1, 2) and (2, 1) will
        discard (2, 1). For maps traced in yED, the existence of a link means it
        was deliberately added, and we wish to keep it. In above example we wish
        to have 2 edges between nodes 1 and 2. To do this, we need to manually
        add another link from (1, 2) and remove (2, 1)"""
        for src, dst, key, data in graph.edges(data=True, keys=True):
            # See if reverse link
            if graph.has_edge(dst, src):
                # Reverse link exists, want all to go same direction
                # All edges for node pair need to originate from same node,
                # choose lower node id to decide
                if src > dst:
                    # Switch the link
                    graph.add_edge(dst, src, attr_dict=data)
                    graph.remove_edge(src, dst, key)

    # Undirected version of graph (both single and multi edge)
    graph = graph.to_undirected()

    return nx.MultiGraph(graph)


def extract_speed(data, label):
    speed_multipliers = {
        'K': 1e3,
        'M': 1e6,
        'G': 1e9,
    }
    num_units = "(\d+\.*\d*)\s*(k|K|m|M|g|G|t|T)(b?p?/?s?)"
    speed_invalidators = "[>|<|<=|>=|=>|=<|*|x|-]"
    if extract_speed:
        if re.search( speed_invalidators+"\s*"+num_units, label, re.IGNORECASE):
            # eg >45Mbps, <=1gbps, 2x>2.5G, etc
            # Ambiguous as to actual speed -> skip
            pass
        elif len(re.findall(num_units, label, re.IGNORECASE)) > 1:
            # Speed should only occur once
            pass
        else:
            # Try and extract units if present
            re_m = re.search(num_units, label, re.IGNORECASE)
            if re_m:
                speed = re_m.group(1)
                units = re_m.group(2)
                # Get the bps bp/s bit as well to remove from note
                units_extra = re_m.group(3)
                data['LinkNote'] = data['LinkNote'].replace(speed, "")
                data['LinkNote'] = data['LinkNote'].replace(units, "")
                data['LinkNote'] = data['LinkNote'].replace(units_extra, "")
                data['LinkSpeed'] = speed
                units = units.upper()
                data['LinkSpeedUnits'] = units
                data['LinkSpeedRaw'] = speed_multipliers[units] * float(speed)

    return data

def extract_type(data, label):
    edge_types = "OC-\d+|T\d|E\d|DS-\d+|STM-\d+|SDH|Fib[e|r]{2}|Optical|GIGE|FE"
    edge_types += "|Ethernet|Satellite|Wireless|Microwave|Serial|ATM|MPLS"

    if len(re.findall(edge_types, label, re.IGNORECASE)) > 1:
        # Should only occur once -> ignore
        # see if occurs as type (over|via) type,
        # eg Ethernet over SDH, Ethernet over Satellite, etc
        over_via =  r"({0})\s(over|via)\s({0})".format(edge_types)
        re_m = re.search(over_via, label, re.IGNORECASE)
        if re_m:
            temp = re_m.group(0)
            data['LinkNote'] = data['LinkNote'].replace(temp, "")
            data['LinkType'] = temp
        else:
            # skip this entry as invalid
            pass
    else:
        re_m = re.search(edge_types, label, re.IGNORECASE)
        if re_m:
            temp = re_m.group(0)
            data['LinkNote'] = data['LinkNote'].replace(temp, "")
            # pretty up the type
            # eg ethernet -> Ethernet, ETHERNET -> Ethernet, STM -> STM ie same
            if not temp.isupper():
                # Not upper case, tidy up
                # Ignore all upper case, eg avoid ATM -> Atm, SDH -> Sdh
                temp = temp.title()
            data['LinkType'] = temp
    return data

def extract_temporal(data, label):
    temporal = "Future|Planned|Current|Proposed"
    temporal += "|Planning|Under Construction|Under Development"
    re_m = re.search(temporal, label, re.IGNORECASE)
    if re_m:
        temp = re_m.group(0)
        data['LinkNote'] = data['LinkNote'].replace(temp, "")
        data['LinkStatus'] = temp.title()
    return data

def extract_edge_data(label):

    #TODO: also do the raw speed for edges (optional command line  argument
    # eg 10M is 10 * 10^6 = 10,000,000 in the GML

    # The "Note" starts off as label and is trimmed as data is extracted
    data = {'LinkLabel': label, 'LinkNote': str(label), 'LinkSpeedUnits': '',
            'LinkType': '', 'LinkSpeed': '', 'time': ''}

    do_extract_speed = True

    # Note: Could likely optimise the following into a single regexp
    # Skip speed if ambiguities, ie if - or + present and not in STM- or OC-
    if re.search(r"-+", label):
        # Contains + or -
        # Remove OC- and STM- and check if still contains -
        temp = re.sub(r"OC-\d+|STM-\d+", "", label)
        if re.search(r"-+", temp):
            # label contains - or + not inside OC- or STM-, so skip speed
            do_extract_speed = False

    if do_extract_speed:
        data = extract_speed(data, label)
    data = extract_type(data, label)
    data = extract_temporal(data, label)

    # Tidying up: remove note if it is just punctuation
    # eg () or + left over from extraction
    if ( (len(data['LinkNote']) > 0) and
        (len(re.findall("\w+", data['LinkNote'], re.IGNORECASE)) == 0) ):
        # Non-trivial length and no alphanumeric characters present so wipe note
        data['LinkNote'] = ''

    # Remove any empty fields
    for key, val in data.items():
        if val == '':
            del data[key]

    # Don't include note if same as label - redundant
    if ('LinkNote' in data) and (data['LinkNote'] == data['LinkLabel']):
        del data['LinkNote']

    return data


def extract_svn_version(path):
    # Tries to obtain SVN version from path
    # http://code.djangoproject.com/browser/django/trunk/django/utils/version.py

    if os.path.isfile(path):
        # likely a single file passed in
        path = os.path.split(path)[0]

    entries_file = "{0}/.svn/entries".format(path)
    # check exists
    if not os.path.exists(entries_file):
        return None
    f_entries = open(entries_file, 'r')
    # line after 'dir' is the revision number
    next_line = False
    for line in f_entries:
        if next_line:
            # this line has the version number
            return int(line)
        if line.strip() == 'dir':
            next_line = True
    f_entries.close()


def get_git_version(path):
    import subprocess
    command = ["git", "rev-parse", "--short", "HEAD"]
    p = subprocess.Popen(command,
                     stdin = subprocess.PIPE,
                     stdout = subprocess.PIPE,
                     cwd=path,
                    )
    line = p.stdout.readlines()[0]
    return line.strip()

def main():
    network_files = []
    if options.file:
        network_files.append(options.file)

    if options.directory:
        network_files = glob.glob(options.directory + "*.graphml")

    if len(network_files) == 0:
        logger.warn("No files found. Specify -f file or -d directory")
        sys.exit(0)

    if options.directory:
        path = options.directory
    elif options.file:
        path = os.path.split(options.file)[0]
        # Get full path - don't want to create in root dir
        path = os.path.abspath(path)

    git_version = get_git_version(path)
    topzootools_version = pkg_resources.get_distribution("TopZooTools").version

    if options.output_dir:
        output_path = options.output_dir
    else:
        output_path = path + os.sep + "zoogml"

    if options.geocode:
        output_path += "_geocoded"

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    # clean up path
    output_path = os.path.normpath(output_path)

    #TODO: check nx write pickle uses cPickle

    #TODO: make this optional
    output_pickle_path = output_path + os.sep + "cache"       
    if not os.path.isdir(output_pickle_path):
        os.mkdir(output_pickle_path)

    geocoder = Geocoder(options.geocode, options.skip_cache)

    #output_path += strftime("%Y%m%d_%H%M%S")
    logger.info("Saving to folder: %s" % output_path)
    # and create cache directory for pickle files
    pickle_dir = path + os.sep + "cache"       
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

        #ToDO: check why sorting
    for source_file in sorted(network_files):
        # Extract name of network from file path
        filename = os.path.split(source_file)[1]
        net_name = os.path.splitext(filename)[0]
        logger.info( "Converting {0}".format(net_name))

        pickle_file = "{0}/{1}.pickle".format(pickle_dir, net_name)
        if (os.path.isfile(pickle_file) and
            os.stat(source_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_graphml(source_file)
            nx.write_gpickle(graph, pickle_file)

        graph = convert_to_undirected(graph)
        # Check for self loops
        for n, data in graph.nodes(data=True):
            if n in graph.neighbors(n):
                logger.warn( "Self loop {0} {1}".format(data['label'], n))

        # if all nodes in network are internal, remove redundant internal tag
        # this makes checking if internal too complex, keep tag for all nodes
        """
        if all(data['Internal'] == 1 for n,data in graph.nodes(data=True)):
            # all internal, remove tag
            for n in graph.nodes():
                del graph.node[n]['Internal']
        """

        #*********************************
        # Geocoding (optional)
        #*********************************
        if options.geocode:
            graph = geocoder.geocode(graph)
            geocoder.save_cache()

        #*********************************
        # Remove graphics data
        #*********************************

        # Will be overwritten with name from metadata if present
        network_label = net_name

        # extract edge data
        for src, dst, key, data in graph.edges(data=True, keys=True):
            if 'label' in data:
                label = data['label']
                extracted_data = extract_edge_data(label)
                # Replace the edge data with extracted
                graph.edge[src][dst][key] = extracted_data

        # remove empty note
        if 'Note' in graph.graph and graph.graph['Note'] == '':
            del graph.graph['Note']

        # Strip & as yEd fails on it
        #TODO: use html entitites fn for this
        network_label_clean = network_label.replace("&", "and")

        # Set other graph attributes
        graph.graph['SourceGitVersion'] = git_version
        graph.graph['Creator'] = "Topology Zoo Toolset"
        graph.graph['ToolsetVersion'] = topzootools_version

        graph.graph['label'] = network_label_clean

        #*********************************
        #OUTPUT - Write the graphs to files
        #*********************************

        filename = network_label.lower().strip()
        filename = filename.title()
        filename = filename.replace(" ", "_")

        pattern = re.compile('[\W_]+')
        filename = pattern.sub('', filename)
        # And also the pickle cache - write first so older timestamp
        pickle_out_file =  "{0}/{1}.pickle".format(output_pickle_path, filename)
        nx.write_gpickle(graph, pickle_out_file)

        gml_file =  "{0}/{1}.gml".format(output_path, filename)
        nx.write_gml(graph, gml_file)
        logger.info("Wrote to %s" % gml_file)


#TODO: move the nested functions out to be main functions - and pass in
 #appropriate args (setup args in prev line to nested call)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
