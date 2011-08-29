#! /usr/bin/env python

import networkx as nx 
import os
import glob
import optparse
import sys
import pprint as pp

import matplotlib.pyplot as plt

import numpy as np

opt = optparse.OptionParser()
opt.add_option('--directory', '-d', help="process directory")
opt.add_option('--file', '-f', help="process file")
opt.add_option('--output_dir', '-o', help="process directory")

options, args = opt.parse_args()

network_files = []

if options.file:
    network_files.append(options.file)

use_pickle = False
if options.directory:
    network_files = glob.glob(options.directory + "*.gml")

if len(network_files) == 0:
    # try pickle format
    #toDO: also make pickle work for single file - use extracted dir
    network_files = glob.glob(options.directory + "*.pickle")
    if len(network_files) > 0:
        use_pickle = True
    else:
        print "No files found. Please specify a -f file or -d directory"
        sys.exit(0)


if options.directory:
    filepath = options.directory
elif options.file:
    filepath, filename = os.path.split(options.file)

pickle_dir = filepath + "cache"
if not os.path.isdir(pickle_dir):
    os.mkdir(pickle_dir)


interconnect = {}

#TODO: look for same network over multiple time series, as may give false or
# misleading stats, eg Garr has many of same external peers over the time period

interconnect_graph = nx.Graph()

for net_file in sorted(network_files):
    # Extract name of network from file path
    filepath, filename = os.path.split(net_file)
    network_name, extension = os.path.splitext(filename)
    print "Analysing: {0}".format(network_name)

    pickle_file = "{0}/{1}.pickle".format(pickle_dir, network_name)
    if (os.path.isfile(pickle_file) and
        os.stat(net_file).st_mtime < os.stat(pickle_file).st_mtime):
        # Pickle file exists, and source_file is older
        G = nx.read_gpickle(pickle_file)
    else:
        # No pickle file, or is outdated
        G = nx.read_gml(net_file)
        nx.write_gpickle(G, pickle_file)
 
    # Convert to simple undirected, single-edge graph
    G = nx.Graph(G)

    # Remove any external nodes
    external_nodes = [n for n, data in G.nodes(data=True)
                      if 'Internal' in data and data['Internal'] == 0]

    # add these nodes to the as graph
    for node in external_nodes:
        node_label = G.node[node]['label']
        
        if node_label not in interconnect:
            interconnect[node_label] = []

        # and add the links from this node
        for neigh in G.neighbors(node):
            # TODO: add edge properties also from node to neigh
            neigh_id = (G.graph['Network'], G.node[neigh]['label'])

            if neigh_id not in interconnect[node_label]:
                # Don't add if already in there (likely from time series plots)
                interconnect[node_label].append(neigh_id)


print "Results: "

# save the graph



for ext_net, data in sorted(interconnect.items()):
    if len(data) > 1:
        # Exclude trivial cases
        print "---{0}:".format(ext_net)
        for pop in data:
            print pop
        print

