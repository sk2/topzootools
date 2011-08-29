#! /usr/bin/env python

import networkx as nx 
import os
import glob
import optparse
import sys
import pprint as pp

from geopy import distance

import re
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

pickle_dir = filepath + "/cache"
if not os.path.isdir(pickle_dir):
    os.mkdir(pickle_dir)

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
  
    G = nx.Graph(G)

    # Remove any external nodes
    external_nodes = [n for n, data in G.nodes(data=True)
                      if 'Internal' in data and data['Internal'] == 0]
    G.remove_nodes_from(external_nodes)

    geocoded_cities = [ n for n, data in G.nodes(data = True)
                if 'Latitude' in data and 'Longitude' in data]

    # Check all nodes geocoded
    if G.number_of_nodes() != len(geocoded_cities):
        print ("Error: not all cities in {0} geocoded."
               "Skipping").format(network_name)

    # Calculate distance for each edge
    for src, dst, data in G.edges(data=True):
        (x1, y1) = (G.node[src]['Latitude'], G.node[src]['Longitude'])
        (x2, y2) = (G.node[dst]['Latitude'], G.node[dst]['Longitude'])
        geo_dist = distance.distance( (x1, y1), (x2, y2))
        #print "{0} to {1} is {2}".format(G.node[src]['label'],
        #                                 G.node[dst]['label'],
        #                                 geo_dist.kilometers)
        # Store the distance on the edge
        G[src][dst]['weight'] = geo_dist.kilometers


    # This is inefficient, but netx doesn't appear to have a function that 
    # returns both distance and path
    dists = nx.all_pairs_dijkstra_path_length(G)
    paths = nx.all_pairs_dijkstra_path(G)

    for src, data in dists.items():
        for dst, length in data.items():
            if src == dst:
                continue

            # work out path
            src_dst_path = paths[src][dst]
            # and convert to labels
            src_dst_path = [G.node[n]['label'] for n in src_dst_path]
            # and format nicely for printing
            src_dst_path = ", ".join(src_dst_path)
            # Direct distance for comparison
            (x1, y1) = (G.node[src]['Latitude'], G.node[src]['Longitude'])
            (x2, y2) = (G.node[dst]['Latitude'], G.node[dst]['Longitude'])
            direct_dist = distance.distance( (x1, y1), (x2, y2))

            print "{0} to {1}".format( G.node[src]['label'],
                                      G.node[dst]['label'])
            print "Direct {0:.2f}".format(direct_dist.kilometers)
            print "Network: {0:.2f} via {1}".format(length, src_dst_path) 


            print "-----"





