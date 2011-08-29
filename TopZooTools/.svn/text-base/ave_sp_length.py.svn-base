#! /usr/bin/env python

import networkx as nx 
import os
import glob
import optparse
import sys
import pprint
#TODO: put these imports into try/except block
from geopy import distance

import re
import numpy as np
from collections import defaultdict

import csv

def distances(G):
    # Calculate distance for each edge
    for src, dst, data in G.edges(data=True):
        (x1, y1) = (G.node[src]['Latitude'], G.node[src]['Longitude'])
        (x2, y2) = (G.node[dst]['Latitude'], G.node[dst]['Longitude'])
        geo_dist = distance.distance( (x1, y1), (x2, y2))
        #print "{0} to {1} is {2}".format(G.node[src]['label'],
        #                                 G.node[dst]['label'],
        #                                 geo_dist.kilometers)
        # Store the distance on the edge
        G[src][dst]['Distance'] = geo_dist.kilometers
    return G

def sp_info(G):
    #G = distances(G)
    ret_val = {}

    ave_length = nx.average_shortest_path_length(G, weight = 'Distance')
    ret_val['average_length'] = ave_length

    # This is inefficient, but netx doesn't appear to have a function that 
    # returns both distance and path
    dists = nx.all_pairs_dijkstra_path_length(G, weight = 'Distance')
    #paths = nx.all_pairs_dijkstra_path(G)
    #pprint.pprint(dists)

    # Find the longest path in network
    curr_best = 0
    best_pair = None
    for src, data in dists.items():
        for dst, length in data.items():
            if length > curr_best:
                curr_best = length
                best_pair = (src, dst)

    
    ret_val['lp_length'] = curr_best
    ret_val['lp_src'] = G.node[best_pair[0]]['label']
    ret_val['lp_dst'] = G.node[best_pair[1]]['label']
    #print "Longest path is %d km from %s to %s" % (,
    #                                            ['label'],
    #                                            G.node[best_pair[1]]['label'])

    # And find the path taken from these nodes
    path = nx.shortest_path(G, best_pair[0], best_pair[1], weight = 'Distance')
    path_country = [G.node[n]['Country'] for n in path] 
    #ret_val['lp_via'] = ", ".join(path_country)
    # Most frequent country in path
    ret_val['lp_mfc'] =  max(set(path_country), key = path_country.count)

    return ret_val

def speed_info(G):
    kms_per_speed = defaultdict(int)
    for s, t, data in G.edges(data = True):
        if ('LinkSpeed' in data and 'LinkSpeedUnits' in data and 'Distance' in
            data):
            key = "%s%s" % (data['LinkSpeed'], data['LinkSpeedUnits'])
            kms_per_speed[key] += int(data['Distance'])
        else:
            #TODO: also handle LinkType eg fiber, DS-3, etc
            pass
    return dict(kms_per_speed)

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

#TODO: remove this once move mst/clique into converter
# Output dir for saving MST vs Clique
output_path = filepath + os.sep + "ave_sp_length"           
print output_path
# clean up path
output_path = os.path.normpath(output_path)
if not os.path.isdir(output_path):
    os.mkdir(output_path)

link_speeds = {}
shortest_path_info = {}

for net_file in sorted(network_files):
    # Extract name of network from file path
    filepath, filename = os.path.split(net_file)
    network_name, extension = os.path.splitext(filename)
    print "\nNetwork: {0}".format(network_name)

    pickle_file = "{0}/{1}.pickle".format(pickle_dir, network_name)
    if (os.path.isfile(pickle_file) and
        os.stat(net_file).st_mtime < os.stat(pickle_file).st_mtime):
        # Pickle file exists, and source_file is older
        G = nx.read_gpickle(pickle_file)
    else:
        # No pickle file, or is outdated
        G = nx.read_gml(net_file)
        nx.write_gpickle(G, pickle_file)
 
    # Convert to single-edge and undirected
    G = nx.Graph(G)

    # Remove any external nodes
    external_nodes = [n for n, data in G.nodes(data=True)
                      if 'Internal' in data and data['Internal'] == 0]
    G.remove_nodes_from(external_nodes)

    hyperedge_nodes = [n for n, data in G.nodes(data=True) if 'hyperedge' in
                          data and data['hyperedge'] == 1]

    geocoded_cities = [ n for n, data in G.nodes(data = True)
                       if 'Latitude' in data and 'Longitude' in data]

    # Check required qty (ratio) of non-hyperedge nodes geocoded
    possible_geocoded_nodes = G.number_of_nodes() - len(hyperedge_nodes)
    actual_geocoded_nodes = float(len(geocoded_cities))
    required_geocode_ratio = 1.0 
    if ((actual_geocoded_nodes / possible_geocoded_nodes) <
        required_geocode_ratio):
        print ("Error: geocoded node count %i/%i < required ratio of %d "
               "Skipping" % (actual_geocoded_nodes, possible_geocoded_nodes,
               required_geocode_ratio))
        print "%d/%d geocoded" % (len(geocoded_cities), G.number_of_nodes())
        continue
        # Get list of non-geocoded nodes

    if not nx.is_connected(G):
        print "not connected"
        #print nx.connected_components(G)
        continue

    #TODO: check is ok to get distances here

    #TODO: look at hyperedges - no distances, but link speeds still valid

    if len(hyperedge_nodes) == 0:
        G = distances(G)
        # Simple case: no hyperedge nodes, calculate for network
        link_speeds[network_name] = speed_info(G)
        #do similar for mst and clique
        shortest_path_info[network_name] = sp_info(G)
    else:
        # skip for now
        # Hyperedge nodes, calculate for both clique and MST
        # across subgraphs formed by hyperedge nodes

        he_subgraph = G.subgraph(hyperedge_nodes)
        
        # Shallow copy for comparisons 
        G_clique = nx.Graph(G)
        G_mst = nx.Graph(G)
        
        he_connected_components = nx.connected_components(he_subgraph) 

        # Optimisation could look at simple case: all hyperedges are 
        # degree 1 or 2, and do not connect to
        # other hyperedges. We can then simply remove he and connect each node

        # Break into connected components
        for he_connected in he_connected_components:
            # Get all nodes connected to these hyperedges
            neigh_list = []
            for node in he_connected:
                # Neighbors that are not hyperedges
                neigh_list += [neigh for neigh in G.neighbors(node) if neigh 
                               not in
                               hyperedge_nodes]

            # Create clique between these neighbors
            # Use subgraph to retain node properties (for distance calcs)
            he_clique = nx.subgraph(G, neigh_list)
            # Remove all edges, as want clique
            he_clique.remove_edges_from(he_clique.edges())
            print "Clique joining " + ", ".join([G.node[n]['label'] for n in
                                                 neigh_list])
            # Edge between all node pairs, but no self-edges
            edges_to_add = [(a,b) for a in neigh_list for b in neigh_list
                            if a != b]
            he_clique.add_edges_from(edges_to_add)
            # Calculate distances for these edges
            # These are used in the MST algorithm
            he_clique = distances(he_clique)
            # Find MST
            #TODO: double check that distance is used as weight
            he_mst = nx.minimum_spanning_tree(he_clique, weight='Distance')
            print "MST edges: " + ", ".join( [ (G.node[s]['label'] + " <-> " + 
                                                G.node[t]['label'] )
                                              for s, t in he_mst.edges()])

            # Apply new links to the graph
            G_clique.add_edges_from(he_clique.edges())
            G_mst.add_edges_from(he_mst.edges())

            # And remove these hyperedges
            component_he_nodes = [n for n in he_connected if n in
                                  hyperedge_nodes]
            G_clique.remove_nodes_from(component_he_nodes)
            G_mst.remove_nodes_from(component_he_nodes)
            
        
        print "Clique: "
        sp_info(G_clique)
        print "MST: "
        sp_info(G_mst)
        shortest_path_info[network_name + "_clique"] = sp_info(G_clique)
        shortest_path_info[network_name + "_mst"] = sp_info(G_mst)
        # And save for checking
        f_clique = output_path + os.sep + network_name + "_clique.gml"
        nx.write_gml(G_clique, f_clique)
        f_mst = output_path + os.sep + network_name + "_mst.gml"
        nx.write_gml(G_mst, f_mst)


# Get all speeds used for row headings in CSV
# Remove entries that have no speeds
empty_speeds = [n for n, data in link_speeds.items() if not len(data)]
for key in empty_speeds:
    del link_speeds[key]
# Sort CSV column headings (fieldnames)
all_speeds =  link_speeds.values()
fieldnames = set([speed for data in all_speeds for speed in data.keys()])
def speed_to_raw(speed):
    speed_multipliers = {'G': 1e9, 'M': 1e6, 'K': 1e3}
    return float(speed[:-1]) * speed_multipliers[speed[-1:]]
fieldnames = sorted(fieldnames, key = speed_to_raw)
# Network is first CSV field
fieldnames.insert(0, 'Network')
f_csv = open(output_path + os.sep + "link_speeds.csv", "w")
# Use dictwriter as will automatically fill in 0 restval if entry not present
exportWriter = csv.DictWriter(f_csv, fieldnames = fieldnames,
                              restval = 0)
exportWriter.writer.writerow(exportWriter.fieldnames)
for network, data in sorted(link_speeds.items()):
    # Add network name to dict to be written
    data['Network'] = network
    exportWriter.writerow(data)
f_csv.close()

f_csv = open(output_path + os.sep + "sp_info.csv", "w")
fieldnames = ['Network', 'average_length', 'lp_length', 'lp_src', 'lp_dst',
              'lp_mfc']
# Use dictwriter as will automatically fill in 0 restval if entry not present
exportWriter = csv.DictWriter(f_csv, fieldnames = fieldnames,
                              restval = 0)
exportWriter.writer.writerow(exportWriter.fieldnames)
for network, data in sorted(shortest_path_info.items()):
    # Add network name to dict to be written
    data['Network'] = network
    exportWriter.writerow(data)
f_csv.close()


