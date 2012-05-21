#! /usr/bin/env python

import networkx as nx

import os
import glob

import csv

import pprint
import optparse

opt = optparse.OptionParser()

opt.add_option('--directory', '-d',
            help="Directory containing .gml files to join")

opt.add_option('--csv', '-c',
            help="CSV file for stitching")

options = opt.parse_args()[0]

def main():
    path = options.directory
    network_files = glob.glob(os.path.join(path, "*.gml"))

    pickle_dir = os.path.join(path, "cache")
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    output_dir = os.path.join(path, "stitched")
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    graph_combined = nx.MultiGraph()
    stitched_networks = []
    network_delimeter = "___" # seperate network and name in node names

    for source_file in sorted(network_files):
        # Extract name of network from file path
        filename = os.path.split(source_file)[1]
        net_name = os.path.splitext(filename)[0]

        pickle_file = "{0}/{1}.pickle".format(pickle_dir, net_name)
        if (os.path.isfile(pickle_file) and
            os.stat(source_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_gml(source_file)
            nx.write_gpickle(graph, pickle_file)

#TODO: only keep internal nodes
        graph.remove_nodes_from(n for n, d in graph.nodes(data=True)
                if 'Internal' in d and d['Internal'] == 0)

        network_name = graph.graph['Network']
        stitched_networks.append(network_name)

        #mapping = dict( (n, nx.utils.misc.generate_unique_node()) for n in graph)
        mapping = {}
        for n, d in graph.nodes(data=True):
# set graph network name
            graph.node[n]['Network'] = network_name
            if d.get("label"):
                mapping[n] = d['label'].strip()
            else:
                mapping[n] = n # for hyperedges, etc, retain the id

        nx.relabel_nodes(graph, mapping, copy=False)
        network_prefix = "%s%s" % (network_name, network_delimeter)
        graph_combined = nx.union(graph_combined, graph, rename=("", network_prefix))

#TODO: look at renaming with labels, eg geant_at, this will simplify the connection process!

#pprint.pprint(sorted(graph_combined.nodes()))

    print "Stitched networks:", ", ".join(stitched_networks)

#FIX "None" labels
    for node, data in graph_combined.nodes(data=True):
        label = data.get("label")
        if not label:
            continue
        
        if label == "None":
            graph_combined.node[node]["label"] = ""
# and escape
        if "&" in label:
            graph_combined.node[node]["label"] = label.replace("&", "&gt;")

# now perform stitching
    csv_file = open( options.csv, "rU" )
    csv_reader = csv.reader(csv_file, dialect='excel')
    for line in csv_reader:
        network_a, node_label_a, network_b, node_label_b = line
#check networks exist
        if network_a not in stitched_networks:
            print "Network", network_a, "not present"
            continue
        if network_b not in stitched_networks:
            print "Network", network_b, "not present"
            continue

        node_id_a = "".join([network_a, network_delimeter, node_label_a])
        if node_id_a not in graph_combined:
# Network valid if reached this point, suggest nodes"
            print "Unable to find", node_label_a, "in", network_a
            print "Nodes in", network_a, ":", ", ".join([d.get("label")
                    for n, d in graph_combined.nodes(data=True)
                    if d.get("Network") == network_a])
            continue

        node_id_b = "".join([network_b, network_delimeter, node_label_b])
        if node_id_b not in graph_combined:
# Network valid if reached this point, suggest nodes"
            print "Unable to find", node_label_b, "in", network_b
            print "Nodes in", network_b, ":", ", ".join([d.get("label")
                    for n, d in graph_combined.nodes(data=True)
                    if d.get("Network") == network_b])
            continue

        #Join
        print "Connecting", node_label_a, "in", network_a, "to", node_label_b, "in", network_b
        graph_combined.add_edge(node_id_a, node_id_b)
# print edges
    for src, dst in sorted(graph_combined.edges()):
        print src, "to", dst

    print "Disconnected nodes", ", ".join([ n for n in graph_combined
        if graph_combined.degree(n) == 0])

#pprint.pprint(graph_combined.nodes())
    print "Saving as single-edge graph"
    graph_combined = nx.Graph(graph_combined) #single edge for now
    nx.write_graphml(graph_combined, os.path.join(output_dir, "graph_combined.graphml"))
# Reset id attribute, as write_gml uses this as node id -> collides, reduced node count
    for n in graph_combined:
        del graph_combined.node[n]['id']
    nx.write_gml(graph_combined, os.path.join(output_dir, "graph_combined.gml"))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
