#! /usr/bin/env python

import networkx as nx
import sys
import os
import glob
from collections import defaultdict
import pprint


def main():
    diff_dir = "difftest" + os.sep
    master_file = "Aarnet.graphml"
    network_files = glob.glob(diff_dir + "*.graphml")
    # Remove main master source to compare to all the others
    # (Don't want to compare it to itself)
    network_files = [n for n in network_files if master_file not in n]
    G_master = nx.read_graphml(diff_dir + master_file)
    for file in network_files:
        filename = os.path.splitext(os.path.split(file)[1])[0]
        print "Comparing %s to %s" % (master_file.replace(".graphml", ""),
                                      filename)
        G_b = nx.read_graphml(file)
        compare(G_master, G_b)
        print

    return
    


    """
    #TODO: if count > 3 then compare first to the
    # rest, eg compare 2 to 3, 4, 5, etc
    if len(sys.argv) < 2:
    print "Please provide two files to compare."
    sys.exit(0)
    file_a = sys.argv[1]
    file_b = sys.argv[2]

    # Load graphs
    G_a = G_b = None
    try:
        G_a = nx.read_graphml(file_a)
    except IOError as (errno, strerror):
        print "Unable to load %s: %s" % (file_a, strerror)
        try:
            G_b = nx.read_graphml(file_b)
        except IOError as (errno, strerror):
            print "Unable to load %s: %s" % (file_b, strerror)

    # Check loaded fine
    if not(G_a and G_b):
        sys.exit(0)
        """

def compare(G_a, G_b):
    statistics = defaultdict(dict)

    # Node count
    statistics['a']['node_count'] = G_a.number_of_nodes()
    statistics['b']['node_count'] = G_b.number_of_nodes()

    statistics['a']['edge_count'] = G_a.number_of_edges()
    statistics['b']['edge_count'] = G_b.number_of_edges()

    # Node names (ids)
    names_a = [n for n in G_a]
    names_b = [n for n in G_b]
    names_diff = set(names_a) ^ set(names_b)
    # Use common names for future comparisons on nodes that exist in both
    names_common = set(names_a) & set(names_b)
    #print sorted(names_a)
    #print sorted(names_b)
    #print names_diff

    # Compare node labels
    #print ",".join(d['label'] for n,d in G_a.nodes(data=True))
    #print ",".join(d['label'] for n,d in G_b.nodes(data=True))
    labels_diff = [n for n in names_common 
                   if G_a.node[n]['label'] != G_b.node[n]['label']]
    for n in names_common:
        if G_a.node[n]['label'] != G_b.node[n]['label']:
            print "diff", G_a.node[n]['label'], " ", G_b.node[n]['label']
    if len(labels_diff):
        print "Different label nodes:", labels_diff


    pprint.pprint(dict(statistics))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
