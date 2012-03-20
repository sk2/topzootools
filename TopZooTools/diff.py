#! /usr/bin/env python

import networkx as nx
import sys
import os
import glob
from collections import defaultdict
import pprint
import argparse

"""
problems: hyperedges (eg cesnet)
multi-edge graphs (eg garr200902)
"""


#TODO: provide option that if glob for multiple files, then handle as time series

def main():
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('fileA')
    parser.add_argument('fileB')
    args = parser.parse_args()
    fileA = args.fileA
    fileB = args.fileB
    graphA = nx.read_gml(fileA)
    graphB = nx.read_gml(fileB)
    print "graph A", str(graphA)
    print "graph B", str(graphB)

#TODO: need to handle hyperedges: as no label

# compare labels
    label_mappingA = dict( (d.get("label"), n) 
            for n, d in graphA.nodes(data=True)
            if not d.get("hyperedge")
            )
    label_mappingB = dict( (d.get("label"), n) 
            for n, d in graphB.nodes(data=True)
            if not d.get("hyperedge")
            )
    labelsA = set(label_mappingA.keys())
    labelsB = set(label_mappingB.keys())
# find labels that have changed
    labels_removed = labelsA - labelsB # in A but not B
    labels_added = labelsB - labelsA # in B but not A
    print "added:", labels_added, "removed:", labels_removed
# and find the nodes which were added/removed (inverse mapping)
    nodes_added = [label_mappingB[label] for label in labels_added]
    nodes_removed = [label_mappingA[label] for label in labels_removed]

# need common nodes for edge comparisons
    
# relabel graphs with labels for comparisons
    graphA_reduced = graphA.subgraph( (n for n in graphA if n not in nodes_removed))
    graphB_reduced = graphB.subgraph( (n for n in graphB if n not in nodes_added))
    mappingA = dict ( (n, d.get("label")) for n, d in graphA_reduced.nodes(data=True))
    mappingB = dict ( (n, d.get("label")) for n, d in graphB_reduced.nodes(data=True))
    nx.relabel_nodes(graphA_reduced, mappingA, copy=False)
    nx.relabel_nodes(graphB_reduced, mappingB, copy=False)
    edgesA = set(graphA_reduced.edges())
    edgesB = set(graphB_reduced.edges())
    edges_removed = edgesA - edgesB # in A but not B
    edges_added = edgesB - edgesA # in B but not A
    print "edges added", edges_added, "edges removed", edges_removed
    edges_same = edgesA & edgesB # edges in both
    for src, dst in edges_same:
        dataA = graphA_reduced[src][dst]
        if graphA.is_multigraph():
            if graphA_reduced.number_of_edges(src, dst) == 1:
                dataA = graphA_reduced[src][dst][0] # use data for the only edge
            elif graphB.is_multigraph():
                if graphA_reduced.number_of_edges() != graphB_reduced.number_of_edges():
                    print "Edge count differs for ", src, dst
# both A and B are multigraphs, check the same edge count for node pair
        dataB = graphB_reduced[src][dst]
        if graphB.is_multigraph():
            if graphB_reduced.number_of_edges(src, dst) == 1:
                dataB = graphB_reduced[src][dst][0] # use data for the only edge

# LinkSpeed, LinkLabel, LinkSpeedUnits are all derived from LinkLabel

        keysA = set(dataA.keys())
        keysB = set(dataB.keys())
        #print dataA, dataB
        added = keysB - keysA
        removed = keysA - keysB
        same = keysA & keysB
        modified = [key for key in same if dataA[key] != dataB[key]]
        for key in modified:
            print src, dst, dataA[key], "->", dataB[key]
    




# plotting: want to mark: unchanged, added, removed

    
    
    



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
