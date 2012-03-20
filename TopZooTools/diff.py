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
#Convert to single-edge, undirected
    graphA = nx.Graph(graphA)
    graphB = nx.read_gml(fileB)
    graphB = nx.Graph(graphB)

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
    labels_common = labelsA & labelsB
    print "Nodes added:", labels_added
    print "Nodes removed:", labels_removed
# and find the nodes which were added/removed (inverse mapping)
    nodes_added = [label_mappingB[label] for label in labels_added]
    nodes_removed = [label_mappingA[label] for label in labels_removed]
    nodes_common = [label_mappingA[label] for label in labels_common]
    nodes_modified = set()
    for node in nodes_common:
        dataA = graphA[node]
        dataB = graphB[node]
        keysA = set(dataA.keys())
        keysB = set(dataB.keys())
        #print dataA, dataB
        added = keysB - keysA
        removed = keysA - keysB
        same = keysA & keysB
        modified = [key for key in same if dataA[key] != dataB[key]]
        if len(added) or len(removed) or len(modified):
            #TODO: check handling for multi-edge
            nodes_modified.add(node)

    print "modified nodes", nodes_modified
    labels_modified = [graphA.node[n].get("label") for n in nodes_modified]

# need common nodes for edge comparisons

#TODO: need to look at node properties
    
# relabel graphs with labels for comparisons
    graphA_reduced = graphA.subgraph( (n for n in graphA if n not in nodes_removed))
    graphB_reduced = graphB.subgraph( (n for n in graphB if n not in nodes_added))
    reduced_mappingA = dict ( (n, d.get("label")) for n, d in graphA_reduced.nodes(data=True))
    reduced_mappingB = dict ( (n, d.get("label")) for n, d in graphB_reduced.nodes(data=True))
    nx.relabel_nodes(graphA_reduced, reduced_mappingA, copy=False)
    nx.relabel_nodes(graphB_reduced, reduced_mappingB, copy=False)
# can't do sets, as they hash differently, (a,b) hashes different to (b,a)
    edges_removed = set( (s,t) for (s,t) in graphA_reduced.edges() if not graphB_reduced.has_edge(s,t))
    edges_added = set( (s,t) for (s,t) in graphB_reduced.edges() if not graphA_reduced.has_edge(s,t))
    edges_common = set( (s,t) for (s,t) in graphA_reduced.edges() if graphB_reduced.has_edge(s,t))
    edges_modified = set()
    for src, dst in edges_common:
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
        if len(added) or len(removed) or len(modified):
            #TODO: check handling for multi-edges
            edges_modified.add( (src, dst))
        for key in modified:
            print src, dst, dataA[key], "->", dataB[key]

#TODO: save a graphml with the diffs marked up
    mappingA = dict ( (n, d.get("label")) for n, d in graphA.nodes(data=True))
    mappingB = dict ( (n, d.get("label")) for n, d in graphB.nodes(data=True))
    nx.relabel_nodes(graphA, mappingA, copy=False)
    nx.relabel_nodes(graphB, mappingB, copy=False)
    
    composed = nx.compose(graphA, graphB)
    if composed.is_multigraph():
        print "Composed graph is multi-graph, converting to single-edge"
        composed = nx.Graph(composed)

    for n in composed:
        delta = ""
        if n in labels_added:
            delta = "added"
        elif n in labels_removed:
            delta = "removed"
        elif n in labels_modified:
            delta = "modified"
        composed.node[n]['delta'] = delta

    print "edges added", edges_added

    for src, dst in composed.edges():
        delta = ""
        if (src, dst) in edges_added:
            delta = "added"
        elif (src, dst) in edges_removed:
            delta = "removed"
        elif (src, dst) in edges_modified:
            delta = "modified"
        composed[src][dst]['delta'] = delta
#TODO: make this refer to the source graph metadata/filename
    filename_composed = "composed.graphml"
    nx.write_graphml(composed, filename_composed)


#TODO: handle self-loops - where do they come from???


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
