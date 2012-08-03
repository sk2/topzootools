#! /usr/bin/env python

import networkx as nx
import argparse
import TopZooTools.geoplot
import pprint



"""
problems: hyperedges (eg cesnet)
multi-edge graphs (eg garr200902)
"""


#TODO: provide option that if glob for multiple files, then handle as time series

def main():
    parser = argparse.ArgumentParser(prog='PROG')
    parser.add_argument('files', nargs='+')
    args = parser.parse_args()
    files = args.files
    if len(files) < 2:
        print "Need at least two files to compare"
        return
    if len(files) == 2:
        fileA, fileB = files
        compare(fileA, fileB)
    else:
# sort chronologically
        print "Loading graph metadata for chronological sorting"
        network_date = {}
        for network_file in files:
            graph = nx.read_gml(network_file)
            network_date[network_file] = (graph.graph['DateYear'], graph.graph['DateMonth'])
        files = sorted(files, key = lambda x: network_date[x])
        pairs = [(a, b) for (a, b) in zip(files, files[1:])]
        for fileA, fileB in pairs:
            compare(fileA, fileB)
            print "---------------"
        
# See if A is a wildcard -> glob

def compare(fileA, fileB):
    print "Comparing ", fileA, "to", fileB
    graphA = nx.read_gml(fileA)
#Convert to single-edge, undirected
    #graphA = nx.Graph(graphA)
    graphB = nx.read_gml(fileB)
    #graphB = nx.Graph(graphB)

    labelsA = [d.get("label") for n,d in graphA.nodes(data=True)]
    duplicateLabelsA = set( label for label in labelsA if label != "None" and labelsA.count(label) > 1 )
    labelsB = [d.get("label") for n,d in graphB.nodes(data=True)]
    duplicateLabelsB = set( label for label in labelsB if label != "None" and labelsB.count(label) > 1 )
    if duplicateLabelsA:
        print "Duplicate labels in graph A:", ", ".join(sorted(duplicateLabelsA))
    if duplicateLabelsB:
        print "Duplicate labels in graph B:", ", ".join(sorted(duplicateLabelsB))

    # relabel hyperedges based on neighbors
    hyperedges_a = [n for n, d in graphA.nodes(data=True) if d.get("hyperedge")]
    for hyperedge in hyperedges_a:
        neighbors = sorted(graphA.node[n].get("label") for n in graphA.neighbors(hyperedge))
        new_label = "_".join(neighbors)
        graphA.node[hyperedge]['label'] = new_label
    hyperedges_b = [n for n, d in graphB.nodes(data=True) if d.get("hyperedge")]
    for hyperedge in hyperedges_b:
        neighbors = sorted(graphB.node[n].get("label") for n in graphB.neighbors(hyperedge))
        new_label = "_".join(neighbors)
        graphB.node[hyperedge]['label'] = new_label

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
    print "Nodes added:", ", ".join(sorted(labels_added))
    print "Nodes removed:", ", ".join(sorted(labels_removed))
# and find the nodes which were added/removed (inverse mapping)
    nodes_added = [label_mappingB[label] for label in labels_added]
    nodes_removed = [label_mappingA[label] for label in labels_removed]
    labels_modified = set()
    for label in labels_common:
        nodeA = label_mappingA[label]
        nodeB = label_mappingB[label]
        dataA = graphA.node[nodeA]
        dataB = graphB.node[nodeB]
        keysA = set(dataA.keys())
        keysB = set(dataB.keys())
        #print dataA, dataB
        added = keysB - keysA
        removed = keysA - keysB
        same = keysA & keysB
        modified = [key for key in same if dataA[key] != dataB[key] and key != 'id']
        if len(added) or len(removed) or len(modified):
            #TODO: check handling for multi-edge
            print "modified", label
            pprint.pprint(dataA)
            pprint.pprint(dataB)
            labels_modified.add(label)
    print "Nodes modified:", ", ".join(sorted(labels_modified))

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

# add in edges from added nodes
    for node in nodes_added:
        for s, t in graphB.edges(node):
            edges_added.add((graphB.node[s]['label'], graphB.node[t]['label']))
# try and remove if exists in opposite direction

    for node in nodes_removed:
        for s, t in graphA.edges(node):
            edges_removed.add((graphA.node[s]['label'], graphA.node[t]['label']))

    edges_common = set( (s,t) for (s,t) in graphA_reduced.edges() if graphB_reduced.has_edge(s,t))
    if edges_added:
        print "Edges added:", ", ".join(sorted("(%s, %s)" % (s,t) for s,t in edges_added))
    if edges_removed:
        print "Edges removed:", ", ".join(sorted("(%s, %s)" % (s,t) for s,t in edges_removed))

    edges_modified = set()
    print "Edges modified:"
    for src, dst in edges_common:
        dataA = graphA_reduced[src][dst]
        if graphA.is_multigraph():
            if graphA_reduced.number_of_edges(src, dst) == 1:
                dataA = graphA_reduced[src][dst][0] # use data for the only edge
            elif graphB.is_multigraph():
                if graphA_reduced.number_of_edges(src) != graphB_reduced.number_of_edges(dst):
                    print "Edge count differs for ", src, dst, "(", graphA_reduced.number_of_edges(src), "vs", graphB_reduced.number_of_edges(dst), ")"
                    edges_modified.add( (src, dst))
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
#TODO: better handling of multi edges here
        modified = [key for key in same if dataA[key] != dataB[key] and key != 'id']
        if len(modified):
            print "same keys", same
            print "modified keys", modified
        if len(added) or len(removed) or len(modified):
            #TODO: check handling for multi-edges
            edges_modified.add( (src, dst))
        for key in modified:
            print src, dst, dataA[key], "->", dataB[key]

#TODO: save a graphml with the diffs marked up
    mappingA = dict ( (n, d.get("label")) for n, d in graphA.nodes(data=True))
    mappingB = dict ( (n, d.get("label")) for n, d in graphB.nodes(data=True))
    graphARelabelled = nx.relabel_nodes(graphA, mappingA)
    graphBRelabelled = nx.relabel_nodes(graphB, mappingB)
# and reduce to single edge
    graphARelabelled = nx.Graph(graphARelabelled)
    graphBRelabelled = nx.Graph(graphBRelabelled)
    
    composed = nx.compose(graphARelabelled, graphBRelabelled)
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

    for src, dst in sorted(composed.edges()):
        delta = ""
        if (src, dst) in edges_added or (dst, src) in edges_added:
            delta = "added"
        elif (src, dst) in edges_removed or (dst, src) in edges_removed:
            delta = "removed"
        elif (src, dst) in edges_modified or (dst, src) in edges_modified:
            delta = "modified"
        composed[src][dst]['delta'] = delta
#TODO: make this refer to the source graph metadata/filename
    if composed.number_of_selfloops():
        for label in composed.nodes_with_selfloops():
            pass

    # and strip out the label from hyperedges
    for n in composed:
        if composed.node[n].get("hyperedge"):
            composed.node[n]['label'] = ""

    # set metadata
    title = ""
    if graphA.graph['Network'] == graphB.graph['Network']:
        title = "%s %s vs %s" % (
                graphA.graph['Network'],
                graphA.graph['NetworkDate'],
                graphB.graph['NetworkDate'],
                )
    else:
        title = "%s %s vs %s %s" % (
                graphA.graph['Network'],
                graphA.graph['NetworkDate'],
                graphB.graph['Network'],
                graphB.graph['NetworkDate'],
                )


    composed.graph["Network"] = title
    composed.name = title

    filename_composed = "composed.graphml"
    nx.write_graphml(composed, filename_composed)
    #nx.write_gml(composed, "composed.gml")


#TODO: integrate plotting - if --plot then plot
    output_path = ""
    TopZooTools.geoplot.plot_graph(composed, output_path,
                    #use_labels=True,
                    #title=True,
                    label_font_size=12,
                    #use_bluemarble=True,
                    line_width = 3,
                    node_size = 150,
                    edge_font_size=8,
                    basemap_resolution_level = 3,
                    #basemap_resolution_level = 1,
                    pdf=True,
                    country_color="#cccccc",
                    user_default_edge_color = "k",
                    )

#TODO: handle self-loops - where do they come from???

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
