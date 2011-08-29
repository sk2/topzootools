#! /usr/bin/env python

import networkx as nx 
import os
import glob
import optparse
import sys
import pprint
import re
import itertools
import difflib

import matplotlib.pyplot as plt

import numpy as np
import cPickle
from collections import defaultdict

import subprocess
#TODO: run pylint across

#!/usr/bin/env python
import numpy as np
import matplotlib.pyplot as plt

#Niran connects ja.net to heanet
# see http://www.ja.net/company/the-janet-network/

geant_to_zoo_mapping = {

}

#print traces
#Mapping of URLS to networks
# from http://www.geant.net/About_GEANT/Partners/Pages/home.aspx 
url_to_network_mapping = {
    "www.aco.net": "ACOnet", "www.belnet.be": "BELnet",
    "www.bren.bg": "BREN", "www.carnet.hr": "CARNet",
    "www.cynet.ac.cy": "CYNET", "www.ces.net": "CESNET",
    "www.eenet.ee": "EENet", "www.renater.fr": "RENATER",
    "www.dfn.de": "DFN", "www.grnet.gr": "GRNET",
    "www.niif.hu": "NIIF", "www.heanet.ie": "HEAnet",
    "www.iucc.ac.il": "IUCC", "www.garr.net": "GARR",
    "www.sigmanet.lv": "SigmaNet", "www.litnet.lt": "LITNET",
    "www.restena.lu": "RESTENA", "dns.marnet.net.mk": "MARNet",
    "www.um.edu.mt": "University of Malta", "www.mren.ac.me": "MREN",
    "www.surfnet.nl": "SURFnet", "www.nordu.net": "NORDUnet",
    "www.man.poznan.pl": "PSNC", "www.fccn.pt": "FCCN",
    "www.nren.ro": "AARNIEC/RoEduNet", "www.amres.ac.rs": "AMRES",
    "www.sanet.sk": "SANET", "www.arnes.si": "ARNES",
    "www.rediris.es": "RedIris", # Corrected from "Switzerland"
    "www.switch.ch": "SWITCH", "www.ulakbim.gov.tr": "ULAKBIM",
    "www.ja.net": "JANET", "www.dante.net": "DANTE",
    "www.terena.org": "TERENA", "www.bas-net.by": "BASNET",
    "www.jscc.ru": "JSCC", "www.renam.md": "RENAM",
    "www.uran.net.ua": "URAN", }    

# Generate list for use in bash traceroute script
#print " ".join("\"%s\""%url for url in url_to_network_mapping)
def parse_traceroute(trace_file, url_to_network_mapping):

    f_trace = open(trace_file)
    traces = defaultdict(list)
    dest = None
    for line in f_trace:
        if "tracing  " in line:
            # format "tracing  dest"
            dest = line.split()[1]
        elif dest:
            traces[dest].append(line.strip())

    # now look at 
    geant_egress = {}
    last_ip = {}
    # Find transition point where trace leaves Geant2 network
    for url, trace in traces.items():
        network = url_to_network_mapping[url]
        for (a,b) in zip(trace[:], trace[1:]):
            if 'geant2.net' in a and 'geant2.net' not in b:
                # extract Geant pop name
                re_m = re.search("(\w+.\w+).geant2.net", a)
                if re_m:
                    geant_pop = re_m.group(1)
                else:
                    print "Geant pop regex failed: %s in %s" % (a, dest)

                re_m = re.search("\d+  ([a-zA-Z0-9\-\.]+)", b)
                if re_m:
                    remote_pop = re_m.group(1)
                elif re.search("\d+  *", b):
                    # Unable to resolve
                    remote_pop = "*"
                else:
                    print "Remote pop regex failed: %s in %s" % (b, dest)

                if geant_pop and remote_pop:
                    geant_egress[network] = (geant_pop, remote_pop)
    return geant_egress

def last_ip(trace_file, url_to_network_mapping):
    f_trace = open(trace_file)
    traces = defaultdict(list)
    dest = None
    for line in f_trace:
        if "tracing  " in line:
            # format "tracing  dest"
            dest = line.split()[1]
        elif dest:
            traces[dest].append(line.strip()) 

    last_ip_result = {}
    for url, trace in traces.items():
        network = url_to_network_mapping[url]
        # Keep the last seen IP for this network
        # overwrite on each loop if valid, store at end
        curr_ip = None
        for elem in trace:
            if elem.split()[1] == "*":
                # Timed out, skip
                pass
            else:
                re_m = re.search("\d+  ([a-zA-Z0-9\-\.]+) "
                                 "\(((\d{1,3}.){3}\d{1,3})\)", elem)
                if re_m:
                    curr_ip = re_m.group(2)

        last_ip_result[network] = curr_ip

    return last_ip_result

geant_name_to_zoo_name = {
    "ARNES": "ARNES",
     #"RoEduNet": "AARNIEC/RoEduNet", 
    # Use simple name without the / in it
    "RoEduNet": "RoEduNet", 
    "ACOnet": "ACOnet",
    "AMRES": "AMRES", "BASNET": "BASNET", "BELNET": "BELnet",
    "BREN": "BREN", "Carnet": "CARNet", "CESNET": "CESNET",
    "Cynet": "CYNET", "DFN": "DFN", "EEnet": "EENet",
    "FCCN": "FCCN", "FUNET": "FUNET", "GARR": "GARR",
    "GEANT": "GEANT", "GRnet": "GRNET", "Heanet": "HEAnet",
    "ILAN": "IUCC", "Janet Backbone": "JANET", "LITNET": "LITNET",
    "MARNET": "MARNet", "MREN": "MREN", "NIIF": "NIIF",
    "NORDU": "NORDUnet", "OPTOSUNET": "SUNET", "PIONIER": "PSNC",
    "RedIris": "RedIris", "Renam": "RENAM", "Renater": "RENATER",
    "Restena": "RESTENA", "RHnet": "RHnet", "SANET": "SANET",
    "LATNET": "SigmaNet", "SURFNET": "SURFnet", "SWITCH": "SWITCH",
    "ULAKNET": "ULAKBIM", "Uni-C": "Uni-C", "Uninett": "Uninett",
    "Malta": "University of Malta", "URAN": "URAN", } 

trace_uofa = parse_traceroute(
    "/Users/sk2/Dropbox/PhD/Dev/geant/traceroute.txt",
    url_to_network_mapping)

trace_lboro = parse_traceroute(
    "/Users/sk2/Dropbox/PhD/Dev/geant/traceroute_lboro.txt",
    url_to_network_mapping)

last_ip_uofa = last_ip(
    "/Users/sk2/Dropbox/PhD/Dev/geant/traceroute.txt",
    url_to_network_mapping)

def network_to_asn(last_ip_list):
    print "Looking up ASN for network:"
    network_to_asn_mapping = {}
    ips_to_lookup = last_ip_list.copy()

    print ips_to_lookup
    # Add Nordu etc

    #basun.sunet.se is an alias for web.sunet.se.
    #web.sunet.se has address 109.105.111.14
    ips_to_lookup['SUNET'] = "109.105.111.14"
    # uninett.no has address 158.38.130.37
    ips_to_lookup["Uninett"] = "158.38.130.37"
    # forskningsnettet.dk has address 130.225.254.150
    ips_to_lookup["Uni-C"] = "130.225.254.150"
    # www.csc.fi has address 81.90.77.32
    ips_to_lookup["FUNET"] = "81.90.77.32"
    # www.um.edu.mt has address 193.188.46.72
    ips_to_lookup["University of Malta"] = "193.188.46.72"
    # www.rhnet.is is an alias for frosti.rhnet.is.
    # frosti.rhnet.is has address 130.208.16.23
    ips_to_lookup["RHnet"] = "130.208.16.23"
    # An IP in the GEANT range
    # as2.rt1.gen.ch.geant2.net (62.40.112.25) 
    ips_to_lookup["GEANT"] = "62.40.112.25"
    # www.mren.ac.me has address 89.188.43.17
    ips_to_lookup["MREN"] = "89.188.43.17"
    # www.renater.fr has address 193.49.159.10
    ips_to_lookup["RENATER"] = "193.49.159.10"
    # www.renam.md has address 81.180.64.35
    ips_to_lookup["RENAM"] = "81.180.64.35"

    for network, ip in ips_to_lookup.items():
        print network
        print ip
        # based on http://stackoverflow.com/questions/4760215
        p = subprocess.Popen(["whois", "-h", "whois.ra.net", ip],
                             stdout=subprocess.PIPE)
        for line in p.stdout.readlines():
            if line.startswith("origin:"):
                asn = line.split()[1]
                network_to_asn_mapping[network] = asn
    return network_to_asn_mapping

#network_to_asn_mapping = network_to_asn(last_ip_uofa)

#pprint.pprint(network_to_asn_mapping)

# One prepared earlier
network_to_asn_mapping = {
    # use simplified form of name
    #'AARNIEC/RoEduNet': 'AS2614',
    'RoEduNet': 'AS2614',
    'ACOnet': 'AS1853', 'AMRES': 'AS13092', 'ARNES': 'AS2107', 
    'BASNET': 'AS21274',
    'BELnet': 'AS2611', 'BREN': 'AS6802', 'CARNet': 'AS2108', 
    'CESNET': 'AS2852',
    'CYNET': 'AS3268', 'DANTE': 'AS20965', 'DFN': 'AS680', 'EENet': 'AS3221',
    'FCCN': 'AS1930', 'FUNET': 'AS8624', 'GARR': 'AS137', 'GEANT': 'AS20965',
    'GRNET': 'AS5408', 'HEAnet': 'AS1213', 'IUCC': 'AS378', 'JANET': 'AS786',
    'JSCC': 'AS3058', 'LITNET': 'AS2847', 'MARNet': 'AS5379', 'MREN': 'AS13092',
    'NIIF': 'AS1955', 'NORDUnet': 'AS2603', 'SUNET': 'AS2603', 'PSNC': 'AS9112',
    'RENAM': 'AS9199', 'RESTENA': 'AS2602', 'RHnet': 'AS15474',
    'RedIris': 'AS766',
    'RENATER': 'AS2200', 'SANET': 'AS2607', 'SURFnet': 'AS1103',
    'SWITCH': 'AS559',
    'SigmaNet': 'AS5538', 'TERENA': 'AS1103', 'ULAKBIM': 'AS1967',
    'URAN': 'AS12687', 'Uni-C': 'AS1835', 'Uninett': 'AS224',
    'University of Malta': 'AS12046'} 

# And remove the "AS" bit from result
for key, val in network_to_asn_mapping.items():
    network_to_asn_mapping[key] = val.replace("AS", "")

# And print for Latex
for key, val in sorted(network_to_asn_mapping.items()):
    print "%s & %s \\\\" % (key, val)

print 

print "--------------------------------------------"
print "ASN Mappings:"
#pprint.pprint(network_to_asn_mapping)
print "--------------------------------------------"

"Print traceroutes via UofA:"
"""
for network, (geant_pop, remote_pop) in sorted(trace_uofa.items(),
                                               key=lambda x: x[1][0]):
    print "%s->%s (%s)"%(geant_pop, network, remote_pop)
"""

for network, (geant_pop, remote_pop) in sorted(trace_uofa.items()):
    print "%s->%s"%(geant_pop, network)

print "--------------------------------------------"
"""
print "---------------"
for network, (geant_pop, remote_pop) in sorted(trace_lboro.items(),
                                               key=lambda x: x[1][0]):
    print "%s->%s (%s)"%(geant_pop, network, remote_pop)

""" 

#TODO: look for nodes with same co-ords but in different networks

#TODO: show geographically nearby nodes

"""
prev_line = ""
for line in f_trace:
    line = line.strip()
    if trace_destination:
        # Process the results
        print "------"
        print "prev is " + prev_line
        print "line is " + line
        if "geant2.net" in prev_line and "geant2.net" not in line:
            print "changeover"
        prev_line = line
    else:
        # See if this line starts a traceroute
        re_m = re.search("traceroute to ([a-zA-Z0-9\-\.]+)", line)
        if re_m:
            trace_destination = re_m.group(1)
""" 

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

# Don't want to write into root directory
filepath = os.path.abspath(filepath)

if options.output_dir:
    output_path = options.output_dir 
else:
    output_path = filepath + os.sep + "interconnect"

    
if not os.path.isdir(output_path):
    os.mkdir(output_path)  

pickle_dir = filepath + "cache"
if not os.path.isdir(pickle_dir):
    os.mkdir(pickle_dir) 

interconnect = {}

#TODO: look for same network over multiple time series, as may give false or
# misleading stats, eg Garr has many of same external peers over the time period

interconnect_graph = nx.Graph()
# Working to find out where interconnects occur
peering_graph = nx.Graph()
next_id = (id for id in itertools.count(0) 
           if id not in interconnect_graph and id not in peering_graph)

node_total = 0

geant_pop_graph = None

networks_present = []
network_names = set()

network_stats = {}

for net_file in sorted(network_files):
    # Extract name of network from file path
    filepath, filename = os.path.split(net_file)
    network_name, extension = os.path.splitext(filename)
    #print "Analysing: {0}".format(network_name)

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
    G = G.to_undirected()

    # Relabel so node ids don't conflict with existing in interconnect_graph
    # and so they are consistent between interconnect_graph and peering_graph
    # Need a unique ID that is not use in interconnect_graph or peering_graph

    label_mapping = dict( (n, next_id.next()) for n in G.nodes_iter())
    G = nx.relabel_nodes(G, label_mapping)

    # Append ASN. ASNs use network name from GEANT, 
    if G.graph['Network'] in geant_name_to_zoo_name:
        geant_net_name = geant_name_to_zoo_name[G.graph['Network']]
        # And substitute GEANT Network name
        G.graph['Network'] = geant_net_name

        # Append ASN
        asn = network_to_asn_mapping[geant_net_name]
        for n in G.nodes():
            # Lower case asn for ANK
            G.node[n]['asn'] = asn

    # Store origin network in nodes
    for n in G.nodes_iter():
        G.node[n]['Network'] = G.graph['Network']
        G.node[n]['GeoLocation'] = G.graph['GeoLocation']
        G.node[n]['NetworkDate'] = G.graph['NetworkDate']

    network_names.add(G.graph['Network'])

    print "----------------------"
    print G.graph['Network']
    print G.graph['GeoLocation']
    print "Layer:\t%s" % G.graph['Layer']
    if G.graph['Layer'] != "IP":
        print "WARNING, layer type is not IP: %s" % G.graph['Layer']

    # Remove any external nodes
    external_nodes = [n for n, data in G.nodes(data=True)
                    if 'Internal' in data and data['Internal'] == 0]
    internal_nodes = [n for n, data in G.nodes(data=True)
                    if 'Internal' in data and data['Internal'] == 1]
    hyperedge_nodes =  [n for n, data in G.nodes(data=True)
                    if 'hyperedge' in data and data['hyperedge'] == 1]

    #TODO: rename with name from GEANT for consistency or show mappings?
    
    # also need to look at networks which are all internal nodes
    non_ext = len(G) - len(external_nodes)
    node_total += non_ext
    #print non_ext
   
    if G.graph['Network'] == 'GEANT':
        geant_pop_graph = nx.Graph(G)

        geant_router_types = set(["IP/MPLS only", "Fully Featured",
                                 "Off fibre net"])
        for n,d in G.nodes(data=True):
            if d['type'] not in geant_router_types:
                print "removing geant node %s " % d['label']
                # Connect the neighbors of the node together, assume clique
                neigh_list = [neigh for neigh in G.neighbors(n)]
                neigh_edges = ( (a,b) for a in neigh_list for b in neigh_list
                               if a != b)
                G.add_edges_from(neigh_edges)

                G.remove_node(n)

        # And include in peering graph
        # Don't want GEANT nodes connected together in peering graph
        boundary_graph = nx.Graph(G)
        # Keep the pop level graph for NREN peerings
        for n in boundary_graph.nodes():
            boundary_graph.node[n]['Internal'] = 1
            # Only keep GEANT router nodes

        boundary_graph.remove_edges_from(boundary_graph.edges())
        peering_graph = nx.compose(peering_graph, boundary_graph)
    else:
        # Only keep edges which connect an internal to an external node
        # And then remove all degree one
        # Find nodes which connect to external nodes
        boundary_graph = nx.Graph(G)
        edges_to_remove = ( (s,t) for s, t in G.edges_iter()
                           # Both Internal == 1 (internal node) or == 0
                           # (external node)
                           if G.node[s]['Internal'] == G.node[t]['Internal'])

        boundary_graph.remove_edges_from(edges_to_remove)
        nodes_to_remove = (n for n in boundary_graph.nodes() if
                           boundary_graph.degree(n) == 0)
        boundary_graph.remove_nodes_from(nodes_to_remove)
        
        # Replace connections from hyperedge to neighboring internal nodes
        boundary_he_nodes = [n for n in boundary_graph if n in hyperedge_nodes]
        for node in boundary_he_nodes:
            print "boundary he node"
            # See who hyperedge connects to
            he_neigh = G.neighbors(node)
            he_int_neigh = [n for n in he_neigh if (n in internal_nodes 
                                                    and n not in hyperedge_nodes)]
            he_ext_neigh = [n for n in he_neigh if (n in external_nodes)]
            # Remove hyperedge
            boundary_graph.remove_node(node)
            # Add data for internal boundary nodes if now included because of
            # hyperedge removal
            for he_int in he_int_neigh:
                if he_int not in boundary_graph:
                    boundary_graph.add_node(he_int, G.node[he_int])

            # Find boundary nodes of hyperedge
            new_links = itertools.product(he_int_neigh, he_ext_neigh)
            boundary_graph.add_edges_from(new_links)
            for s,t in boundary_graph.edges_iter():
                boundary_graph[s][t]['label'] = 'z'

        boundary_graph = boundary_graph.to_undirected()
        peering_graph = nx.compose(peering_graph, boundary_graph)

    # Look at types present
    types = [d['type'] for n, d in G.nodes_iter(data=True)
                if 'type' in d]
    types_unique = set(types)
    if types_unique:
        print "Node types:\t" + ", ".join( "%s (%s)"%(x, types.count(x))
                                          for x in types_unique)

    G.remove_nodes_from(external_nodes)
    # relabel based on label, network name
    #label_mapping = dict( (n, (G.graph['Network'], d['label'])) for n, d in
    #                     G.nodes_iter(data=True))
    #G = nx.relabel_nodes(G, label_mapping)
    interconnect_graph = nx.compose(interconnect_graph, G)

    print "Nodes:\t%s" % G.number_of_nodes()
    print "Edges:\t%s" % G.number_of_edges()
    print "External Connections:\t%s" % len(external_nodes)
    if "Note" in G.graph:
        print "Note: %s " % G.graph['Note']
    network_stats[G.graph['Network']] = (G.number_of_nodes(),
                                         G.number_of_edges(),
                                         len(external_nodes),
                                         G.graph['NetworkDate'])
    networks_present.append(G.graph['Network'])

    print

print "-------------------------"
print "removing disconnected Nordu node of Espoo"
nordu_espoo_id = [n for n, d in interconnect_graph.nodes(data=True)
                  if d['Network'] == 'NORDUnet' and d['label'] == 'Espoo'].pop()
interconnect_graph.remove_node(nordu_espoo_id)


print [n for n, d in interconnect_graph.nodes(data=True)
     if d['label'] == "TR"]
print  "********************"

# Lower case versions of networks for matching
lower_zoo_networks = set(n.lower() for n in network_names)
lower_geant_list_networks = set(n.lower()
                                for n in network_to_asn_mapping.keys())

# Create graph of just the countries of each network
countries_graph_nodes = set( (data['Network'], data['GeoLocation']) for node,
                            data in interconnect_graph.nodes_iter(data=True))
countries_graph = nx.Graph()
for (network, location) in countries_graph_nodes:
    # Find average lat/lon for this network
    (lats, lons) = itertools.izip(* ((data['Latitude'], data['Longitude']) 
                                     for node, data in
                                     interconnect_graph.nodes_iter(data=True)
                                     if ('Network' in data 
                                         and data['Network'] == network 
                                         and 'Latitude' in data
                                         and 'Longitude' in data)))
    mean_lat = sum(lats) / len(lats)
    mean_lon = sum(lons) / len(lons)
    countries_graph.add_node(network, GeoLocation=location,
                             label = network,
                             Latitude=mean_lat, Longitude=mean_lon)

nx.write_gml(countries_graph, output_path + os.sep + "countries_graph.gml")

# Add Geant traceroutes
geant_nodes = dict( (d['label'], n)
                       for n,d in peering_graph.nodes_iter(data=True) 
                       if d['Network'] == "GEANT")
# Process traceroutes
print "total node count: %s" % node_total
# Remove ID from each node as this is written by default and these clobber in
# file output 

interconnect_graph.name = "Europe Interconnect"
for n in interconnect_graph.nodes_iter():
    del interconnect_graph.node[n]['id'] 

print set(d['Network'] for n, d in peering_graph.nodes(data=True))
peering_graph.name = "Europe Peering Interconnect"
for n in peering_graph.nodes_iter():
    if 'id' in peering_graph.node[n]:
        del peering_graph.node[n]['id']
       

#TODO: justify each of these with reasons
#TODO: list each of these with the network they are being removed from
#TODO: put all of this into neat summary table

print "================ Filtered External Nodes by Category  ================"
# List the peering nodes to manually sort them into NREN, IXP or Commercial
peering_nodes =  (sorted(set(d['label'] for n, d in 
                       peering_graph.nodes_iter(data=True) if 'label' in d)))
peering_nodes = list(peering_nodes)

peering_ixps = set(['AMS-IX', 'AMSIX', 'CyIX Cypress ISPs', 'GBLX', 
'GRIX', 'IIX', 'Kharkiv-IX', 
                'LINX', 'DeCix', 'CIXP',
                'MIX', 'NAMEX', 'NIX', 'Odessa-IX', 'RIX', 'SFINX', 'SIX', 
                'SwissIX', 'TIX', 'TO-PIX', 'UA-IX', 'VIX', 'VSIX'])
peering_commercial = set(['Google', 'Internet mondial', 'Swisscom', 'Level3', 
                          'GTS',
                      'TELIA', 'StarNet ISP', 'Global Crossing', 'Internet',
                      'INTERNET', 'Internet via Bruxelles (BE)',
                      'Telekom Serbia', 'KyprosNet', 'BG Internet',
                      'Private Peering with ISPs',  'Telekom', 'Telia',
                      # TODO: check what GC (from DFN) is 
                      'GC', "ONnet NEOTEL SONET", "MOL",
                      "CyIX Cypress ISPs",
                      'Global Transit to the World'])
peering_research = set(['CERN', 'Univ. of Cyprus', 'Academy of Sciences'])
peering_misc = set(["External Users", 'Mayotte, Guadeloupe, Martinique, Guyane',
                'Outre Mer: Nouvelle Caledonie, Polynesie Francaise, La Reunion',
                "Int'l backup", 'Grid', 'Grid-SW', 'Svizzera',                ])
peering_ext_nren = set(['Syria NREN',
                    # http://www.belwue.de/
                    'BelWue', 'EUMED CONNECT',
                    'ICELINK: Greenland, Canada, USA', 'Amsterdam GLORIAD',
                    'BIHarnet',
                   ])
peering_countries = set(['Belarus', 'Russia', 'Lithuania', 'France',
                         'Bosnia and Herzegovina', 'London',
                         'New York', 'St Petersburg'
                        ])

# store the nodes connected to above
peering_nodes_categorised = {
    'ixp': defaultdict(list),
    'commercial': defaultdict(list),
    'research': defaultdict(list),
    'misc': defaultdict(list),
    'ext_nren': defaultdict(list),
    'countries': defaultdict(list),
}

for node, data in peering_graph.nodes(data=True):
    if data['label'] in peering_ixps:
        peering_nodes_categorised['ixp'][data['label']].append(node)
    elif data['label'] in peering_commercial:
        peering_nodes_categorised['commercial'][data['label']].append(node)
    elif data['label'] in peering_research:
        peering_nodes_categorised['research'][data['label']].append(node)
    elif data['label'] in peering_misc:
        peering_nodes_categorised['misc'][data['label']].append(node)
    elif data['label'] in peering_ext_nren:
        peering_nodes_categorised['ext_nren'][data['label']].append(node)
    elif data['label'] in peering_countries:
        peering_nodes_categorised['countries'][data['label']].append(node)


# And list the nodes connected to these
for category, data in peering_nodes_categorised.items():
    print category.title()
    for ext_node, nodes in sorted(data.items()):
        print ext_node + ": " +  ", ".join( peering_graph.node[n]['Network']
            for n in nodes)
    print "----------"

# and for paper
for category, data in sorted(peering_nodes_categorised.items()):
    print "%s & % s\\\\" % (category.title(), len(data))
print

#TODO:
#note some may connect using IXPs
#and also note that some may specify country rather than the NREN name 

# Keep nodes that don't have one of the above
peering_labels_to_exclude = (peering_ixps | peering_commercial |
                                peering_countries | peering_research | 
                                peering_misc | peering_ext_nren)

# Map labels to node ids that match the label
peering_nodes_to_keep = [n for n, d in peering_graph.nodes_iter(data=True)
                         if 'label' in d and 
                         d['label'] not in peering_labels_to_exclude]
#print ", ".join(d['label'] for n, d in peering_graph.nodes(data=True) if n in
#                peering_nodes_to_keep)

peering_nodes_to_remove = [n for n,d in peering_graph.nodes_iter(data=True) 
                           if (not d['Internal']
                               and n not in peering_nodes_to_keep) ] 

peering_graph.remove_nodes_from(peering_nodes_to_remove)

# Remove single edge nodes (mainly those from Geant graph)
peering_graph.remove_nodes_from([n for n in peering_graph.nodes_iter() 
                                if peering_graph.degree(n) == 0])

# And list the networks of internal nodes
print "================== NREN Egress Points  =================="
for node, data in sorted(peering_graph.nodes_iter(data=True)):
    if 'Internal' in data and data['Internal'] == 1:
        #print "%s %s" % (data['label'], data['Network'])
        pass 

geant_peering_nodes = []
nren_peering_nodes = []
for node, data in peering_graph.nodes_iter(data=True):
    if 'Internal' in data and data['Internal'] == 1:
        for neigh in peering_graph.neighbors(node):
            if 'GEANT' in peering_graph.node[neigh]['label'].upper():
                geant_peering_nodes.append(node)
                geant_peering_nodes.append(neigh)
            else:
                nren_peering_nodes.append(node)
                nren_peering_nodes.append(neigh)
        # Relabel peering nodes to include origin
        #peering_graph.node[node]['label'] = "%s (%s)"%(data['label'],
        #                                               data['Network'])
print "---------------------------------------------"
geant_peering_graph = nx.subgraph(peering_graph, geant_peering_nodes) 

# And compare to traceroutes
#TODO: Note that need to handle the differences more cleanly
trace_merged = {}
# All of UofA trace
for network, (geant_pop, remote_pop) in trace_uofa.items():
    trace_merged[network] = (geant_pop, remote_pop)
# Include the traces not shown up in UofA
# Only include the geant_pop from the (geant_pop, remote_pop) tuple
trace_merged['NORDUnet'] = trace_lboro['NORDUnet']
# use simplified form of name
#trace_merged['AARNIEC/RoEduNet'] = trace_lboro['AARNIEC/RoEduNet']
trace_merged['RoEduNet'] = trace_lboro['AARNIEC/RoEduNet']

print "================== Possible Geant Peering Connections  =================="
possible_direct_geant_peer_networks = set()
for node, data in sorted(geant_peering_graph.nodes(data=True),
                         key = lambda info: info[1]['Network']):
    if data['Internal']:
        for neigh in geant_peering_graph.neighbors(node):
            node_name = data['label']
            node_network = data['Network']
            possible_direct_geant_peer_networks.add(node_network)
            link_data = geant_peering_graph.get_edge_data(node, neigh)
            
            print '("%s", "%s") -> %s' % (
                node_network, node_name,
                geant_peering_graph.node[neigh]['label']) 
            # print link_data
            # print
        # See if match from traceroutes 

print "Possible networks: %s" % (
    ", ".join(sorted(possible_direct_geant_peer_networks)))

print "==================== Geant Traceroutes (from UofA)  ===================="
for network in sorted(trace_merged):
    # Show the matched traceroute
    # get the GEANT PoP eg bud.hu -> HU
    geant_pop_dns, remote_pop = trace_merged[network]

    geant_pop = geant_pop_dns.split(".")[1].upper()
    #print "%s -> %s (merged trace)"%(geant_pop, network)
    # Print suggested connect for whole network
    # And connect
    # Find GEANT PoP
    geant_pop_id = [n for n, d in
                    interconnect_graph.nodes(data=True)
                    if (d['label'] == geant_pop
                        and d['Network'] == "GEANT")][0]
    # Display as ('AMRES', 'Sabac') not (u'AMRES', u'Sabac') ie no u
    # For easy copy-paste into Python data structure

    print '("GEANT", "%s") -> %s (%s)' % (geant_pop, network, remote_pop)
    #interconnect_graph.add_edge(inter_id, geant_pop_id)

#TODO: be more explicit with the traceroute results
# TODO: rerun traceroutes on routers inside the network, not just the domain
# names
#TODO: add university of Malta
geant_interconnect_list = [
    #TODO: SPLIT UP, and list origins at end in third tuple
    # ----- traceroutes

    #TODO: State origin in tuple - if from map, tracert, or website

    #==== Router PoPs ===
    #Fully Featured AT, CH, CZ, DE, DK, ES, FR, HU, IT, NL, UK

    # Source for each direction
    # TM = Topology Map
    # TR = Traceroute

    # ACOnet topology map
    # ("ACOnet", "Vienna1") -> GEANT
    # ("ACOnet", "Vienna2") -> GEANT
    # And traceroute: 
    # 15  aconet-gw.rt1.vie.at.geant2.net (62.40.124.2)  321.762 ms
    # 16  vlan73.wien21.aco.net (193.171.23.41)  319.594 ms
    (("ACOnet", "Vienna1"), ("GEANT", "AT")),
    (("ACOnet", "Vienna2"), ("GEANT", "AT")),
    # Cesnet Topology Map
    #  ("CESNET", "Praha") -> GEANT
    # Traceroute:
    # 15  cesnet-gw.rt1.pra.cz.geant2.net (62.40.124.30)  326.377 ms
    # 16  www.cesnet.cz (195.113.144.230)  326.983 ms !Z
    (("CESNET", "Praha"), ("GEANT", "CZ")),
    # DFN topology map
    # ("DFN", "FRA") -> Geant
    # Traceroute: 
    # 17  dfn-gw.rt1.fra.de.geant2.net (62.40.124.34)  360.237 ms
    # 18  xr-fzk1-te2-3.x-win.dfn.de (188.1.145.50)  361.206 ms
    (("DFN", "FRA"), ("GEANT", "DE")),
    # Garr Topology Map
    # ("GARR", "MI-1") -> GEANT
    # ("GARR", "MI-2") -> GEANT
    # Traceroute
    # 14  garr-lb2-gw.rt1.mil.it.geant2.net (62.40.120.110)  307.250 ms
    # 15  rt1-mi1-rt-mi2.mi2.garr.net (193.206.134.190)  307.493 ms
    (("GARR", "MI-1"), ("GEANT", "IT")),
    (("GARR", "MI-2"), ("GEANT", "IT")),
    # GRNET Topology Map
    # ("GRNET", "Athens") -> GEANT
    # Traceroute
    # 16  grnet-gw.rt1.ath2.gr.geant2.net (62.40.124.90)  358.941 ms
    # 17  kol1-to-eie2.backbone.grnet.gr (195.251.27.53)  359.396 ms
    (("GRNET", "Athens"), ("GEANT", "GR")),
    # Janet Topology Map (from Janet External Connection Map)
    # States from JANET to GEANT, not the Janet PoP
    # 14  janet-gw.rt1.lon.uk.geant2.net (62.40.124.198)  306.256 ms
    # 15  ae12.lond-sbr1.ja.net (146.97.33.137)  302.690 ms
    (("JANET", "London") , ("GEANT", "UK")),
    # NIIF Topology Map
    #("NIIF", "Budapest") -> GEANT
    # Traceroute
    # 15  so-3-0-0.rt1.bud.hu.geant2.net (62.40.112.14)  334.571 ms
    # 16  be2.rtr1.vh.hbone.hu (195.111.96.60)  334.923 ms
    (("NIIF", "Budapest"), ("GEANT", "HU")),
    # Topology Map
    # ("NORDUnet", "Copenhagen") -> GEANT
    # Traceroute 
    # 10  nordunet-gw.rt2.cop.dk.geant2.net (62.40.124.46)  26.595 ms
    # 11  se-fre.nordu.net (109.105.97.5)  36.147 ms
    (("NORDUnet", "Copenhagen"), ("GEANT", "DK")),
    (("PSNC", "Poznan"), ("GEANT", "PL")),
    # Topology Map
    # ("RENATER", "Paris") -> Geant2
    # 8  as0.rt1.par.fr.geant2.net (62.40.112.105)  12.853 ms
    # 9  renater-gw.rt1.par.fr.geant2.net (62.40.124.70)  13.364 ms
    # 10  *
    (("RENATER", "Paris"), ("GEANT", "FR")),
    # Topology Map
    #("SWITCH", "CERN") -> GEANT2
    #Traceroute
    #10  switch-lb2-gw.rt1.gen.ch.geant2.net (62.40.124.106)  21.714 ms
    #11  swice3-10ge-1-4.switch.ch (130.59.36.210)  21.894 ms
    (("SWITCH", "CERN"), ("GEANT", "CH")),
    ##### More difficult cases
    #     # assume same city
    # Nothing on topology map
    # Traceroute 
    # 18  surfnet-gw.rt1.ams.nl.geant2.net (62.40.124.158)  365.351 ms
    # 19  ae2.500.jnr01.asd001a.surf.net (145.145.80.78)  365.429 ms
    # 20  v1131.sw4.amsterdam1.surf.net (145.145.19.170)  365.314 ms
    (("SURFnet", "Amsterdam"), ("GEANT", "NL")),
    # Nothing on topology map for RedIris
    #     # Nacional appears to be main POP for Spain
    #Traceroute
    #12  rediris-gw.rt1.mad.es.geant2.net (62.40.124.54)  278.321 ms
    #13  xe3-0-0-264.eb-iris6.red.rediris.es (130.206.206.133)  277.958 ms
    (("RedIris", "Nacional"), ("GEANT", "ES")),


    #Off fibre net GR, PL

    #IP/MPLS only BG, EE, LT, LV, RO
    # Topology Map
    #("LITNET", "Kaunas") -> GEANT
    #Traceroute
    #12  litnet-gw.rt1.kau.lt.geant2.net (62.40.125.162)  47.281 ms
    #13  193.219.153.21 (193.219.153.21)  47.740 ms
    # 
    (("LITNET", "Kaunas"), ("GEANT", "LT")),
    #RO
    # Topology Map
    #("RoEduNet", "Bucaresti") -> GEANT
    #13  roedunet-gw.rt1.buc.ro.geant2.net (62.40.125.138)  48.692 ms
    #14  te-0-1-0-0.core1.nat.roedu.net (89.37.13.2)  49.265 ms
    (("RoEduNet", "Bucaresti"), ("GEANT", "RO")),
    # BG
    #Topology Map
    #("BREN", "IPP-BAS") -> GEANT
    #13  bren-gw.rt1.sof.bg.geant2.net (62.40.125.142)  50.691 ms
    #14  www.bren.bg (194.141.251.17)  50.469 ms
    (("BREN", "IPP-BAS"), ("GEANT", "BG")),
    #EE
    #Topology Map
    #("EENet", "Tallinn") -> GEANT2
    #Traceroute
    #11  eenet-bckp-gw.rt2.tal.ee.geant2.net (62.40.124.50)  39.839 ms
    #12  trt-fe.bb.eenet.ee (193.40.133.6)  42.989 ms
    (("EENet", "Tallinn") , ("GEANT", "EE")),
    # LV
    #Topology Map
    #("SigmaNet", "Riga") -> GEANT2
    #Traceroute
    #NOTE THIS IS A DISCREPANCY
    #http://lg.ls.lv/lg/ to a GEANT IP shows
    #5 te1-1.200.stk.globalcom.lv (85.254.1.241) 16 msec 12 msec 16 msec
    #6 s-b3-link.telia.net (213.248.66.153) 16 msec 17 msec 16 msec
    #7 s-bb1-link.telia.net (80.91.249.221) 12 msec
    #8 kbn-bb2-link.telia.net (213.155.130.175) 20 msec 20 msec
    #9 kbn-b4-link.telia.net (80.91.253.245) 24 msec
    #10 dante-ic-125712-kbn-b2.c.telia.net (213.248.97.146) 24 msec 24 msec 20 msec
    #11 so-7-3-0.rt1.fra.de.geant2.net (62.40.112.49) 36 msec 40 msec 40 msec
    # Assume goes via LATNET diagram displayed exit point to GEANT PoP in the
    # city
    (("SigmaNet", "Riga"), ("GEANT", "LT")),
    #==== Routerless PoPs ===

    #Routerless Off Fibre net LU, PT, RU
    # Routerless -> Use local PoP
    # And tag edge appropriately
    # LU
    # Topology Map
    #("RESTENA", "BCE") -> GEANT Frankfurt (DE)
    #("RESTENA", "BCE") -> GEANT Paris (FR)
    #Traceroute 
    #14  restena-gw.rt1.fra.de.geant2.net (62.40.125.114)  334.316 ms
    #15  gate-2-te-5-4.bce.restena.lu (158.64.16.62)  333.620 ms
    ## GEANT Topology Map shows FR <-> LU <-> DE links
    (("RESTENA", "BCE"), ("GEANT", "DE")),
    (("RESTENA", "BCE"), ("GEANT", "FR")),
    # PT
    # No external connection info for FCCN topology map
    # traceroute
    # 12  fccn-gw.rt1.mad.es.geant2.net (62.40.124.98)  303.099 ms
    # 13  router3.10ge.lisboa.fccn.pt (193.137.0.27)  316.122 ms
    # GEANT Topology Map shows UK <-> PT <-> ES links
    (("FCCN", "Lisboa"), ("GEANT", "ES")),
    (("FCCN", "Lisboa"), ("GEANT", "UK")),
    # RU
    # Listed as associate member, but shown on GEANT topology map
    # No network topology information for JSCC
    # Traceroute
    #10  jscc-gw.rt1.fra.de.geant2.net (62.40.125.190)  72.320 ms
    #11  m9-6k-vl108.rasnet.ru (194.190.37.34)  233.648 ms
    # GEANT Topology Map Shows DK <-> RU <-> DE links
    # No topology map for JSCC, assume single node (University only)
    # Traceroute confirms
    # 10  jscc-gw.rt1.fra.de.geant2.net (62.40.125.190)  72.320 ms
    #11  m9-6k-vl108.rasnet.ru (194.190.37.34)  233.648 ms
    (("JSCC", "Moscow"), ("GEANT", "DK")),
    (("JSCC", "Moscow"), ("GEANT", "DE")),

    #Routerless BE, HR, IE, SK, SL
    # BE
    # Belnet topology map shows no external connections
    # GEANT Topology map shows UK <-> BE <-> NL links
    # Need to determine Belnet exit point - see below
    # see below for discussion of Evere 
    # This one needs confirmation from  tracert
    # even from lboro:
    # 9  belnet-gw.rt1.ams.nl.geant2.net (62.40.124.162)  16.917 ms
    # 10  10ge.cr1.brueve.belnet.net (193.191.16.21)  13.802 ms
    # This is consistent with GEANT deliverable document stating BE in Brussels
    (("BELnet", "Evere"),  ("GEANT", "NL")),
    # Assume other link from same PoP
    (("BELnet", "Evere"),  ("GEANT", "UK")),
    # IE
    # No external connection info on HEAnet topology map
    # Traceroute from GEANT
    # 14  heanet-gw.rt1.lon.uk.geant2.net (62.40.125.126)  312.795 ms
    #15  te5-1-blanch-sr1.services.hea.net (193.1.236.2)  312.331 ms
    # Traceroute from hea.net looking glass
    #http://www.hea.net/cgi-bin/lg.cgi 
    #from Router: Citywest - ar3-cwt.hea.net  to a GEANT IP (London PoP IP)
    #1 gi0-12-0-2-cr2-cwt.hea.net (193.1.238.229) 4 msec 0 msec 0 msec
    #2 as0.rt1.lon.uk.geant2.net (62.40.112.106) [AS 20965] 12 msec 12 msec 8 msec
    #and also from other looking glass router
    #Router: Blanchardstown - blanch-sr1.services.hea.net 
    #1 te0-8-0-1-cr2-cwt.hea.net (193.1.236.1) [AS 1213] 4 msec 4 msec 0 msec
    #2 as0.rt1.lon.uk.geant2.net (62.40.112.106) [AS 1213] 12 msec 12 msec 12 msec
    # GEANT Topology Map shows two connections from IE <-> UK
    # Assume these both go from the same HEAnet PoP
    # This is consistent with GEANT deliverable document stating IE PoP in Dublin
    (("HEAnet", "CityWest"), ("GEANT", "UK")),
    # HR
    # CARNet topology map
    # ("CARNet", "Zagreb") -> GEANT
    #("CARNet", "Zagreb") -> GEANT
    # This is consistent with GEANT deliverable document stating HR PoP in Zagreb
    #traceroute
    #15  carnet-gw.rt1.vie.at.geant2.net (62.40.124.10)  329.089 ms
    #16  cn-srce-02-ro.core.carnet.hr (193.198.238.106)  329.009 ms
    #GEANT Topology map shows two links from CARNet
    #SI <-> HR <-> HU
    #However note that trace appears from AT node in GEANT
    #This is because SI is a routerless PoP
    #Thus we need to add a link from egress PoP in CARNet to external connected PoP
    #in ARNES (Slovenia). This is done just below in the SI PoP bit
    #This is consistent with the two GEANT connections shown on CARNet topology map
    (("CARNet", "Zagreb"), ("GEANT", "HU")),
    #SK
    (("SANET", "Bratislava"), ("GEANT", "AT")),
    # SI
    # Traceroute doesn't give a lot
    # 14  so-6-3-0.rt1.vie.at.geant2.net (62.40.112.17)  334.560 ms
    # 15  rarnes1-x0-3-0x0.arnes.si (62.40.124.6)  353.860 ms
    # 16  lljtpl1-v121.arnes.si (212.235.160.209)  326.253 ms
    # ARNES states "The two most important PoPs are located in Ljubljana"
    # on http://www.arnes.si/en/infrastructure/network-infrastructure/pops.html
    # Additionally, GEANT deliverable document states GEANT SI PoP in Ljubljana
    (("ARNES", "Ljubljana"), ("GEANT", "AT")),
    # And also add the GEANT link from SI to HR
    # Note this is an NREN to NREN connection but through GEANT link
    (("ARNES", "Ljubljana"), ("CARNet", "Zagreb")),

    #==== NREN PoPs ===
    #NREN POPs CY, IL, ME, MK, MT, RS, TR
    # CY
    # CYNET topology map shows:
    # ("CYNET", "Border Router") -> GEANT Milano
    # ("CYNET", "Border Router") -> GEANT Athens
    # GEANT topology map shows connections to GR and IT
    # Traceroute however shows via london:
    # 8  cynet-ap1-gw.rt1.lon.uk.geant2.net (62.40.124.166)  83.134 ms
    # 9  lef-eleni.ptp.cynet.ac.cy (82.116.192.18)  83.545 ms
    # CHOICE: choose the links stated on both CYNET and GEANT topology maps
    (("CYNET", "Border Router"), ("GEANT", "GR")),
    (("CYNET", "Border Router"), ("GEANT", "IT")),
    # IL
    # IUCC topology map shows 
    # ("IUCC", "Petach Tikva GigaPoP") -> GEANT 2 Germany
    # GEANT topology map shows IT <-> IL <-> UK
    # CHOICE: Choose the external node from IUCC, but use the links from GEANT
    # (assumed to be more authoritative)
    # Traceroute 
    #7  janet.rt1.lon.uk.geant2.net (62.40.124.197)  5.611 ms
    #8  iucc-lb2-gw.rt1.lon.uk.geant2.net (62.40.125.194)  81.584 ms
    #9  *
    (("IUCC", "Petach Tikva GigaPoP"), ("GEANT", "DE")),
    (("IUCC", "Petach Tikva GigaPoP"), ("GEANT", "UK")),

    # ME
    # No topology map information for MREN, assume largest central PoP
    # especially due to source document stating
    # "The link between Belgrade (AMRES) and Podgorica (MREN) consists 
    # currently of 2 x 2 Mb/s (2 E1) lines, and will be soon upgraded
    # through the SEEREN2 project to 34Mb/s. "
    # GEANT topology map shows link from ME to HR.
    # HR is routerless, so connect to the externally connected PoP in HR
    # ie ("CARNet", "Zagreb")
    # Traceroute 
    # 11  so-2-0-0.rt1.bud.hu.geant2.net (62.40.112.42)  36.269 ms
    # 12  mren-gw.rt1.bud.hu.geant2.net (62.40.125.210)  66.857 ms
    # 13  *
    (("MREN", "Podgorica"), ("CARNet", "Zagreb")),

    #MK
    # MARNet topology map shows
    # ("MARNet", "NOC and CampusSocial Sciences") -> GEANT SEEREN
    # GEANT topology map shows connection from MK to BG
    # Confirmed by traceroute
    # 13  marnet-gw.rt1.sof.bg.geant2.net (194.149.130.109)  54.964 ms
    # 14  vl12-msw.ukim.mk (194.149.130.1)  55.218 ms
    (("MARNet", "NOC and CampusSocial Sciences"), ("GEANT", "BG")),

    #MT
    # No topology map for Malta, assume single node (University only)
    # GEANT topology map shows connection from IT to MT
    # Traceroute confirms
    # 11  malta-gw.rt1.mil.it.geant2.net (62.40.124.234)  60.850 ms
    # 12  l-universita-ta-malta.um.edu.mt (193.188.32.254)  60.724 ms
    (("Malta", "University of Malta"), ("GEANT", "IT")),

    #RS
    # AMRES network map states no connection to GEANT
    # but they specify connection to Hungarnet from Subotica
    # NIIF (Hungary) shows connection from Szeged to Szabadka
    # Szabadka is Hungarian name for Subotica
    # GEANT topology map shows link from RS to HU, confirming this
    # Traceroute shows commercial links.
    # Thus best available is to use the NREN specified link
    # Even though it likely shows a lower-layer connection
    (("NIIF", "Szabadka"), ("AMRES", "Subotica")),

    # TR
    # ULAKNET topology map shows
    #("ULAKNET", "Istanbul") -> GEANT
    #GEANT topology map shows BG <-> TR <-> RO
    # BG and RO are router PoPs, connect
    # Traceroute:
    # 17  ulakbim-lb1-gw.rt1.buc.ro.geant2.net (62.40.125.154)  357.367 ms
    # 18  193.140.0.149 (193.140.0.149)  367.141 ms
    (("ULAKBIM", "Istanbul"), ("GEANT", "BG")),
    (("ULAKBIM", "Istanbul"), ("GEANT", "RO")),

    #NORDUnet FI, IS, NO, SE
    # connected in seperate section
    
    # UA
    # Associate partner of GEANT, not shown on topology map
    # URAN topology map shows 
    # ('URAN', 'Kiev'): Kiev (URAN) -> Frankfurt (Internet)
    # But this is inconclusive
    # traceroute 
    # 15  uran-gw.rt1.poz.pl.geant2.net (62.40.124.250)  369.226 ms
    #16  ge0-1798.kvr1.uran.net.ua (212.111.192.23)  353.483 ms
    (("URAN", "Lviv"), ("GEANT", "PL")),

    # BASNET also associate partner of GEANT
    # BASNET topology map states 
    # ("BASNET", "Minsk") -> Pionier GEANT 2
    # Nothing on GEANT topology map
    # Traceroute
    #15  basnet-gw.rt1.poz.pl.geant2.net (62.40.125.82)  346.786 ms
    #16  c7604-ge1-2.bas-net.by (80.94.160.109)  345.651 ms
    (("BASNET", "Minsk"), ("GEANT", "PL")), 

] 

""" more on bren:

# From traceroute
#15  belnet-gw.rt1.ams.nl.geant2.net (62.40.124.162)  337.807 ms
#16  10ge.cr1.brueve.belnet.net (193.191.16.21)  321.151 ms 

    http://www.belnet.be/en/node/63
Optical ring is expanded in Brussels

Since April 2008 BELnet manages its own optical infrastructure consisting of 
approximately 1600 km fiber. The optical network consists of four rings, 
the smallest ring (those in Brussels) by PoPs BRUCAM (VUB-ULB), 
BRUSCI (BELnet offices), BRUVIL (Belgacom data center) 
and BRUEVE (Level3 data center) runs.

and from
http://www.belnet.be/en/news/new-belgian-national-internet-exchange

" One switch is in the Level3 data center in Ever"

so BRUEVE appears to be BRUssels EVEre
"""
# Replace non geant pops in geant network with their NREN equivalent
# so retain same structure of full geant map but with NREN pops where
# appropriate
#==================== ====================

MT_data = [d for n, d in geant_pop_graph.nodes(data=True)
           if d['label'] == 'MT']
malta_lat = MT_data[0]['Latitude'] 
malta_lon = MT_data[0]['Longitude'] 
interconnect_graph.add_node(next_id.next(), Latitude = malta_lat,
                            Longitude = malta_lon, label="University of Malta",
                            Network = "Malta",
                            asn = network_to_asn_mapping['University of Malta'])
RU_data = [d for n, d in geant_pop_graph.nodes(data=True)
           if d['label'] == 'RU']
ru_lat = RU_data[0]['Latitude'] 
ru_lon = RU_data[0]['Longitude'] 
interconnect_graph.add_node(next_id.next(), Latitude = ru_lat,
                            Longitude = ru_lat, label="Moscow",
                            Network = "JSCC",
                            asn = network_to_asn_mapping['JSCC'])

print "================= Geant Topolgy Graph PoP Information ================="
geant_pop_types = set(d['type'] for n, d in
                      geant_pop_graph.nodes_iter(data=True))
for pop_type in geant_pop_types:
    print pop_type
    print ", ".join(sorted(d['label'] 
                           for n, d in geant_pop_graph.nodes_iter(data=True)
                           if d['type'] == pop_type))

# Now append the NREN PoPs and relevant connections from geant graph
print "GEANT NREN PoPS:"

"""
# neighbours of each PoP
for node, d in geant_pop_graph.nodes(data=True):
        # Check GEANT neighbours
        for neigh in geant_pop_graph.neighbors(node):
            print '%s %s -> ("GEANT", "%s")' % (d['label'],
                                             d['Country'],
                                             geant_pop_graph.node[neigh]['label'])
"""

print "---------------------------------------------"
print "GEANT Routerless PoP Neighbors:"
for node, d in geant_pop_graph.nodes(data=True):
    if d['type'] == 'Routerless':
        # Check GEANT neighbours
        for neigh in geant_pop_graph.neighbors(node):
            print '%s %s -> ("GEANT", "%s")' % (d['label'],
                                             d['Country'],
                                             geant_pop_graph.node[neigh]['label'])
print "---------------------------------------------"
print "GEANT Routerless Off Fibre net PoP neighbors:"
for node, d in geant_pop_graph.nodes(data=True):
    if d['type'] == 'Routerless Off Fibre net':
        # Check GEANT neighbours
        for neigh in geant_pop_graph.neighbors(node):
            print '%s %s -> ("GEANT", "%s")' % (d['label'],
                                             d['Country'],
                                             geant_pop_graph.node[neigh]['label'])
print "---------------------------------------------"
   
pprint.pprint([ (n,d) for n,d in interconnect_graph.nodes(data=True)
              if 'label' not in data])

def name_network_to_id(name, network):
    matches = [n for n, d in interconnect_graph.nodes_iter(data=True)
               if d['label'] == name and d['Network'] == network]
    if len(matches):
        return matches[0]
    else:
        print "No match for %s (%s)" % (name, network)

for src, dst in geant_interconnect_list:
    #print "connecting " + str(src) + " to " + str(dst)
    #TODO: don't add edge from the external node
    src_net, src_name = src
    dst_net, dst_name = dst 
    src_id = name_network_to_id(src_name, src_net)
    dst_id = name_network_to_id(dst_name, dst_net)
    interconnect_graph.add_edge(src_id, dst_id)

print "==================== Geant Connections ===================="
for src, dst in sorted(geant_interconnect_list,
                       # Sorted by the GEANT POP
                       key = lambda (src, dst): (dst[1], src[0], src[1])):
    print "%s & %s & %s\\\\" % (dst[1], src[0], src[1]) 

geant_peering_graph.graph = {'Network': 'Geant peering graph'}
nren_peering_graph = nx.subgraph(peering_graph, nren_peering_nodes)

print "==================  Possible NREN Peering Connections  ================="
for node, data in sorted(nren_peering_graph.nodes(data=True),
                         key = lambda info: info[1]['Network']):
    if data['Internal']:
        for neigh in nren_peering_graph.neighbors(node):
            if 'label' in nren_peering_graph.node[node]:
                # Display as ('AMRES', 'Sabac') not (u'AMRES', u'Sabac') ie no u
                # For easy copy-paste into Python data structure
                node_name = data['label']
                node_network = data['Network']
                print "('%s', '%s'): %s (%s) -> %s" % (
                    node_network, node_name,
                    nren_peering_graph.node[node]['label'],
                    nren_peering_graph.node[node]['Network'],
                    nren_peering_graph.node[neigh]['label']) 

nren_peering_graph.graph = {'Network': 'NREN peering graph'} 

print "====================NREN to NREN Connections===================="
# Interconnects from peering nodes
peering_interconnect_list = [
    # aconet - cesnet
    # ACOnet topology map
    #('ACOnet', 'Vienna1'): Vienna1 (ACOnet) -> CESNET
    #('ACOnet', 'Vienna2'): Vienna2 (ACOnet) -> CESNET
    # CESNET topology map
    #('CESNET', 'Brno'): Brno (CESNET) -> SANET
    (('ACOnet', 'Vienna1'), ('CESNET', 'Brno')),
    (('ACOnet', 'Vienna2'), ('CESNET', 'Brno')),

    # aconet - sanet
    # ACOnet topology map
    #('ACOnet', 'Vienna1'): Vienna1 (ACOnet) -> SANET
    # ('ACOnet', 'Vienna2'): Vienna2 (ACOnet) -> SANET
    # SANET topology map
    #('SANET', 'Vieden'): Vieden (SANET) -> ACOnet
    (('ACOnet', 'Vienna1'), ('SANET', 'Vieden')),
    (('ACOnet', 'Vienna2'), ('SANET', 'Vieden')),

    # cesnet - sanet
    # Cesnet topology map
    # ('CESNET', 'Brno'): Brno (CESNET) -> SANET
    # SANET topology map
    # ('SANET', 'Brno'): Brno (SANET) -> CESNET
    (('CESNET', 'Brno'), ('SANET', 'Brno')),
    # Also have 
    # ('CESNET', 'Ostrava'): Ostrava (CESNET) -> PSNC
    # but this is ambigious as to where connects in PSNC

    # cesnet - pionier
    # Cesnet topology map
    # ('CESNET', 'Ostrava'): Ostrava (CESNET) -> PSNC
    # PSNC topology map
    # ('PSNC', 'Bielsko-Biala'): Bielsko-Biala (PSNC) -> CESNET
    (('CESNET', 'Ostrava'), ('PSNC', 'Bielsko-Biala')),

    # sanet - pionier
    # SANET topology map
    # ('SANET', 'Bielsko-Biala'): Bielsko-Biala (SANET) -> PSNC
    # PSNC topology map
    # ('PSNC', 'Bielsko-Biala'): Bielsko-Biala (PSNC) -> SANET
    (('SANET', 'Zilina'), ('PSNC', 'Bielsko-Biala')),
    # also ('SANET', 'Zilina'): Zilina (SANET) -> PSNC
    # ignore as don't know end point conclusively

    # PSNC topology map
    # ('PSNC', 'Poznan'): Poznan (PSNC) -> SURFnet/NORDUnet
    # NORDUnet topology map
    # ('NORDUnet', 'Hamburg'): Hamburg (NORDUnet) -> Poznan
    # Confirmed by "There was also deployed 2x10Gbit/s DWDM transmission system
    # from Poznan through Slubice to Hamburg (Germany) and connects directly
    # with SURFnet and NORDunet. " from 
    # http://www.man.poznan.pl/online/en/projects/69/PIONIER_Network.html
    # Assume IP layer connection from Poznan to Hamburg
    (('PSNC', 'Poznan'), ('NORDUnet', 'Hamburg')),

    # Roedunet - renam
    # RENAM is an associate partner of GEANT
    # Roedunet topology map
    # ('RoEduNet', 'Iasi'): Iasi (RoEduNet) -> Chisinau RENAM
    # RENAM topology map
    # ('RENAM', 'Chisinau'): Chisinau (RENAM) -> RoEduNet
    (('RoEduNet', 'Iasi'), ('RENAM', 'Chisinau')),
] 

for src, dst in sorted(peering_interconnect_list,
                       # Sorted by the GEANT POP
                       key = lambda (src, dst): (src[0], src[1], dst[0], dst[1])
                      ):
    #print "connecting " + str(src) + " to " + str(dst)
    #TODO: don't add edge from the external node
    src_net, src_name = src
    dst_net, dst_name = dst 
    src_id = name_network_to_id(src_name, src_net)
    dst_id = name_network_to_id(dst_name, dst_net)
    interconnect_graph.add_edge(src_id, dst_id)
    print "%s & %s & %s & %s\\\\" % (src[0], src[1], dst[0], dst[1])



print "====================Nordu Connections===================="

# Interconnects from Nordu  nodes
nordu_interconnect_list = [
    (('NORDUnet', 'Hamburg'), ('PSNC', 'Poznan')),

    # And for NORDUnet
    (("NORDUnet", "Copenhagen"), ("Uni-C", "Lyngby")),
    (("NORDUnet", "Copenhagen"), ("Uni-C", "Orestad")),
    (("NORDUnet", "Stockholm"), ("SUNET", "Stockholm")),
    #TODO: what about Nordu in Oslo?????
    #TODO: check weathermap
    #(("NORDUnet", "Oslo"), ("UNINETT", "UiO ")),
    (("NORDUnet", "Copenhagen"), ('Uninett', 'UiO St Olavsplass 5')),
    # FUNET is Finland
    (("NORDUnet", "Helsinki"), ('FUNET', 'Helsinki')),
    (("NORDUnet", "Helsinki"), ('FUNET', 'Espoo')),
    # RHnet is Iceland
    (("NORDUnet", "Reyjavik"), ('RHnet', 'Taeknigarour')),
    (("NORDUnet", "Reyjavik"), ('RHnet', 'Sturlugata')),
    # Uninett is Norway
    (("NORDUnet", "Stockholm"), ('Uninett', 'UiO')),
    (("NORDUnet", "Stockholm"), ('Uninett', 'Stockholm')),
] 

for src, dst in sorted(nordu_interconnect_list,
                       # Sorted by the GEANT POP
                       key = lambda (src, dst): (src[0], src[1], dst[0], dst[1])
                      ):
    src_net, src_name = src
    dst_net, dst_name = dst 
    src_id = name_network_to_id(src_name, src_net)
    dst_id = name_network_to_id(dst_name, dst_net)
    interconnect_graph.add_edge(src_id, dst_id)
    print "%s & %s & %s & %s\\\\" % (src[0], src[1], dst[0], dst[1])


#TODO: log netx bug about deleting labels when writing GML

#========================================
# NetworkX can't write tuples to GML
geant_peering_graph = nx.convert_node_labels_to_integers(geant_peering_graph)
nx.write_gml(geant_peering_graph, 
             output_path + os.sep + "geant_peering_graph.gml")
nren_peering_graph = nx.convert_node_labels_to_integers(nren_peering_graph)
nx.write_gml(nren_peering_graph,
             output_path + os.sep + "nren_peering_graph.gml")

nx.write_gml(peering_graph, output_path + os.sep + "peering_graph.gml")

# save the graph
print "================ Zoo and ASN Networks========================"
print "Zoo networks:"
print ", ".join(network_names)
print "asn networks:"
print ", ".join(network_to_asn_mapping.keys())
print "================= Disconnected Components ======================="

#TODO: write as (graph.node[n]['label'] for n in some_list)
# rather than (d['label'] for n, d in graph.nodes(data=True) if n in some_list) 

# Identify disconnected NRENs
# take connected components, list those of networks with more than say 5 nodes
# all same
for component in nx.connected_components(interconnect_graph):
    if len(component) < 200:
        # Non-main component, no need to list all nodes
        print "Disconnected network (%s nodes):" % len(component)
        print ", ".join("%s (%s)" % (interconnect_graph.node[n]['label'],
                                   interconnect_graph.node[n]['Network'])
                        for n in component)
        #print ", ".join(set( interconnect_graph.node[n]['Network']
        #                    for n in component)) 
#TODO: set the edge_color for connections between networks to be a different
# color

#####INterconnects
#TODO: make this more efficient

#TODO: combine the traceroute file, websites, reverse dns, dns mappings, etc
# for reproducability

# remove networks that match in both

#TODO: do automatic plotting like in the graph product code calling the
# topzootools library

# Do the Network blockmodel
as_graph = nx.Graph()
as_graph.add_nodes_from(network_names)
for src, dst, data in interconnect_graph.edges_iter(data=True):
    if src and dst:
        src_net = interconnect_graph.node[src]['Network']
        dst_net = interconnect_graph.node[dst]['Network']
        if src_net != dst_net:
            as_graph.add_edge(src_net, dst_net)

nx.write_dot(as_graph, output_path + os.sep + "as_graph.dot")
nx.write_gml(as_graph, output_path + os.sep + "as_graph.gml")

print "Writing to %s" % output_path
#TODO: write to sub dir of source directory rather than zt directory
print len(interconnect_graph)
interconnect_graph.graph = {
    'Network': 'European NRENs',
    'Date': 2011,
    'GeoLocation': 'Europe',
}

#pprint.pprint(interconnect_graph.nodes(data=True))
# Make GEANT PoP nodes bigger

print "================= Setting Edge Colors ======================="

# and set edge colors
for src, dst, data in interconnect_graph.edges_iter(data=True):
    if src and dst:
        src_net = interconnect_graph.node[src]['Network']
        dst_net = interconnect_graph.node[dst]['Network']
        if src_net == dst_net == 'GEANT':
            # GEANT to GEANT link
            #interconnect_graph[src][dst]['edge_color'] = 'r'
            interconnect_graph[src][dst]['edge_color'] = '#333333'
            interconnect_graph[src][dst]['edge_width'] = 4.5
            interconnect_graph[src][dst]['zorder'] = 1.5
        if src_net == dst_net == 'NORDUnet':
            # NORDUnet to NORDUnet link
            #interconnect_graph[src][dst]['edge_color'] = '#333333'
            #interconnect_graph[src][dst]['edge_width'] = 2.5
            #interconnect_graph[src][dst]['zorder'] = 1.4
            pass
        elif ((src_net == 'GEANT' != dst_net) 
              or (src_net != 'GEANT' == dst_net)):
            # GEANT to NREN link
            interconnect_graph[src][dst]['edge_color'] = '#333333'
            interconnect_graph[src][dst]['edge_width'] = 4.5
            interconnect_graph[src][dst]['zorder'] = 1.3
        elif src_net != dst_net:
            # NREN to NREN link
            interconnect_graph[src][dst]['edge_color'] = '#333333'
            interconnect_graph[src][dst]['edge_width'] = 4.5 
            interconnect_graph[src][dst]['zorder'] = 1.2 

print "================= Creating Condensed Graph ======================="
# Create condensed graph - remove obvious edge nodes
inter_condensed = nx.Graph(interconnect_graph)
# Nodes to remove
condense_remove = {
    "ARNES": set(["Small Node"]),
    "BREN": set(["End Customer"]),
    "CESNET": set(["Black Node", "Light Blue Node", "Dark Blue Node"]),
    "CARNet": set(["Smaller Centre"]),
    "CYNET": set(["PC", "Cloud", "Cloud with Switch", "Cloud with Router"]),
    "FCCN": set(["Node"]),
    "GRNET": set(["PoP"]),
    "IUCC": set(["Purple Node"]),
    "JANET": set(["Blue Square", "Node"]),
    "LITNET": set(["PoP"]),
    "RoEduNet": set(["PoP"]),
    "ULAKBIM": set(["Yellow Colour"]),
    #TODO: check effect on JANET Dublin node wrt HEANET
    # Come back to (harder to connect/remove some nodes)
    # Amres
}

# Also keep the border router to keep connections easier
# TODO: DISCUSS CYNET WITH MATT
condense_remove_nodes = [] 

#TODO: check that GEANT should be less PoPs - remove the smaller ones 

for node, data in inter_condensed.nodes_iter(data=True):
    if (data['Network'] in condense_remove
        and 'type' in data
        and data['type'] in condense_remove[data['Network']]):
        # Faster to do bulk remove than one by one
        #TODO: check this
        condense_remove_nodes.append(node)

# Custom for Latnet, only keep those on the "100 Mbps and more" edge
# custom for marnet, only keep 3 pop nodes determined from source map
marnet_keep_nodes = set(["St Clement of Ohrid University in Bitola",
                     "F. of Mining and Geology Stip",
                     "NOC and CampusSocial Sciences"])

bren_remove_nodes = set(["Todos Kableshkov Uni",
                         "Sofia Uni.",
                         "Medical Uni Sofia",
                         "Sofia University",
                         "Economy Uni.",
                         "Technical Uni"])

aconet_condense = set(["Innsbruck1", "Innsbruck2",
                       "Salzburg1", "Salzburg2",
                       "Linz1", "Linz2",
                       "Klagenfurt1", "Klagenfurt2",
                       "Graz1", "Graz2",
                       "Vienna1", "Vienna2"])
# Keep track of the cities renamed previous nodes
# and store the ID so don't have to search for later for other part of pair
aconet_cities_kept = {} 

belnet_condense = set(["Gent1", "Gent2",
                       "Antwerpen1", "Antwerpen2",
                       "Liege1 ", "Liege2",
                       "Klagenfurt1", "Klagenfurt2",
                       "Leuven1", "Leuven2"])
# Keep track of the cities renamed previous nodes
# and store the ID so don't have to search for later for other part of pair
belnet_cities_kept = {}

nordunet_networks = set(["SUNET", 
                         #TODO: check this
                         "Uni-C",
                         "Uninett", "RHnet", "FUNET", "NORDUnet"])

#TODO: make this more template/dict based rather than repeated code

for node, data in inter_condensed.nodes_iter(data=True): 

    if data['Network'] in nordunet_networks:
        condense_remove_nodes.append(node)

    elif data['Network'] == "SigmaNet":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "100 Mbps and more" in d['LinkLabel'])
                   ]):
                   # Not on backbone
                   condense_remove_nodes.append(node)

    elif data['Network'] == "Uninett":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "10 Gbit/s" in d['LinkLabel'])
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node)

    elif data['Network'] == "NIIF":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "10Gb/s" in d['LinkLabel'])
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node) 

    elif data['Network'] == "GARR":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and 
                        ("10 Gbps" in d['LinkLabel'] 
                         or "Fibre ottica spenta (Dark Fibre)" in d['LinkLabel']
                         or "2.5 Gbps" in d['LinkLabel'])
                       )
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node)

    elif data['Network'] == "RedIris":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "10 Gbps" in d['LinkLabel'])
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node) 

    elif data['Network'] == "SWITCH":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "10 Gbps" in d['LinkLabel'])
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node) 

    elif data['Network'] == "SANET":
        #TODO: ask Matt if to apply this to main interconnect_graph also
        # Remove Fiber network connection sites nodes
        if 'type'in data and data['type'] == "Repeater":
            if inter_condensed.degree(node) == 1:
                # Just remove
                condense_remove_nodes.append(node)
            elif inter_condensed.degree(node) == 2:
                # Remove and join neighbors together
                neigh1, neigh2 = inter_condensed.neighbors(node)
                edge_data1 = inter_condensed.get_edge_data(node, neigh1)
                edge_data2 = inter_condensed.get_edge_data(node, neigh2)
                if edge_data1 != edge_data2:
                    print "EDGE data not the same in SANET"

                inter_condensed.add_edge(neigh1, neigh2, edge_data2)
                condense_remove_nodes.append(node)
            else:
                # Degree > 3
                # replace with hyperedge, ie no label, and hyperedge 1
                inter_condensed.node[node]['hyperedge'] = 1
                inter_condensed.node[node]['label'] = ""

    elif data['Network'] == "FUNET":
        #TODO: ask Matt if to apply this to main interconnect_graph also
        # Remove Fiber network connection sites nodes
        if data['type'] == "Fiber network connection sites":
            if inter_condensed.degree(node) == 1:
                # Just remove
                condense_remove_nodes.append(node)
            elif inter_condensed.degree(node) == 2:
                # Remove and join neighbors together
                neigh1, neigh2 = inter_condensed.neighbors(node)
                edge_data1 = inter_condensed.get_edge_data(node, neigh1)
                edge_data2 = inter_condensed.get_edge_data(node, neigh2)
                if edge_data1 != edge_data2:
                    print "EDGE data not the same in FUNET"

                inter_condensed.add_edge(neigh1, neigh2, edge_data2)
                condense_remove_nodes.append(node)
            else:
                # Degree > 3
                # replace with hyperedge, ie no label, and hyperedge 1
                inter_condensed.node[node]['hyperedge'] = 1
                inter_condensed.node[node]['label'] = ""

    elif data['Network'] == "Uni-C":
        #TODO: ask Matt if to apply this to main interconnect_graph also
        # Remove Fiber network connection sites nodes
        if data['type'] == "Lower speed connection":
            if inter_condensed.degree(node) == 1:
                # Just remove
                condense_remove_nodes.append(node)
            elif inter_condensed.degree(node) == 2:
                # Remove and join neighbors together
                neigh1, neigh2 = inter_condensed.neighbors(node)
                edge_data1 = inter_condensed.get_edge_data(node, neigh1)
                edge_data2 = inter_condensed.get_edge_data(node, neigh2)
                if edge_data1 != edge_data2:
                    print "EDGE data not the same in Uni-C"

                inter_condensed.add_edge(neigh1, neigh2, edge_data2)
                condense_remove_nodes.append(node)
            else:
                # Degree > 3
                # replace with hyperedge, ie no label, and hyperedge 1
                inter_condensed.node[node]['hyperedge'] = 1
                inter_condensed.node[node]['label'] = ""

    elif data['Network'] == "URAN":
        # See if has an edge on the core network link
        # Ie at least one edge in the list
        if not len([d for (src, dst, d) 
                    in inter_condensed.edges(nbunch=node, data=True)
                    if ('LinkLabel' in d 
                        and "1Gbps" in d['LinkLabel'])
                    ]):
                    # Not on backbone
                    condense_remove_nodes.append(node)

    elif data['Network'] == "MARNet":
        if data['label'] not in marnet_keep_nodes:
            condense_remove_nodes.append(node)

    elif data['Network'] == "BREN":
        # remove the ring that is also in Sofia, the MAN
        if data['label'] in bren_remove_nodes:
            condense_remove_nodes.append(node)

    elif data['Network'] == "ACOnet":
        if data['label'] in aconet_condense:
            # See which city this is
            city = data['label'][:-1]
            if city not in aconet_cities_kept:
                # Rename this node
                inter_condensed.node[node]['label'] = city
                # Store ID for other part of pair
                aconet_cities_kept[city] = node
            else:
                keeping_node = aconet_cities_kept[city]
                # Other half of pair has had data kept
                for (src, dst, data) in inter_condensed.edges(nbunch=node,
                                                              data=True):
                    if dst == keeping_node:
                        # Don't connect back to self, eg Graz1 - Graz2 becomes
                        # Graz - Graz
                        continue

                    # Add edge
                    inter_condensed.add_edge(keeping_node, dst, data)
                    condense_remove_nodes.append(node)

    elif data['Network'] == "BELnet":
        if data['label'] in belnet_condense:
            # See which city this is
            city = data['label'][:-1]
            if city not in belnet_cities_kept:
                # Rename this node
                inter_condensed.node[node]['label'] = city
                # Store ID for other part of pair
                belnet_cities_kept[city] = node
            else:
                keeping_node = belnet_cities_kept[city]
                print data['label']
                # Other half of pair has had data kept
                for (src, dst, data) in inter_condensed.edges(nbunch=node,
                                                              data=True):
                    if dst == keeping_node:
                        # Don't connect back to self, eg Graz1 - Graz2 becomes
                        # Graz - Graz
                        continue

                    # Add edge
                    inter_condensed.add_edge(keeping_node, dst, data)
                    condense_remove_nodes.append(node) 

# And remove the dual nodes from ACOnet and Belnet
# eg two nodes in same city

inter_condensed.remove_nodes_from(condense_remove_nodes)

print "total in condensed %s vs %s saving of %s " % (
    inter_condensed.number_of_nodes(),
    interconnect_graph.number_of_nodes(),
    interconnect_graph.number_of_nodes() - inter_condensed.number_of_nodes())
print "%s Gb vs %s Gb" % (
    inter_condensed.number_of_nodes()*1.0*32/1000,
    interconnect_graph.number_of_nodes()*1.0*32/1000,
)

# List network sizes
condensed_networks = [d['Network'] for n, d in inter_condensed.nodes(data=True)]
normal_networks = [d['Network'] for n, d in interconnect_graph.nodes(data=True)]
for count, name in sorted([(condensed_networks.count(name), name)
                           for name in network_names],
                          key = lambda (count, name): name):
    print "%s & %s & %s \\\\" % (name, normal_networks.count(name), count)
print "\hline"
print "Total & %s & %s \\\\" % (
    interconnect_graph.number_of_nodes(),
    inter_condensed.number_of_nodes(),
)

for network, stats in sorted(network_stats.items()):
    print "%s & %s & %s & %s & %s  \\\\" % (network,
                                                stats[0],
                                                # condensed nodes
                                                condensed_networks.count(network),
                                                stats[1],
                                                stats[2],
                                              #  stats[3],
                                               )

# Find networks without an asn
print "No ASN in node:"
for node, data in interconnect_graph.nodes(data=True):
    if 'asn' not in data:
        print "No asn for: " +  data['label'] + " " + data['Network']
    if interconnect_graph.degree(node) == 0:
        print "Disconnected node: " + data['label'] + " " + data['Network']

print "================= Creating West Europe Condensed Graph ================="
inter_condensed_west_europe = nx.Graph(inter_condensed)
west_europe_networks = set(["JANET", "DFN", "RENATER", "GEANT",
                            "BELnet", "ACOnet", "PSNC", "SURFnet",
                            "GARR", "SWITCH", "RedIris", "FCCN", "ARNES",
                            "RESTENA"])
non_west_europe_nodes = []
for node, data in inter_condensed_west_europe.nodes_iter(data=True):
    if data['Network'] not in west_europe_networks:
        non_west_europe_nodes.append(node)

inter_condensed_west_europe.remove_nodes_from(non_west_europe_nodes)
print "inter_condensed_west_europe size %s " % len(inter_condensed_west_europe)
nx.write_gml(inter_condensed_west_europe, output_path + os.sep 
             + "condensed_west_europe.gml") 

nx.write_gml(inter_condensed, output_path + os.sep + "condensed.gml")

nx.write_gml(interconnect_graph, output_path + os.sep + "interconnect.gml")

# List networks still missing

