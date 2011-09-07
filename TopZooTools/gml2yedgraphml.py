#! /usr/bin/env python

import networkx as nx
from collections import defaultdict
import os
import glob
import sys
import itertools 
import pprint as pprint
#import numpy as np

import gml as gml_custom
import time

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
#TODO: use username parameter for geonames  - command line argument,
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

opt.add_option('--csv', '-c',
            help="CSV file for metadata")

opt.add_option('--geocode', '-g',  action="store_true",
                default=False,
                help="Attempt to lookup co-ordinates")


opt.add_option('--skip_cache',  action="store_true",
                default=False,
                help="Don't use cache for geocoding")

opt.add_option('--debug',  action="store_true",
                default=False,
                help="Print debugging information to terminal")

opt.add_option('--dataset_only', action="store_true",
                default=False,
                help="Only use entries marked True in dataset column of CSV")

#TODO: change logic on tagged_only
opt.add_option('--tagged_only', action="store_true",
                default=True,
                help="Only use networks present in the CSV")

opt.add_option('--keep_graphics', action="store_true",
                default=False,
                help="Keep yEd graphics data")

#TODO: make this required if using geocoding
opt.add_option('--geonames_username', '-u', 
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

#TODO: make this a callback function for verification
if options.geocode and not options.geonames_username:
    logger.error("Please enter a Geonames username to use geocoding")
    sys.exit(0)

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

def process_shapes(graph):
    for node, data in graph.nodes(data=True):
        if "graphics" in data and "type" in data["graphics"]:
            shape = data["graphics"]["type"]
            #TODO: remove all diamonds from source files
            if shape == "diamond":
                logger.warn( "Node {0}".format(data['label']))
                logger.warn( "Warning: diamond shape no longer supported")

            if shape == "triangle":
                graph.node[node]['Internal'] = False

            #TODO: replace octagons for hyperedges with diamonds
            # As currently have overloaded the doubt and hyperedge symbols
            if shape == "octagon" or shape == "hexagon":
                # Check if hyperedge or doubted node
                if data['label'] == '':
                    graph.node[node]['hyperedge'] = True
                else:
                    # Unsure about this node
                    graph.node[node]['doubted'] = True
    
    return graph

def process_legend_keys(graph):
    legend_keys = {}
    geo_legend_keys = {}
    for node, data in graph.nodes(data=True):
        label = data['label']
        if label[:11] == "GeoLegend: ":
            # Find returns position of search string, -1 if not found
            # Format the label
            legend_key = label[11:].strip()
            # Color corresponding to this key
            color = data['graphics']['outline']
            geo_legend_keys[color] = legend_key
            # Remove the legend node from graph as not a network node
            graph.remove_node(node)
        elif label[:8] == "Legend: ":
            # Find returns position of search string, -1 if not found
            # Format the label
            legend_key = label[8:].strip()
            # Color corresponding to this key
            color = data['graphics']['fill']
            shape = data['graphics']['type']
            if shape not in legend_keys:
                legend_keys[shape] = {}
            legend_keys[shape][color] = legend_key
            # Remove the legend node from graph as not a network node
            graph.remove_node(node)

    # apply geolegend to other nodes in graph
    #toDO: see if should do this before do the {{>}} parse as that is more
    #specific match
    if len(geo_legend_keys) > 0:
        # Using legend keys, set node details appropriately
        for node, data in graph.nodes(data=True):
            if 'hyperedge' in data:
                # Don't apply color class to hyperedges
                continue
            if 'outline' not in data['graphics']:
                continue
            color = data['graphics']['outline']

            if color in geo_legend_keys:
                legend_label = geo_legend_keys[color]
                #toDO : generalise this with nbefore bit
                graph.node[node]['geocode_append'] = legend_label
            else:
                # Ignore default color FFCC00 (yellow), and external node shape
                if color != "#000000":
                    logger.warn(("Color {0} for node {1} has no geo legend "
                                "associated").format(color, data['label']))


    # Apply the legend to other nodes in graph
    if len(legend_keys) > 0:
        # Using legend keys, set node details appropriately
        for node, data in graph.nodes_iter(data=True):
            if 'hyperedge' in data:
                # Don't apply color class to hyperedges
                continue
            if 'fill' in data['graphics']:
                color = data['graphics']['fill']
            else:
                logger.warn("Node %s has no fill color" % data['label'])
            shape = data['graphics']['type']

            if shape in legend_keys and color in legend_keys[shape]:
                legend_label = legend_keys[shape][color]
                graph.node[node]['type'] = legend_label
            else:
                # Ignore default color FFCC00 (yellow), and external node shape
                if color != "#FFCC00" and shape != "triangle":
                    logger.warn(("Color {0} for node {1} has no legend "
                                "associated").format(color, data['label']))

    return graph

def graphml_graphics(in_file, graph):
    f_in = open(in_file, 'r')
    out_file = in_file.replace("_working.graphml", ".graphml")
    f_out = open(out_file, 'w')

    written_key = False

    multi_edge_index = defaultdict(int)

    edge_key_data_string = ""
    # sort attribute lines so looks neater in yED GUI
    attribute_lines = []

    skip_attributes = [
        # yED will put url and description in anyway
        #'key attr.name="url" attr.type="string" for="edge"',
        #'key attr.name="description" attr.type="string" for="edge"',
        #'key attr.name="description" attr.type="string" for="node"',
        #'key attr.name="url" attr.type="string" for="node" id',
        'attr.name="key" attr.type="int" for="edge"',
        'attr.name="id" attr.type="int" for="node"',
        'key attr.name="label" attr.type="string" for="graph"',
    ]

    skip_attributes_ids = set()

    for line in f_in:
        #print line.strip()

        yed_header = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
        <graphml xmlns="http://graphml.graphdrawing.org/xmlns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:y="http://www.yworks.com/xml/graphml" xmlns:yed="http://www.yworks.com/xml/yed/3" xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd">       
        """

        if 'attr.type="integer"' in line:
            line = line.replace("integer", "int")
        
        if 'key attr.name="key" attr.type="int" for="edge"' in line:
            re_m = re.search('attr.type="int" for="edge" id="(d\d+)" ' , line)
            if re_m:            
                edge_key_data_string = '<data key="%s">' % re_m.group(1)

        # Don't write first line
        if line.strip().startswith('<?xml version="1.0"'):
            f_out.write(yed_header)
        elif 'key attr.name' in line:
            # store for sorting
            attribute_lines.append(line)
        elif '<data key=' in line:
            # May be a data key we wish to skip eg node id
            re_m = re.search('<data key="d(\d+)">', line)
            if re_m:
                id_key = re_m.group(1)
                if id_key in skip_attributes_ids:
                    continue
                else:
                    # use this line
                    f_out.write(line)
        else:
            if len(attribute_lines):
                # If reached here, this line is not an attribute
                # but they were set, so sort then and print them as 
                # finished attributes now
                for curr_line in sorted(attribute_lines):
                    write_line = True
                    for skip_attr in skip_attributes:
                        # Attributes we don't want
                        # eg key for edges, and Url/description in yED
                        if skip_attr in curr_line:
                            write_line = False
                            # and get ID
                            re_m = re.search('id="d(\d+)"', curr_line)
                            if re_m:
                                id_key = re_m.group(1)
                                skip_attributes_ids.add(id_key)
                    if write_line:
                        f_out.write(curr_line)
                # and reset
                attribute_lines = []
            f_out.write(line)

        if line.strip().startswith("<key attr") and not written_key:
            written_key = True
            f_out.write('<key for="node" id="d50" yfiles.type="nodegraphics"/>\n')
            f_out.write('<key for="edge" id="d51" yfiles.type="edgegraphics"/>"\n')


        if line.strip().startswith('<node id='):
            re_m = re.search( '<node id="(\d+)">' , line)
            if re_m:
                node_id = int(re_m.group(1))
                node_label = graph.node[node_id]['label']
                #print graph.node[node_id]
                x = graph.node[node_id]['graphics']['x']
                y = graph.node[node_id]['graphics']['y']
                h = graph.node[node_id]['graphics']['h']
                w = graph.node[node_id]['graphics']['w']
                try:
                    fill = graph.node[node_id]['graphics']['fill']
                except KeyError:
                    fill = '#FFCC00'
                shape_type = graph.node[node_id]['graphics']['type']
            
            shape_info = """      <data key="d50">
            <y:ShapeNode>
            <y:Geometry height="%s" width="%s" x="%s" y="%s"/>
            <y:Fill color="%s" transparent="false"/>
            <y:BorderStyle color="#000000" type="line" width="1.0"/>
            <y:NodeLabel alignment="center" autoSizePolicy="content"
            fontFamily="Dialog" fontSize="8" fontStyle="plain"
            hasBackgroundColor="false" hasLineColor="false" height="13.421875"
            modelName="internal" modelPosition="c" textColor="#000000"
            visible="true" width="29.16796875" x="-7.083984375"
            y="0.7890625">%s</y:NodeLabel>
            <y:Shape type="%s"/>
            </y:ShapeNode>
            </data>  
            """ % (h, w, x, y, fill, node_label, shape_type)
            f_out.write(shape_info)

        if line.strip().startswith('<edge source="'):
            re_m = re.search( '<edge source="(\d+)" target="(\d+)">' , line)
            if re_m:
                src_id = int(re_m.group(1))
                dst_id = int(re_m.group(2))
                #node_label = graph.node[node_id]['label']

            f_out.write('\t<data key="d51"> \n\t<y:PolyLineEdge>\n ')
            # And now the intermediate points if present
            # Iterate over the points
            points = None

            edge_index = multi_edge_index[(src_id, dst_id)]
            multi_edge_index[(src_id, dst_id)] += 1

            width = 1.0
            if ('graphics' in graph[src_id][dst_id][edge_index] and 'Line' in
                  graph[src_id][dst_id][edge_index]['graphics']):
                points = graph[src_id][dst_id][edge_index]['graphics']['Line']['point']
            fill = graph[src_id][dst_id][edge_index]['graphics']['fill']

            if 'width' in graph[src_id][dst_id][edge_index]['graphics']:
                    width = graph[src_id][dst_id][edge_index]['graphics']['width']

            if points:
                f_out.write('\t<y:Path sx="0.0" sy="0.0" tx="0.0" ty="0.0">\n')
                # Use second last point
                point = points[-2]
                f_out.write('<y:Point x="%s" y="%s"/>' % (point['x'],
                                                           point['y']))
                f_out.write('\t</y:Path>\n')

            edge_info = """
            <y:LineStyle color="%s" type="line" width="%s"/>
            <y:Arrows source="none" target="none"/>
            <y:BendStyle smoothed="false"/>
            </y:PolyLineEdge>
            </data> 
            """ % (fill, width)
            f_out.write(edge_info)

    f_out.close()
    f_in.close()
    os.remove(in_file)

def remove_graphics(graph):
    for node, data in graph.nodes(data=True):
        if "LabelGraphics" in data:
            # Note we need to remove entry from graph itself
            del graph.node[node]["LabelGraphics"]

        if "graphics" in data:
            # Note we need to remove entry from graph itself
            del graph.node[node]["graphics"]

    for src, dst, key, data in graph.edges(data=True, keys=True):
        # Need key as may be multiple edges between same node pair
        # also remove for edge formatting
        # Note need to remove entries from graph itself, not data
        if "graphics" in data:
            del graph[src][dst][key]["graphics"]
        if "LabelGraphics" in data:
            del graph[src][dst][key]["LabelGraphics"]
        if "edgeAnchor" in data:
            del graph[src][dst][key]["edgeAnchor"]

    return graph

def process_geo_annotations(graph):
    for node, data in graph.nodes(data=True):
        # assume internal unless set otherwise
        graph.node[node]['Internal'] = True
        # remove whitespace
        label = data['label'].strip()
        # check if location manually forced
        # ie contains {} - assumed that {} occurs at end
        # todo: document this
        if label[-2:] == "}}" and "{{" in label:
            pos = label.find("{{")
            # geocode_id is from just after {{ to one before }}
            geocode_info = label[pos+2:-2]
            # Firstly see if this is a forced skip of cache, using a !
            if len(geocode_info) == 0:
                logger.warn("No geocode info given for "
                            "{0}".format(label))
            elif geocode_info[0] == '!':
                # Remove the symbol
                geocode_info = geocode_info[1:]
                # force cache skip
                graph.node[node]['skip_cache'] = True
            # label is bit up to the {{country}}, and remove any whitespace
            label = label[:pos].strip()
            # set country and label
            graph.node[node]['label'] = label
            if geocode_info.isdigit():
                # Numeric, assume is geoname_id
                graph.node[node]['geoname_id'] = geocode_info
            elif len(geocode_info) > 0:
                #TODO: use beginswith
                # see if starts with > or +
                if geocode_info[0] == "/":
                    # Skip geocode for this node
                    graph.node[node]['skip_geocode'] = True
                elif geocode_info[0] == ">":
                    graph.node[node]['geocode_extent'] = geocode_info[1:]
                    # {{>geo_extent}} eg tokyo {{>Japan}}
                    # {{i}} means international, eg tokyo {{i}}
                elif (len(geocode_info) == 1 and
                    geocode_info[0].lower() == "c"):
                    # Country
                    graph.node[node]['geocode_country'] = label
                elif geocode_info[0] == "$":
                    # Can also manually specify a country
                    graph.node[node]['geocode_country'] = geocode_info[1:]
                elif geocode_info[0] == "+":
                    # {{+extra}} eg Jacksonville {{+North Carolina}}
                    graph.node[node]['geocode_append'] = geocode_info[1:]
                elif geocode_info[0] == "#":
                    # {{#geocodeid}} eg Rajgarh {{#1258875}}
                    #tODO: check rest is numeric as sanity check
                    graph.node[node]['geocode_id'] = geocode_info[1:]
                else:
                    # {{city_name}}, eg Syd {{Sydney}}
                    # Assume is place name to try
                    # ie if comma present then split on it, "place, country"
                    # Check if extent also specified
                    if ">" in geocode_info:
                        geocode_name, geocode_extent = geocode_info.split(">")
                        graph.node[node]['geocode_name'] = geocode_name.strip()
                        graph.node[node]['geocode_extent'] = geocode_extent.strip()
                    else:
                        graph.node[node]['geocode_name'] = geocode_info
    return graph


def add_metadata(graph, net_name, metadata, metadata_headings,
                 valid_class_tags):
    network_label = metadata[net_name]['Network']
    # Insert into graph
    for key in metadata_headings:
        if key in metadata[net_name]:
            value = metadata[net_name][key]
            # Ensure key is in camelCase format
            # as whitespace seperates key val in GML
            key = key.title()
            key = key.replace(" ", "")

            # Split classification up into sub parts
            if key == "Classification":
                network_tags = value.split()
                for tag in valid_class_tags:
                    if tag in network_tags:
                        # Network has this tag
                        graph.graph[tag] = True
                    else:
                        # Network doesn't have this tag
                        graph.graph[tag] = False
            # Split "developed" into true/false
            elif key == 'Developed':
                if value == 'developed':
                    value = True
                else:
                    value = False
            elif key == "NetworkDate":
                # Convert C (Current) and D (Dynamic) to be date
                # obtained - allows easier use scripts
                network_date = value
                # Store the date type
                date_type = None
                if value == "C":
                    date_type = "Current"
                elif value == "D":
                    date_type = "Dynamic"
                else:
                    # Assume to be from date in the past
                    date_type = "Historic"
                graph.graph['DateType'] = date_type

                # For separated values
                year = month = day = 0
                modifier = "="

                if "<=" in value:
                    modifier = "<="
                elif "<" in value:
                    modifier = "<"
                elif ">=" in value:
                    modifier = ">="
                elif ">" in value:
                    modifier = ">"

                # Convert Current or Dynamic to actual dates
                if value == 'C' or value == 'D':
                    value = metadata[net_name]['Last Access']
                    try:
                                (day, month, year) = value.split("/")
                                # Possible y2k bug here,
                                # but not many networks predate the 1960s!
                                if year < 60:
                                    year = "19" + year
                                else:
                                    year = "20" + year
                                value = "{0}_{1}".format(year, month)

                    except Exception, e:
                        logger.warn(e)
                        logger.warn("WARNING: No known date for %s " 
                                    % network_label )
                        value = "Unknown"
                elif len(value) == 7:
                            # correct length, see if fits date pattern
                            # partly neater way for stackoverflow.com/q/447086 
                            re_m = re.search("(\d{4})-(\d{2})", value) 
                            if re_m:
                                year = re_m.group(1)
                                month = re_m.group(2)
                                value = "{0}_{1}".format(year, month)
                                # Write the metadata value to the graph 
                elif len(value) == 4 and value.isdigit():
                    year = value

                graph.graph['DateYear'] = year
                graph.graph['DateMonth'] = month
                graph.graph['DateModifier'] = modifier 
                graph.graph['LastProcessed'] = time.strftime("%Y_%m_%d",
                                                             time.localtime())
            if key != "Classification":
                # Don't write out the full classification
                graph.graph[key] = value
    return graph

def main():
    network_files = []
    if options.file:
        network_files.append(options.file)

    if options.directory:
        network_files = glob.glob(options.directory + "*.gml")

    if len(network_files) == 0:
        logger.warn("No files found. Specify -f file or -d directory")
        sys.exit(0)

    if options.directory:
        path = options.directory
    elif options.file:
        path = os.path.split(options.file)[0]
        # Get full path - don't want to create in root dir
        path = os.path.abspath(path)

    if options.output_dir:
        output_path = options.output_dir
    else:
        output_path = path + os.sep + "cleaned"

    # clean up path
    output_path = os.path.normpath(output_path)
    #output_path += strftime("%Y%m%d_%H%M%S")
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    #TODO: check nx write pickle uses cPickle

    #TODO: make this optional
    output_pickle_path = output_path + os.sep + "cache"       
    if not os.path.isdir(output_pickle_path):
        os.mkdir(output_pickle_path)

    logger.info("Saving to folder: %s" % output_path)
    # and create cache directory for pickle files
    pickle_dir = path + os.sep + "cache"       
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    metadata = {}
    # Metadata to use from CSV
    metadata_headings = ["Network", "Geo Extent", "Geo Location",
                         "Type", "Classification", "Last Access",
                        "Source", "Layer", "Date Obtained", 
                         "Network Date", "Developed", "Note", "Provenance"]

    if options.csv:
        #TODO: check csv file exists
        # Excel
        csv_file = open( options.csv, "rU" )
        csv_reader = csv.DictReader(csv_file, dialect='excel')

        for line in csv_reader:
            if line["Filename"] != "":
                net_name = line["Filename"].replace(".gml", "")
                metadata[net_name] = line
        logger.debug( "Loaded csv {0}".format(options.csv))

    # Work out the classification tags
    valid_class_tags = []
    for data in metadata.values():
        # Extract tags for each network
        for tag in data['Classification'].split():
            # Space seperated, break apart and add to list
            valid_class_tags.append(tag)
    # Remove any question marks, as Testbed? is same tag as Testbed
    valid_class_tags = [tag.replace("?","") for tag in valid_class_tags]
    # Also remove any stray commas
    valid_class_tags = [tag.replace(",","") for tag in valid_class_tags]
    # Unique
    valid_class_tags = list(set(valid_class_tags))

    #ToDO: check why sorting
    for source_file in sorted(network_files):
        # Extract name of network from file path
        filename = os.path.split(source_file)[1]
        net_name = os.path.splitext(filename)[0]
        logger.info( "Converting {0}".format(net_name))

        if options.csv and options.tagged_only:
            if net_name not in metadata:
                logger.info("Skipping {0}".format(net_name))
                continue

        pickle_file = "{0}/{1}.pickle".format(pickle_dir, net_name)
        if (os.path.isfile(pickle_file) and
            os.stat(source_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            #graph = gml_custom.read_gml(source_file)
            graph = gml_custom.read_gml(source_file)
            nx.write_gpickle(graph, pickle_file)

        # check for self-loops
        for src, dst in graph.edges():
            if src == dst:
                logger.warn( "Self edge from %s to %s " % (
                    graph.node[src]['label'],
                    graph.node[dst]['label']))

        graph = convert_to_undirected(graph)

        graph = process_geo_annotations(graph)
        graph = process_shapes(graph)

        # if all nodes in network are internal, remove redundant internal tag
        # this makes checking if internal too complex, keep tag for all nodes
        """
        if all(data['Internal'] == 1 for n,data in graph.nodes(data=True)):
            # all internal, remove tag
            for n in graph.nodes():
                del graph.node[n]['Internal']
        """

        # yEd import cleanup
        # remove directed and hiearchic from the yEd import
        # netx gml writer will write as directed if appropriate
        del graph.graph['directed']
        del graph.graph['hierarchic']

        # Add SVN version
        #*********************************
        # Apply key/legend nodes
        #*********************************
        # Check to see if any legend nodes, if so extract the shape/color combo
        # TODO check 'fill' in node for merge as else throw error if no fill set
        graph = process_legend_keys(graph)

        #*********************************
        # Add metadata
        #*********************************

        if net_name in metadata:
            graph = add_metadata(graph, net_name, metadata,
                                    metadata_headings, valid_class_tags)
        elif options.csv:
            # Not found, and using csv, give error
            logger.warn("{0} not found in metadata file".format(net_name))

        #*********************************
        # Remove graphics data
        #*********************************
        for src, dst, key, data in graph.edges(data=True, keys=True):
            #print "src %s to dst %s " % (src, dst)
            #print "src %s to dst %s " % (graph.node[src]['label'],
            #                             graph.node[dst]['label'])
            #pprint.pprint(data)
            pass

        # create copy for graphics data
        graph_graphics_kept = graph.copy()

        if options.keep_graphics is False:
            graph = remove_graphics(graph)

        # Will be overwritten with name from metadata if present
        network_label = net_name

        # remove empty note
        if 'Note' in graph.graph and graph.graph['Note'] == '':
            del graph.graph['Note']

        # Strip & as yEd fails on it
        #TODO: use html entitites fn for this
        network_label_clean = network_label.replace("&", "and")

        # Set other graph attributes
        graph.graph['Creator'] = "Topology Zoo Toolset"
        graph.graph['Version'] = "1.0"

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

        # Remove label from node
        for n in graph.nodes():
            del graph.node[n]['label']

        # And GML
        graphml_file =  "%s%s%s_working.graphml" % (output_path,
                                                    os.sep, filename)
        #graphml_file =  "{0}/{1}.graphml".format(output_path, filename)
        nx.write_graphml(graph, graphml_file)
        # and do graphics hacks
        graphml_graphics(graphml_file, graph_graphics_kept)
        logger.info("Wrote to %s" % graphml_file)


#TODO: move the nested functions out to be main functions - and pass in
 #appropriate args (setup args in prev line to nested call)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
