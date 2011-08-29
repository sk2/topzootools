#! /usr/bin/env python

from pyparsing import Word, alphas, nums, OneOrMore, Literal, Group, Optional
import networkx as nx
import optparse
import os
import glob
import re
import sys
import logging
import logging.handlers

#TODO: fix logging support

opt = optparse.OptionParser()
opt.add_option('--directory', '-d', help="process directory")
#TODO: make this required if using geocoding
opt.add_option('--geocode', '-g',  action="store_true",
                default=False,
                help="Attempt to lookup co-ordinates")
opt.add_option('--debug',  action="store_true",
                default=False,
                help="Print debugging information to terminal")
opt.add_option('--geonames_username', '-u', 
                default=False,
                help="Username for Geonames web service")
opt.add_option('--skip_cache',  action="store_true",
                default=False,
                help="Don't use cache for geocoding")
opt.add_option('--intra_as',  action="store_true",
                default=False,
               help="Only process Intra-AS topologies ie xxx:xxx not xxx:yyy "
               "folder format")

options = opt.parse_args()[0]

logger = logging.getLogger("rocketfuel2zoo")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)-6s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
if options.debug:
    ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logging.getLogger('').addHandler(ch)
log_dir = "."
LOG_FILENAME =  os.path.join(log_dir, "rocketfuel2zoo.log")
LOG_SIZE = 2097152 # 2 MB
fh = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_SIZE, backupCount=5)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
fh.setFormatter(formatter)
logging.getLogger('').addHandler(fh)

def main():
    #TODO: make this a callback function for verification
    if options.geocode and not options.geonames_username:
        logger.error("Please enter a Geonames username to use geocoding")
        sys.exit(0)

    directory = options.directory + os.sep
    output_path = directory + os.sep + "rf2zoo" + os.sep
    output_path = os.path.normpath(output_path)
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    logger.info("Writing to %s" % output_path)
    all_folders = glob.glob(directory + "*")

    if options.geocode:
        from geocoder import Geocoder
        geocoder = Geocoder(options.geonames_username, options.skip_cache)

    for folder in all_folders:
        if not os.path.isdir(folder):
            continue
        #tODO: allow support for specifying the edge file directly

        # Check valid format of x:y where x, y are valid ASNs
        (_, data_folder) = os.path.split(folder)
        if not re.match("\d+:\d+", data_folder):
            continue

        # Only do internal topologies
        if options.intra_as:
            (asn_a, asn_b) = data_folder.split(":")
            if asn_a != asn_b:
                continue
        
        logger.info("Processing %s" % data_folder)

        G = nx.Graph()

        #TODO: check on bidirectionality
        #TODO: download file if not present
        #TODO: add pyparsing to topzootools dependencies for Pypi

        # Definitions for parsing
        colon = Literal(":").suppress()
        #TODO: see if comma built in
        comma = Literal(",").suppress()
        arrow = Literal("->").suppress()
        ASN = Word(nums)
        ObsCount = Word(nums)
        # Can have . eg St. Louis
        place_name = Group(
            OneOrMore(Word(alphas+".")) 
            + Optional(comma) +
            Optional(Word(alphas)))

        node = Group(ASN + colon + place_name)
        entry = (node + arrow + node + ObsCount)

        filename = folder + os.sep + "edges"
        f = open(filename)

        #ToDO; print unparsed lines

        #ToDo: push all into dict and use add_nodes_from and add_edges_from

        for line in f:
            #print line
            processed = entry.parseString(line)
            #print processed
            (src, dst, obs_count) = processed
            src_asn, src_place = src
            dst_asn, dst_place = dst
            src_place = " ".join(src_place)
            dst_place = " ".join(dst_place)
            # Use simple string for ID (not list returned from Pyparsing which
            # don't always hash the same
            src_id = "%s %s" % (src_place, src_asn)
            dst_id = "%s %s" % (dst_place, dst_asn)
            # use full hash for node id
            G.add_node(src_id, ASN=int(src_asn), label =src_place )
            G.add_node(dst_id, ASN=int(dst_asn), label=dst_place )
            G.add_edge(src_id, dst_id, obs_count = int(obs_count))

        f.close()

        # Relabel nodes to have id based on index

        if options.geocode:
                G = geocoder.geocode(G)
                geocoder.save_cache()

        
        G.name = data_folder
        G.graph['Creator'] = 'rocketfuel2zoo'

        out_file = output_path + os.sep + data_folder.replace(":", "_") + ".gml"

        #import pprint
        #pprint.pprint( sorted(G.nodes()))
        #pprint.pprint( G.nodes(data=True))
        nx.write_gml(G, out_file)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
