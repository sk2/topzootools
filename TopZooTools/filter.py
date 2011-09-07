#! /usr/bin/env python

import networkx as nx
import os
import glob
import sys
from time import strftime

import optparse
import csv
from collections import defaultdict
import pprint
import cPickle

def file_to_name(file_data, subset):
    return sorted([file_data[net]['Network'] for net in subset])

def main():
    #toDO: move parser out of main
    opt = optparse.OptionParser()
    opt.add_option('--file', '-f', help="Load data from FILE")
    opt.add_option('--directory', '-d', help="process directory")
    opt.add_option('--output_dir', '-o', help="process directory")

    opt.add_option('--unique_networks', action="store_true",
               default=False, help="Remove all but most recent entry for "
                                    "time series networks")
    opt.add_option('--ip_only', action="store_true",
                   default=False, help="Only keep IP level networks")
    opt.add_option('--non_ixp', action="store_true",
                   default=False, help="Remove Internet Exchange networks")

    opt.add_option('--europe_nren_only', action="store_true",
                   default=False, help="Keep only European NRENs")

    # Graph options
    opt.add_option('--single_edge', action="store_true",
                   default=False, help="Reduce multi edges to single edge")
    opt.add_option('--internal_only', action="store_true",
                   default=False, help="Exclude external nodes")
    options = opt.parse_args()[0]

    network_files = []
    if options.file:
        network_files.append(options.file)

    if options.directory:
        network_files = glob.glob(options.directory + "*.gml")

    if len(network_files) == 0:
        print "No files found. Please specify -f file or -d directory"
        sys.exit(0)

    if options.directory:
        path = options.directory
    elif options.file:
        path, filename = os.path.split(options.file)
        path = os.path.abspath(path)

    if options.output_dir:
        output_path = options.output_dir 
    else:
        output_path = path + os.sep + "filtered"

    if options.unique_networks:
        output_path += "_unique"
    if options.ip_only:
        output_path += "_iponly"
    if options.non_ixp:
        output_path += "_nonixp"
    if options.single_edge:
        output_path += "_se" 
    if options.internal_only:
        output_path += "_int"
    if options.europe_nren_only:
        output_path += "_europenren"

    print("Saving to folder: %s" % output_path)

    # Load geo information for filtering based on continents
    country_info = {}
    country_info_file = 'country_info.data'
    if (os.path.isfile(country_info_file)):
        country_info = cPickle.load(open(country_info_file, 'rb'))
        # Mapping of country to continent
        country_continents = dict( (d['Country'], d['Continent']) for d in
                                  country_info.values())

    if os.path.isdir(output_path):
        if len(os.listdir(output_path)) > 0:
            print "WARNING: Output directory is not empty"
    else:
        os.mkdir(output_path)  

    pickle_dir = path + os.sep + "cache"
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    file_data = {}

    for net_file in network_files:

        # Extract name of network from file path
        path, filename = os.path.split(net_file)
        network_name = os.path.splitext(filename)[0]

        print "Reading: {0}".format(network_name)

        #if network_files.index(net_file) > 20:
        #    break

        pickle_file = "{0}/{1}.pickle".format(pickle_dir, network_name)
        if (os.path.isfile(pickle_file) and
            os.stat(net_file).st_mtime <= os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older or created at same
            # time
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_gml(net_file)
            nx.write_gpickle(graph, pickle_file)

        # Record the data for filtering
        file_data[net_file] = graph.graph

    #pprint.pprint(file_data)

    remove_list = []
    print "Networks to remove: "
    if options.ip_only:
        non_ip = [net_file for net_file, data in file_data.items()
                  if not ('Layer' in data and data['Layer'] == 'IP')]
        print "Non IP Networks: " + ", ".join(file_to_name(file_data, 
                                                           non_ip))
        remove_list += non_ip

    if options.non_ixp:
        ixp = [net_file for net_file, data in file_data.items()
                    if data['IX'] == 1]
        print "IXP Networks: " + ", ".join(file_to_name(file_data, 
                                                        ixp))
        remove_list += ixp
    
    if options.unique_networks:
        non_unique_networks = []
        freq_dict = defaultdict(list) 
        # Dict format of 'Network Name': 'Unique entries'
        # eg 'Renater': ['renater_2004', 'renater_2006']
        for net_file, data in file_data.items():
            network_name = data['Network']
            freq_dict[network_name].append(net_file)
        # Keep only those with multiple entries
        for key, val in freq_dict.items():
            if len(val) <= 1:
                del freq_dict[key]
        for network, entries in freq_dict.items():
            #TODO: tidy up the nested else if statements
            # Find most recent date
            # Sorting using string comparison is fine as year is listed
            # first then month
            entries_dated = sorted(entries, key = lambda x:
                                   file_data[x]['NetworkDate'])
            # Remove most recent (ie last after sorted) entry
            if options.europe_nren_only:
                if file_data[entries_dated[-1]]['Layer'] != "IP":
                    entries_ip = [file for file in entries_dated if
                                  file_data[file]['Layer'] == "IP"]
                    if len(entries_ip):
                        # There exists an older network that is IP
                        # entries_ip preserves sorted chronological order
                        # So pop removes most recent IP network
                        ip_network_to_use = entries_ip.pop()
                        # Remove this from the list of entries, so not excluded
                        entries_dated.remove(ip_network_to_use)
                    else:
                        # Just remove most recent so it is not excluded
                        entries_dated.pop()
                else:
                    # Just remove most recent so it is not excluded
                    entries_dated.pop()
            else:
                # Just use most recent network
                entries_dated.pop()
            # And mark the rest to be skipped as old versions
            # Only want to list the network name, not the date used for
            # sorting
            #TODO: just append the list?
            non_unique_networks += [e for e in entries_dated]
        print "Non Unique Networks: " + ", ".join(file_to_name(file_data, 
                                                        non_unique_networks))
        remove_list += non_unique_networks 

    if options.europe_nren_only:
        non_europe_nrens = []
        for net_file, data in file_data.items():
            if data['Type'] != 'REN':
                non_europe_nrens.append(net_file)

                #ToDO: also want to keep GEANT

            if not ((data['GeoLocation'] in country_continents 
                and country_continents[data['GeoLocation']] == 'EU') 
                or data['GeoLocation'].endswith('UK')
                or data['GeoLocation'] == 'Europe'
                or data['GeoLocation'] == 'Turkey'
                or data['GeoLocation'] == 'Israel'
                ):
                non_europe_nrens.append(net_file)
                
            # Skip MANs
            if data['GeoExtent'] == 'Region':
                non_europe_nrens.append(net_file)

            # Skip Janet External
            if data['Network'] == "Janet External":
                non_europe_nrens.append(net_file)


            # Itnet is historical, use Heanet instead
            if data['Network'] == "Itnet":
                non_europe_nrens.append(net_file)

        remove_list += non_europe_nrens 

    #print "Removing: " + ", ".join(remove_list)

    keep_files = set(file_data.keys()) - set(remove_list)
                    
    for net_file in sorted(keep_files):

        #toDO: put pickle code into function
        # Extract name of network from file path
        path, filename = os.path.split(net_file)
        network_name = os.path.splitext(filename)[0]

        print "Filtering: {0}".format(network_name)

        #if network_files.index(net_file) > 20:
        #    break

        pickle_file = "{0}/{1}.pickle".format(pickle_dir, network_name)
        if (os.path.isfile(pickle_file) and
            os.stat(net_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_gml(net_file)
            nx.write_gpickle(graph, pickle_file)

        graph = graph.to_undirected()
        graph = nx.MultiGraph(graph)

        #*********************************
        #Graphs - create different graphs for writing
        #*********************************
        if options.single_edge:
            # Graph with max one edge between any node pair
            graph = nx.Graph(graph)

        #TODO Update this to be true/false once netx fixes boolean quote bug
        if options.internal_only:
            external_nodes = [ n for n in graph.nodes()
                              if graph.node[n]['Internal'] == 0]
            graph.remove_nodes_from(external_nodes)
        # ************

        out_file = "{0}/{1}".format(output_path, network_name)
        # Append any custom options to filename


        #*********************************
        #OUTPUT - Write the graphs to files
        #*********************************
        gml_file =  out_file + ".gml"
        nx.write_gml(graph, gml_file)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass    

