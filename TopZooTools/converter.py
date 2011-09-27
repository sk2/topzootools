#! /usr/bin/env python

import networkx as nx

import os
import glob
import sys
from time import strftime

import optparse
import csv

import pprint

from mako.lookup import TemplateLookup    
from pkg_resources import resource_filename

import zipfile
import cPickle
from collections import defaultdict

def continent_breakdown(geo_locations_by_extent):
    retval = {'GlCPlus': [], 'AP': [], 'Eur': [], 'NA/LA': []}

    country_info = {}
    country_info_file = 'country_info.data'
    if (os.path.isfile(country_info_file)):
        country_info = cPickle.load(open(country_info_file, 'rb'))

    # Mapping of country to continent
    country_continents = dict( (d['Country'], d['Continent']) for d in
                              country_info.values())
    # unique continent codes

    for values in geo_locations_by_extent['Global'].values():
        retval['GlCPlus'] += values
    for values in geo_locations_by_extent['Continent+'].values():
        retval['GlCPlus'] += values
    # Remove so can see leftovers at end
    del geo_locations_by_extent['Global']
    del geo_locations_by_extent['Continent+']

    # Now by continent
    cont_mapping = {'Asia-Pacific': 'AP', 'Europe': 'Eur', 
                    'North America': 'NA/LA', 'Latin America': 'NA/LA'}
    for key, val in cont_mapping.items():
        retval[val] += geo_locations_by_extent['Continent'][key]
        del geo_locations_by_extent['Continent'][key]

    # Now by country
    # Continents to group countries by
    # then convert these into groups for retval
    by_continent = {'AF': [], 'NA': [], 'OC': [], 'AN': [], 'AS': [], 'EU': [],
                    'SA': []}
    # Append manual country lookups
    country_continents['US'] = "NA"
    country_continents['Palestine'] = "AF"
    country_continents['Northern Ireland'] = "EU"
    country_continents['USA'] = "NA"
    country_continents['UK'] = "EU"

    for country, data in geo_locations_by_extent['Country'].items():
        if country in country_continents:
            # Have continent for this country
            cont = country_continents[country]
            by_continent[cont] += data
            del geo_locations_by_extent['Country'][country]
        else:
            print "no continent for %s" % country


    for countries, data in geo_locations_by_extent['Country+'].items():
        # For country+ we take the first country to find the continent
        country = countries.split(",")[0]
        if country in country_continents:
            # Have continent for this country
            cont = country_continents[country]
            by_continent[cont] += data
            del geo_locations_by_extent['Country+'][countries]
        else:
            print "no continent for country+ %s" % countries


    # Region
    for region, data in geo_locations_by_extent['Region'].items():
        # Region generally in format "region, country" so try split on comma
        country = region.split(",")[1].strip()
        if country in country_continents:
            cont = country_continents[country]
            by_continent[cont] += data
            del geo_locations_by_extent['Region'][region]
        else:
            print "no continent for region %s with country of %s" % (region,
                                                                     country)

    # same for metro
    for metro, data in geo_locations_by_extent['Metro'].items():
        # metro generally in format "metro, country" so try split on comma
        country = metro.split(",")[1].strip()
        if country in country_continents:
            cont = country_continents[country]
            by_continent[cont] += data
            del geo_locations_by_extent['Metro'][metro]
        else:
            print "no continent for metro %s with country of %s" % (metro,
                                                                    country)

    # And now map these back
    # Ignore Africa and Antarctica
    cont_mapping = {'NA': 'NA/LA', # North America -> North/Latin America
                    'OC': 'AP',     #Oceania -> Asia pac
                    'AS': 'AP',     # Asia -> asia pac
                    'EU': 'Eur',
                    'SA': 'NA/LA'   # South America -> North/Latin America
                   }
    for key, val in cont_mapping.items():
        retval[val] += by_continent[key]
        del by_continent[key]

    return retval

def matlab_keys(summary_data, output_path, counts_only=False):
    matlab_keys = {}

    keys_to_use = ["Classification", "Type", "Layer", "GeoExtent",
                    "GeoLocation", "Developed", "Dataset", "NetworkData",
                   "Provenance", "Access", "Backbone", "Customer", "Transit",
                   "Testbed", "IX", "NetworkDate"]

    # Index of this network in the directory listing. Note: starting at 1
    network_index = 1
    file_index = {}
    geo_extents = defaultdict(list)
    geo_locations_by_extent = defaultdict(dict)
    
    for network, data in sorted(summary_data.items()):

        file_index[network_index] = network

        geo_extent = data['GeoExtent']
        geo_location = data['GeoLocation']
        geo_extents[geo_extent].append(network_index)
        geo_locations_by_extent[geo_extent].setdefault(geo_location,
                                            []).append(network_index)

        # look at properties of this network 
        for key, val in data.items():
        
            if key not in keys_to_use:
                # Not interested in this key (eg source, version, etc), skip
                continue

            if key not in matlab_keys:
                matlab_keys[key] = {}
            
            if val not in matlab_keys[key]:
                matlab_keys[key][val] = []

            matlab_keys[key][val].append(network_index) 


        network_index += 1

    # Custom keys for Hung for JSAC
    # breakdown by country/region/multi
    # Store for printing
    matlab_keys['cou_reg_mul'] = {}
    matlab_keys['cou_reg_mul']['Country'] = (geo_extents['Country'] +
                                             geo_extents['Country+'])
    matlab_keys['cou_reg_mul']['Region'] = (geo_extents['Metro'] +
                                            geo_extents['Region'])
    matlab_keys['cou_reg_mul']['Multi'] = (geo_extents['Continent'] + 
                                           geo_extents['Continent+'] + 
                                           geo_extents['Global'])

    c_breakdown = continent_breakdown(geo_locations_by_extent)
    matlab_keys['continent_breakdown'] = c_breakdown
    # Sanity check
    #for continent, values in c_breakdown.items():
    #    print continent
    #    print "; ".join([summary_data[file_index[network_index]]['GeoLocation'] for
    #                    network_index in values])

    f_matlab_keys = open("{0}/matlab_keys.txt".format(output_path), "w")
    
    if not counts_only:
        f_matlab_keys.write("-----------\nFile Index\n")
        for key, val in sorted(file_index.items()):
            f_matlab_keys.write("{0} {1}\n".format(key, val))
    # Also print out in a list for Matlab purposes so can select on index
    f_matlab_keys.write("\n['" + "', '".join(file_index.values()) + "']")
    for heading, data in matlab_keys.items():
        f_matlab_keys.write("\n-----------------\n" + heading + "\n")
        # do keys with names at end so can copy paste first bit into matlab
        keys_with_names = '\nNamed:\n'
        keys_with_counts = '\nCounts:\n'
        for key, values in sorted(data.items()):
            if key == '':
                key = "No value"
            # Also list with names for double checking
            if not counts_only:
                f_matlab_keys.write("{0}\t{1}\n".format(key, sorted(values)))
                values_named = sorted([file_index[x] for x in values])
                keys_with_names += "{0}\t[{1}]\n".format(key, 
                                                         ','.join(values_named))
            keys_with_counts += "{0}\t{1}\n".format(key, len(values))
        if not counts_only:
            f_matlab_keys.write(keys_with_names)
        f_matlab_keys.write(keys_with_counts)
    f_matlab_keys.close()


def main():
    #toDO: move parser out of main
    opt = optparse.OptionParser()
    opt.add_option('--file', '-f', help="Load data from FILE")
    opt.add_option('--directory', '-d', help="process directory")
    opt.add_option('--output_dir', '-o', help="process directory")
    opt.add_option('--map_dir', help="geoplot map input dir for gallery")

    # Graph options
    opt.add_option('--single_edge', action="store_true",
                   default=False, help="Reduce multi edges to single edge")
    opt.add_option('--internal_only', action="store_true",
                   default=False, help="Exclude external nodes")
    opt.add_option('--matlab_keys', action="store_true",
                   default=False, help="Generate list of keys for Matlab")
    opt.add_option('--counts_only', action="store_true",
                   default=False, help="Only record counts for Matlab keys")
    # Output options
    opt.add_option('--matlab', action="store_true", default=False,
                   help="Write Matlab adjacency list Format")
    opt.add_option('--gml',  action="store_true", default=False, 
                   help="Write GML Format")
    opt.add_option('--graphml', action="store_true", default=False,
                   help="Write Graphml Format")
    opt.add_option('--dot', action="store_true", default=False, 
                   help="Write GraphViz dot Format")
    opt.add_option('--gexf', action="store_true", default=False,
                   help="Write GEXF Format")
    opt.add_option('--plot', action="store_true", default=False,
                   help="Plot graph")
    opt.add_option('--gallery', action="store_true", default=False,
                   help="Render gallery HTML")

    opt.add_option('--summary_csv', action="store_true", default=False,
                   help="Generate summary data in CSV format")


    opt.add_option('--pickle', action="store_true", default=False,
                   help="Write NetworkX Pickle Format")
    opt.add_option('--archive', action="store_true", default=False,
                   help="Archive converted files")
    opt.add_option('--html', action="store_true", default=False, 
                   help="HTML summary")
    opt.add_option('--skip_type',  help="List of types to skip")
    options = opt.parse_args()[0]

    template_dir =  resource_filename("TopZooTools","templates")
    lookup = TemplateLookup(directories=[ template_dir ],
            module_directory= "/tmp/mako_modules",
            #cache_type='memory',
            #cache_enabled=True,
            )

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
        output_path = path + os.sep + "converted"
    
    if not os.path.isdir(output_path):
        os.mkdir(output_path)  


    pickle_dir = path + os.sep + "cache"
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    summary_data = {}

    archive_file =  None
    if options.archive:
        archive_filelist = []
        archive_file = "archive.zip"

    #These headings are used in HTML template, if not present then set to "" 
    metadata_headings = ["Network", "GeoExtent", "GeoLocation", 
                         "Type", "Classification", "LastAccess",
                        "Source", "Layer", "DateObtained",
                        "NetworkDate", "Note"]
    for net_file in network_files:

        # Extract name of network from file path
        path, filename = os.path.split(net_file)
        network_name = os.path.splitext(filename)[0]

        print "Converting: {0}".format(network_name)

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

        if options.skip_type:
            skip_type = options.skip_type
            print "Skipping {0}".format(skip_type)
            # Search for nodes with this type set
            for node, data in graph.nodes(data=True):
                if 'type' in data and data['type'] == skip_type:
                    graph.remove_node(node)

        if (options.html or options.matlab_keys or options.gallery or
            options.summary_csv):
            summary_data[network_name] = {} 
            classification_types = set(["Testbed", "Customer", "Backbone", "Transit", "IX",
                "Commercial"])
            classification = []

            for key, val in graph.graph.items():
                if key in classification_types and val == True:
# append, eg if Backbone then add "Backbone" to classification
                    classification.append(key)
                # And also store in html data
                summary_data[network_name][key] = val

            summary_data[network_name]['Classification'] = ", ".join(sorted(classification))

            # also some graph statistics
            summary_data[network_name]['Nodes'] = graph.number_of_nodes()
            summary_data[network_name]['Edges'] = graph.number_of_edges()

        #*********************************
        #Graphs - create different graphs for writing
        #*********************************
        if options.single_edge:
            # Graph with max one edge between any node pair
            graph = nx.Graph(graph)

        #TODO Update this to be true/false once netx fixes boolean quote bug
        if options.internal_only:
            external_nodes = [ n for n in graph.nodes()
                              if ('Internal' in graph.node[n] and 
                              graph.node[n]['Internal'] == 0)]
            graph.remove_nodes_from(external_nodes)
        # ************

        out_file = "{0}/{1}".format(output_path, network_name)
        # Append any custom options to filename
        if options.skip_type:
            #TODO Make sure only alphanumeric (safe file name) chars in filename
            out_file += "_skip_" + options.skip_type.lower()
        if options.single_edge:
            out_file += "_se" 
        if options.internal_only:
            out_file += "_int"

        #*********************************
        #OUTPUT - Write the graphs to files
        #*********************************
        if options.gml:
            #Add quotes
            gml_file =  out_file + ".gml"
            # workaround for bug where gml writer removes label from node
            #TODO: see if this is needed
            graph2 = graph.copy()
            nx.write_gml(graph2, gml_file)
            if options.archive:
                archive_filelist.append(gml_file)
   
        if options.graphml:
            graphml_file =  out_file + ".graphml"
            nx.write_graphml(graph, graphml_file)
            if options.archive:
                archive_filelist.append(graphml_file) 
     
        if options.dot:
            dot_file =  out_file + ".dot"
            nx.write_dot(graph, dot_file) 
            if options.archive:
                archive_filelist.append(dot_file)
        
        if options.gexf:
            gexf_file =  out_file + ".gexf"
            nx.write_gexf(graph, gexf_file) 
            if options.archive:
                archive_filelist.append(gexf_file)

        if options.pickle:
            pickle_file =  out_file + ".pickle"
            nx.write_gpickle(graph, pickle_file) 
            if options.archive:
                archive_filelist.append(pickle_file)

        #TODO: merge options.plot with geoplot
        # make geoplot part of this

        if options.matlab:
            # Write as a sparse matrix
            mat_file =  out_file + ".mat"
            mat_fh = open(out_file + ".txt", "w")
            if options.archive:
                archive_filelist.append(mat_file)
            sparse_matrix = nx.to_numpy_matrix(graph)

            # Write out manually (numpy.tofile() writes as all on one line)
            for line in sparse_matrix.tolist():
                # Convert list to a string format numbers as integers not floats
                line = [ str(int(x)) for x in line]
                mat_fh.write( " ".join(line) )
                mat_fh.write("\n")
            mat_fh.close()

    if options.matlab_keys:
        matlab_keys(summary_data, output_path, options.counts_only)

    if options.summary_csv:
        network_list = [data['Network'] for data in summary_data.values()]
        fieldnames = ['Name', 'Network', 'Date', 'Type', 'Layer',
                      'Nodes', 'Edges',
                      'Developed', 'GeoExtent', 'GeoLocation',
                      'Access', 'Backbone', 'Commercial', 'Customer', 
                      'Testbed', 'Transit']

        dw = csv.DictWriter(open('summary.csv','w'), delimiter=',', 
                            fieldnames=fieldnames, restval='')
        dw.writeheader()

        for node, data in sorted(summary_data.items()):
            node_dict = {}
            network = data['Network']
            node_dict['Network'] = network
            # All the other properties to include
            node_dict['Access'] = data['Access']
            node_dict['Backbone'] = data['Backbone']
            node_dict['Commercial'] = data['Commercial']
            node_dict['Customer'] = data['Customer']
            node_dict['Developed'] = data['Developed']
            node_dict['GeoExtent'] = data['GeoExtent']
            node_dict['GeoLocation'] = data['GeoLocation']
            node_dict['Layer'] = data['Layer']
            node_dict['Name'] = node
            node_dict['Network'] = data['Network']
            node_dict['Nodes'] = data['Nodes'] 
            node_dict['Edges'] = data['Edges'] 
            node_dict['Testbed'] = data['Testbed']
            node_dict['Transit'] = data['Transit']
            node_dict['Type'] = data['Type']
            node_dict['SourceGitVersion'] = data['SourceGitVersion']
            node_dict['ToolsetVersion'] = data['ToolsetVersion']

            if network_list.count(network) > 1:
                # Multiple observations of this network, record date
                node_dict['Date'] = data['NetworkDate']
            dw.writerow(node_dict)

        # write csv to output directory

    # Pre-processing for html and gallery as they both use these dictionaries
    if options.html:
        for network, data in summary_data.items():
            # Set any missing entries to blank
            for key in metadata_headings:
                if key not in data:
                    summary_data[network][key] = ""

    gallery_data = []
    if options.gallery and options.map_dir:
        # See which networks have an associated map
        gallery_data = [] 
        for network in summary_data:
            map_file = "{0}{1}.jpg".format(options.map_dir, network)
            if os.path.isfile(map_file):
                # Map file  exists, add to gallery
                gallery_data.append(network)
            else:
                print("No plot file found for {0}".format(network))
            
    if options.html:
        #TODO: if matlab, plot, gml, graphml etc set, then provide hyperlink 
        date = strftime("%A %d %B %Y")

        html_template = lookup.get_template("html.mako")
        f_html = open("{0}/dataset.html".format(output_path), "w")
        f_html.write(html_template.render(
            summary_data = summary_data,
            gallery_data = gallery_data,
            metadata_headings = metadata_headings,
            archive_file = archive_file,
            date = date,
        ))    
        f_html.close()
       
    if options.gallery:
        if options.map_dir:
            date = strftime("%A %d %B %Y")          
            #TODO: if matlab, plot, gml, graphml etc set, then provide hyperlink 
            # Set any missing entries to blank
            gallery_template = lookup.get_template("gallery.mako")
            f_html = open("{0}/gallery.html".format(output_path), "w")
            f_html.write(gallery_template.render(
                summary_data = summary_data,
                gallery_data = gallery_data,
                date = date,
            ))    
            f_html.close()
        else:
            print ("Please specify maps source directory to use in creating"
                   " gallery")


    if options.archive:
        archive_fh = zipfile.ZipFile(
            "{0}/{1}".format(output_path, archive_file), "w")
        for name in archive_filelist:
            archive_fh.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)
        archive_fh.close()
        

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass    
