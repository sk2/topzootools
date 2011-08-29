#! /usr/bin/env python

import networkx as nx 
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap 
import os
import glob
import optparse
import sys
import pprint as pp
import calendar
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as colors
from matplotlib.font_manager import FontProperties

opt = optparse.OptionParser()
opt.add_option('--directory', '-d', help="process directory")
opt.add_option('--file', '-f', help="process file")
opt.add_option('--output_dir', '-o', help="process directory")
opt.add_option('--bluemarble', action="store_true",
               default=False, help="Use Blue Marble background image")

opt.add_option('--pdf', action="store_true",
               default=False, help="Output to PDF format")
opt.add_option('--eps', action="store_true",
               default=False, help="Output to EPS format")
opt.add_option('--jpg', action="store_true",
               default=False, help="Output to JPG format")
opt.add_option('--png', action="store_true",
               default=False, help="Output to PNG format")


opt.add_option('--explode', action="store_true",
               default=False, help="Explode co-located nodes to be spaced out")
opt.add_option('--labels', action="store_true",
               default=False, help="Print node labels")
opt.add_option('--heatmap', action="store_true",
               default=False, help="Plot heatmap")
opt.add_option('--title', action="store_true",
               default=False, help="Add title to graph")
opt.add_option('--highlight_outliers', action="store_true",
               default=False, help="Highlight outlier nodes")
opt.add_option('--straight_line', action="store_true", default=False,
               help="Draw straight line between nodes instead of great circle")


options, args = opt.parse_args()

#TODO: make mark outliers handle great circle - or disable option
# and warn user

#Todo; allow user to plot using normal scatter plot, ie not only basemap
#TODO: add option to mark outliers

#TODO: remove magic numbers with constants

# Sanity checking
if not (options.pdf or options.eps or options.jpg or options.png):
    print "No output format specified"
    sys.exit(0)

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
    path = options.directory
elif options.file:
    path, filename = os.path.split(options.file)

if options.output_dir:
    output_path = options.output_dir 
else:
    output_path = path + "/plotted"

if options.bluemarble:
    output_path += "_bm"
else:
    output_path += "_flat"

if not os.path.isdir(output_path):
    os.mkdir(output_path)  

pickle_dir = path + "/cache"
if not os.path.isdir(pickle_dir):
    os.mkdir(pickle_dir)

#toDO: See if can warn about nodes outside map area

#TODO: add logger debug

lats_all = []
lons_all = []
labels_all = []
#TODO: place hyperedges on map halfway between the nodes they relate to

for net_file in sorted(network_files):
    # Extract name of network from file path
    path, filename = os.path.split(net_file)
    network_name, extension = os.path.splitext(filename)
    print "Plotting: {0}".format(network_name)

    pickle_file = "{0}/{1}.pickle".format(pickle_dir, network_name)
    if (os.path.isfile(pickle_file) and
        os.stat(net_file).st_mtime < os.stat(pickle_file).st_mtime):
        # Pickle file exists, and source_file is older
        G = nx.read_gpickle(pickle_file)
    else:
        # No pickle file, or is outdated
        G = nx.read_gml(net_file)
        nx.write_gpickle(G, pickle_file)
   
    # Reduce to undirected single edge graph for simplicity
    G = nx.Graph(G)


    lats = []
    lons = []     
    latlon_node_index = []

    geocoded_cities = [ n for n, data in G.nodes(data = True)
                if 'Latitude' in data and 'Longitude' in data]

    hyperedge_nodes = [ n for n, data in G.nodes(data = True)
                       if 'hyperedge' in data and data['hyperedge'] == 1]

    # Handle non geocoded places, as give misleading appearance of disconnected
    # graph
    non_geocoded_cities = [n for n in G.nodes() if (n not in geocoded_cities and
                                                   n not in hyperedge_nodes)]

    #G_full = G.copy()
    #G = G.subgraph(geocoded_cities)

    #for n in non_geocoded_cities:
   #     if G.degree(n) < 2:
  #          G.remove_node(n)
  #      elif G.degree(n) == 2:
  #          # replace with edge from neighbors
  #          neigh = G.neighbors(n)
  #          #TODO: mark as diff type so can dash accordingly when plotting
  #          G.add_edge(neigh[0], neigh[1], inferred=True)
  #          G.remove_node(n)
  #      elif G.degree(n) > 2:
  #          for neigh in G.neighbors(n):
  #              G[n][neigh]['inferred'] = True
  #          # Treat as a hyperedge
  #          hyperedge_nodes.append(n)

    #TODO: check number of edges in graphdoesn't increase without bound.... 
    #should go down

    # Update list of hyperedge nodes (rather than inside the iterator before)


    # work out how to do multiple hyperedges connected to each other eg deltacom
    #toDO: make this option argument command line-able
    #for node in hyperedge_nodes:
   #     he_lats = []
   #     he_lons = []
   #     for neigh in G.neighbors(node):
   #         if 'Latitude' in G.node[neigh]:
   #             he_lats.append( G.node[neigh]['Latitude'] )
   #         if 'Longitude' in G.node[neigh]:
   ##             he_lons.append( G.node[neigh]['Longitude'] )
   #     if len(he_lats) > 0 and len(he_lons) > 0:
   #         G.node[node]['Latitude'] = np.mean(he_lats)
   #         G.node[node]['Longitude'] = np.mean(he_lons)
   #         # And add to geocoded places list as now has co-ords
   #     else:
   #         # Replace with inferred edges
   #         neigh_list = G.neighbors(node)
   #         for n_a in neigh_list:
   #             for n_b in neigh_list:
   #                 if n_a != n_b:
   #                     G.add_edge(n_a, n_b, inferred=True)
   #         G.remove_node(node)
#

    # Remove cities with no co-ords
    #TODO: see if this makes a difference
    #G = G.subgraph(geocoded_cities)
   # hyperedge_nodes = [n for n in hyperedge_nodes if n in G]

    if G.number_of_nodes() == 0:
        print "No geocoded nodes in {0}, skipping".format(network_name)
        continue

    for n in geocoded_cities:  
        lats.append(G.node[n]['Latitude'])                 
        lons.append(G.node[n]['Longitude'])
        # Also store the node order for retrieval
        latlon_node_index.append(n)
        labels_all.append(data['label'])
 

    # check that not all co-located points
    # Check if smaller than typical rounding error
    # Otherwise get error when doing Basemap calculations
    rnd_err = 0.0001
    if ( np.std(lats) < rnd_err and np.std(lons) < rnd_err):
        # No std dev -> all points == mean, ie all same
        print ("All nodes in {0} have same location, "
               "skipping").format(network_name)
        continue

    # store for heatmap
    # TODO: check on fastest way to do the append to list
    lats_all += lats
    lons_all += lons

    # corner lats and lons
    llcrnrlon = min(lons)       
    llcrnrlat = min(lats) 
    urcrnrlon = max(lons)
    urcrnrlat = max(lats)          

    # expand a little bit so have frame around border nodes   
    # expand by 15% of distance between border nodes
    margin_lon = 0.15 * abs(urcrnrlon - llcrnrlon)
    margin_lat = 0.15 * abs(urcrnrlat - llcrnrlat)      
    # and expand
    llcrnrlon -= margin_lon      
    llcrnrlat -= margin_lat
    urcrnrlon += margin_lon
    urcrnrlat += margin_lat

    # ensure that lat fits within (-90,90) once expanded
    llcrnrlat = max(llcrnrlat, -89.9)
    urcrnrlat = min(urcrnrlat, 89.9)

    lat_1 = (urcrnrlat + llcrnrlat)/2
    lon_0 = (urcrnrlon + llcrnrlon)/2
    #TODO: see if stil need this try catch b lock now to stdev check
    try:
        m = Basemap(resolution ='c', projection='merc', llcrnrlat = llcrnrlat,
                    urcrnrlat = urcrnrlat,  llcrnrlon = llcrnrlon,
                    urcrnrlon = urcrnrlon, lat_ts = lat_1)

    except ZeroDivisionError, e:
        print "Error {0}".format(e)
        continue
           
    # Convert lats and lons into x,y for plotting
    mx,my = m(lons,lats)

    if options.explode:
        # Find nodes that are quite close together
        #TODO: find algorithm to detect close by nodes
        # note may be more than one, and may not be at exact co-ords
        print "Explode is not yet supported"
        pass


    pos = {} 
    for index in range(len(mx)):
        # Extract the converted lat lons for each node
        node = latlon_node_index[index]
        pos[node] = (mx[index], my[index])
                                
    pp.pprint(pos)

    # and labels
    labels = {}
    for n, data in G.nodes(data = True):
        # Only do massive (likely unfeasible) nodes
        labels[n] =  data['label']


    # initialise to be halfway between nearest neighbors
    

    # Use basic spring layout for non-geocoded nodes
    pos_non_geo = nx.spring_layout(G, pos=pos, fixed=geocoded_cities,
                                   iterations=5000)
    pp.pprint(pos_non_geo)
    # And convert these back to lat/lon for basemap plotting
    non_geo_x = []
    non_geo_y = []
    non_geo_index = []
    for n in non_geocoded_cities:
        non_geo_index.append(n)
        non_geo_x.append(pos_non_geo[n][0])
        non_geo_y.append(pos_non_geo[n][1])

    non_geo_lons, non_geo_lats = m(non_geo_x, non_geo_y, inverse=True)

    for index, n in enumerate(non_geo_index):
        print "found lat for " + str(n)
        G.node[n]['Latitude'] = non_geo_lats[index]
        G.node[n]['Longitude'] = non_geo_lons[index]

    for n, data in G.nodes(data=True):
        print n
        print data


    # Draw the map 
    plt.clf()
    m.drawcountries(linewidth = 0.2) 
    m.drawcoastlines(linewidth = 0.2)
    
    if options.bluemarble:
        m.bluemarble()
    else:
        #TODo: fix bug where if using straight line it gets drawn under
        # continent fill
        m.fillcontinents()

    # Colours depending on if using bluemarble image or white background
    if options.bluemarble:
        node_color ="#FF8C00"
        font_color = "w"
        default_edge_color = "#bbbbbb"
        title_color = "w"
        #colormap = cm.autumn
        colormap = cm.jet
    else:
        node_color = "k"
        font_color = "k"
        default_edge_color = "#338899"
        title_color = "k"
        colormap = cm.jet

    """
    Colormap based on
    http://www.packtpub.com/article/plotting-data-using-matplotlib-part2
    and http://fatweasel.com/tutorials/python-tutorials/
         matplotlib/histogram-with-normalized-data/
    and
    http://stackoverflow.com/questions/4700614/how-to-put-the-legend-out-of-the-plot
    """

    # Find all edge speeds
    speed_labels = {}
    speeds = []
    # Flag if there are any links with no attributes, which need to be added to
    # the legend with default color
    no_speed_edge_present = False
    for s,t,data in G.edges(data=True):
        if 'LinkSpeedRaw' in data and data['LinkSpeedRaw'] not in speeds:
            raw_speed = data['LinkSpeedRaw']
            speeds.append(raw_speed)
            speed_labels[raw_speed] = "{0} {1}".format(data['LinkSpeed'],
                                                           data['LinkSpeedUnits'])
        else:
            no_speed_edge_present = True

    # Normalize to use in color map
    #TODO: look at logarithmic scale here
    legend = None
    speed_colors = {}
    if len(speeds) > 0:
        speeds_norm = colors.normalize(0, len(speeds))

        legend = {}
        legend['shapes'] = []
        legend['labels'] = []
        for index, raw_speed in enumerate(sorted(speeds)):
            edge_color = colormap(speeds_norm(index))
            # store color for use when plotting
            speed_colors[raw_speed] = edge_color

            legend['shapes'].append( plt.Rectangle((0, 0), 0.51, 0.51, 
                                                fc = edge_color))
            legend['labels'].append( speed_labels[raw_speed])


        if no_speed_edge_present:
            legend['shapes'].append( plt.Rectangle((0, 0), 0.51, 0.51, 
                                                fc = default_edge_color)) 
            legend['labels'].append( "Unknown")


        # Smaller legend font
        fontP = FontProperties()
        fontP.set_size('small')

        #p1 =    p2 = plt.Rectangle((0, 0), 0.51, 0.51, fc="g")
        #p3 = plt.Rectangle((0, 0), 0.51, 0.51, fc="r")
        legend = plt.legend(legend['shapes'], legend['labels'],
                fancybox=True,
                ncol=6,
                prop = fontP,
                loc='upper center', bbox_to_anchor=(0.5, -0.05),
                )



    # Try Great Circle plot
    #TODO: also handle multigraphs - with key also
    if options.straight_line:
        #TODO: also handle colormap for non gc edges
        nx.draw_networkx_edges(G, pos, 
                               arrows = False, alpha = 0.8, width = 1,
                               linewidths = (0,0), font_weight = "bold",
                               font_size = 10, font_color = "w",
                               edge_color = default_edge_color) 
    else:
        for src, dst, data in G.edges(data=True):
            lon1 = G.node[src]['Longitude']
            lat1 = G.node[src]['Latitude']
            lon2 = G.node[dst]['Longitude']
            lat2 = G.node[dst]['Latitude']
            #print '\n' + G.node[src]['label'] + ' to ' + G.node[dst]['label']
            #print "{0} {1}, {2} {3}".format(lon1, lat1, lon2, lat2)

            # Set color based on normalised link speeds
            if 'LinkSpeedRaw' in data:
                edge_color = speed_colors[data['LinkSpeedRaw']]
            else:
                #TODO: handle this in legend
                edge_color = default_edge_color

            # Mark inferred links clearly
            linestyle = 'solid'
            if 'inferred' in data and data['inferred']:
                linestyle = 'dotted';

            if (lat1 == lat2) and (lon1 == lon2):
                # Case of same location, eg Perth1 Perth2
                continue

            # Generate small set of points to use for wrap-around check
            (x, y) = m.gcpoints(lon1, lat1, lon2, lat2, 15)
            #TODO Look how great circle decides how to go across or wrap around
            # and use this instead of lon1 lon2 comparison

            # Matplotlib can't handle wrap around of co-ords
            # Monotonicity check for wrap around eg
            # [32060855, 32615246, -2674010, -2101788]

            x_diff = np.diff(x)
            if np.all(x_diff > 0) or np.all(x_diff < 0) or np.all(x_diff == 0):
                # Either strictly increasing or strictly decreasing
                # No wrap
                # Draw normal great circle
                #print ("drawing gc for " + G.node[src]['label']
                #       + " " + G.node[dst]['label'])
                m.drawgreatcircle(lon1, lat1, lon2, lat2, color = edge_color,
                                  #linewidth=0.2,
                                  linestyle = linestyle,
                                  zorder=1)
            else:
                # Generate larger set of points to plot with
                (x, y) = m.gcpoints(lon1, lat1, lon2, lat2, 50)
                # Recalculate diff for new (bigger) set of co-ords
                x_diff = np.diff(x)

                # Work out if (aside from break point) increasing or decreasing
                # This changes the comparison to find the breakpoint 
                if lon1 < lon2:
                    # decreasing
                    break_index = np.nonzero( x_diff > 0)[0]
                else:
                    # increasing
                    break_index = np.nonzero( x_diff < 0)[0]
                # Compensate for the diff operation shifting values left one
                break_index += 1
                # TODO: could check that the highest in first half, and lowest 
                #in second half are close enough to the boundaries, if not then
                #interpolate or increasing the number of points otherwise get
                #big gap in the line 
                # Plot either side of this index
                #
                #TODO work out why internode cuts off across pacific 
                # - may need to interpolate
                #edge_color = 'r'
                m.plot(x[:break_index],y[:break_index], color = edge_color,
                       linestyle=linestyle)
                m.plot(x[break_index:],y[break_index:], color = edge_color,
                      linestyle=linestyle)
            
    nx.draw_networkx_nodes(G, pos, nodelist = geocoded_cities,
                           node_size = 15, 
                           alpha = 0.8, linewidths = (0,0),
                           node_color = node_color,
                           zorder=5)

    nx.draw_networkx_nodes(G, pos, nodelist = hyperedge_nodes,
                           node_size = 10, 
                           alpha = 0.8, linewidths = (0,0),
                           node_color = node_color,
                           node_shape='d',
                           zorder=5) 


  
    

    if options.labels:
        nx.draw_networkx_labels(G, pos, 
                                labels=labels,
                                font_size = 5,
                                font_color = font_color)

    if options.highlight_outliers:
        outliers = [n for n in G.nodes() if 'outlier' in G.node[n]]
        g_outliers = G.subgraph(outliers)
        #TODO make nodes scale to map size{{>Turkey}}
        nx.draw_networkx_nodes(G, pos,nodelist = outliers, node_size = 3,
                            labels = labels, 
                            arrows = False, alpha = 1, linewidths = (0,0), 
                            font_weight = "bold", font_size = 3,
                               font_color = "r",
                            node_color = "#336699" ) 


        nx.draw_networkx_edges(g_outliers, pos, labels = labels, 
                            with_labels = False,
                            arrows = False, alpha = 1, width = 0.3,
                            linewidths = (0,0), font_weight = "bold", 
                            font_size = 10, font_color = "w", 
                            edge_color ='#336699')  
   
    # Add title
    if options.title:
        network_date = G.graph['NetworkDate']
        if network_date == 'C' or network_date == 'D':
            network_date = G.graph['LastAccess']
            try:
                (day, month, year) = network_date.split("/")
                # Possible y2k bug here, but not many networks predate the 1960s!
                if year < 60:
                    year = "19" + year
                else:
                    year = "20" + year

                # Convert month from numeric to abbreviate name
                month = calendar.month_abbr[int(month)]

                #TODO: do cleanup for month in NetworkDate also
                network_date = "{0} {1}".format(month, year)
            except Exception, e:
                print e
                print "WARNING: No known date for " + G.graph['Network']
                network_date = "Unknown date"
        
        title_string =  "{0}, {1} ({2})".format(G.graph['Network'],
                                               G.graph['GeoLocation'],
                                               network_date)
        plt.title(title_string, color = 'k', fontsize = 10)


    #TODO: add www.topology-zoo.org to bottom of plots

    #TODO: work out why png sometimes doesn't work
    #TODO: increase jpeg compression level

    out_file = "{0}/{1}".format(output_path, network_name)
    # legend code from
    # http://www.mail-archive.com/matplotlib-users@lists.sourceforge.net/msg15262.html
    if options.pdf:
        plt_file_pdf = open(out_file + ".pdf", "w")
        if legend:
            plt.savefig( plt_file_pdf, format = 'pdf',
                        bbox_inches='tight',
                        facecolor = "w", dpi = 300,
                        pad_inches=0.1,
                        bbox_extra_artists = [legend.legendPatch],
                       )
        else:
            plt.savefig( plt_file_pdf, format = 'pdf',
                        bbox_inches='tight',
                        facecolor = "w", dpi = 300,
                        pad_inches=0.1,
                       )

    if options.eps:
        plt_file_eps = open(out_file + ".eps", "w")
        plt.savefig( plt_file_eps, format = 'eps', bbox_inches='tight',
                    facecolor = "w", dpi = 300, pad_inches=0)

    if options.jpg:
        plt_file_jpg = open(out_file + ".jpg", "w")
        plt.savefig( plt_file_jpg, bbox_inches='tight',
                    facecolor = "w", dpi = 300, pad_inches=0)

    if options.png:
        plt_file_png = open(out_file + ".png", "w")
        plt.savefig( plt_file_png, format = 'png',
                     bbox_inches='tight',
                    facecolor = "w", dpi = 300,
                    edgecolor='w',
                    #pad_inches=0
                   )

# try heatmap
if options.heatmap:
    plt.clf()   
    llcrnrlon = min(lons_all)       
    llcrnrlat = min(lats_all) 
    urcrnrlon = max(lons_all)
    urcrnrlat = max(lats_all)          
    margin_lon = 0.15 * abs(urcrnrlon - llcrnrlon)
    margin_lat = 0.15 * abs(urcrnrlat - llcrnrlat)      
    # and expand
    llcrnrlon -= margin_lon      
    llcrnrlat -= margin_lat
    urcrnrlon += margin_lon
    urcrnrlat += margin_lat

    # ensure that lat fits within (-90,90) once expanded
    llcrnrlat = max(llcrnrlat, -89.9)
    urcrnrlat = min(urcrnrlat, 89.9)

    lat_1 = (urcrnrlat + llcrnrlat)/2
    lon_0 = (urcrnrlon + llcrnrlon)/2

    #TODO: work out why errors happening
    try:
        m = Basemap(resolution ='c', projection='merc', llcrnrlat = llcrnrlat,
                    urcrnrlat = urcrnrlat,  llcrnrlon = llcrnrlon,
                    urcrnrlon = urcrnrlon, lat_ts = lat_1)
    except ZeroDivisionError, e:
        print "Error {0}".format(e)


    if options.bluemarble:
        m.bluemarble(scale = 0.8)
    else:
        m.drawcountries(linewidth = 0.2) 
        m.drawcoastlines(linewidth = 0.2)
        m.fillcontinents(color='k')

    lons, lats = m(lons_all, lats_all)

    m.scatter(lons, lats, s = 0.2, color='#FF8C00', 
              zorder=5,alpha=0.5, linewidths = (0,0))

    # and labels
    #for index, label in enumerate(labels_all):
    #    plt.text(lons[index], lats[index], label, size=1)

    out_file = "{0}/{1}".format(output_path, 'heatmap')
    plt_file_png = open(out_file + ".png", "w")
    #plt_file_jpg = open(out_file + ".jpg", "w")
    plt_file_pdf = open(out_file + ".pdf", "w")
    plt.savefig( plt_file_pdf, format = 'pdf', bbox_inches = 'tight',
                facecolor = "black", dpi = 300)

