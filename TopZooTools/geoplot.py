#! /usr/bin/env python

import networkx as nx 
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap as Basemap 
import os
import glob
import optparse
import sys
import math

from collections import defaultdict
import numpy as np
import matplotlib.cm as cm
import matplotlib.colors as colors
from matplotlib.font_manager import FontProperties

__all__ = ['plot_graph']

# Hide axes as per http://matplotlib.sourceforge.net/users/customizing.html
plt.rc('axes',linewidth=0)
#plt.rcParams['font.sans-serif'] = 'Helvetica'
#plt.rcParams['font.sans-serif'] = 'Times'
#TODO: allow this to be selectable from command line

#TODO: make this selectable from command line
# Use on Linux that needs more fonts
#plt.rc('text', usetex=True)

#TODO see if netx bug in multigraph when adding edge attributes 
# in reverse direction?

#look at doing mogrify from pdf for plots and avoid errors with png
#Look up clipping in matplotlib
#Fix gallery chmod error
#fix geoplot to use new cleaned up date - may still need processing for 
# string month name

#TODO: how to handle multiple edges when doing speed width plots?
#TODO: look at Plot/scatter position and marker size in the same coordinates -SO

#TODO: only call this from main rather than on script load

opt = optparse.OptionParser()
opt.add_option('--directory', '-d', help="process directory")
opt.add_option('--file', '-f', help="process file")
opt.add_option('--output_dir', '-o', help="process directory")
opt.add_option('--bluemarble', action="store_true",
               default=False, help="Use Blue Marble background image")

opt.add_option('--back_image', help="Image to use as background")
opt.add_option('--edge_label', help="attribute to use for edge_label")

opt.add_option('--pdf', action="store_true",
               default=False, help="Output to PDF format")
opt.add_option('--eps', action="store_true",
               default=False, help="Output to EPS format")
opt.add_option('--jpg', action="store_true",
               default=False, help="Output to JPG format")
opt.add_option('--png', action="store_true",
               default=False, help="Output to PNG format")

opt.add_option('--explode_scale', type="float", default=0,
                help="Scale for co-located nodes to be spaced out. Default 0"
               "(no explosion). Example range: 1-10")

opt.add_option('--expand_scale', type="float", default=1,
                help="Scale to expand map beyond outermost nodes"
               " Example range: 1-5")

opt.add_option('--external_node_scale', type="float", default=0,
                help="Scale for external nodes to be spaced out. Default 0"
               "(No external nodes plotted). Example range: 1-10")
opt.add_option('--labels', action="store_true",
               default=False, help="Print node labels")
opt.add_option('--numeric_labels', action="store_true",
               default=False, help="Use numeric node labels")

opt.add_option('--debuglabels', action="store_true",
               default=False, help="Print debugging (numeric) node labels")

opt.add_option('--no_watermark', action="store_false",
               default=True, help="Topology Zoo URL watermark")

opt.add_option('--res', help="Resolution level from 0 (none) to 5"
               " (full)", type="int", default=1)

opt.add_option('--node_size', help="Size to plot nodes as", type="float", default=10) 
opt.add_option('--image_scale', 
               help="Image quality level for bluemarble and warp images"
               "default 0 (none), 1 is high",
               type="float", default=0)

opt.add_option('--label_font_size', help="Size to plot nodes labels as", type="float", default=10)
opt.add_option('--country_color', help="Background color for countries", type="str", default="#336699")

opt.add_option('--line_width', help="Size to plot lines as", type="float", default=1) 
opt.add_option('--heatmap', action="store_true",
               default=False, help="Plot heatmap")
opt.add_option('--title', action="store_true",
               default=False, help="Add title to graph")
opt.add_option('--highlight_outliers', action="store_true",
               default=False, help="Highlight outlier nodes")
opt.add_option('--straight_line', action="store_true", default=False,
               help="Draw straight line between nodes instead of great circle")

opt.add_option('--edge_speeds', action="store_true", default=False,
               help="Plot edge speeds in different colours")

options, args = opt.parse_args()

#TODO: make mark outliers handle great circle - or disable option
# and warn user

#TODO: allow user to plot using normal scatter plot, ie not only basemap
#TODO: add option to mark outliers

#TODO: split into functions
def plot_graph(G, output_path, title=False, use_bluemarble=False,
               back_image = False,
               render_legend=False, explode_scale=0, use_labels=False,
               expand_scale = 1,
               numeric_labels=False, external_node_scale=0,
               opt_edge_speeds=False,
               basemap_resolution_level = 1,
               node_size = 10, line_width = 1,
               manual_image_scale = 0,
               pickle_dir = None,
               country_color = "#666666",
               label_font_size=10,
               no_watermark = False,
               show_figure=False,
               edge_font_size =3,
               edge_label_attribute=False, pdf=False, png=False):

    output_path = os.path.abspath(output_path)
    basemap_resolution_levels = {
        0: None,
        1: 'c',
        2: 'l',
        3: 'i',
        4: 'h',
        5: 'f'
    }
    if (basemap_resolution_level and basemap_resolution_level in
        basemap_resolution_levels):
        basemap_resolution = basemap_resolution_levels[basemap_resolution_level]
    else:
        basemap_resolution = basemap_resolution_levels[1]

    #TODO: make title allow to send through a string, or alternatively specify
    #to come from network name
    #TODO: clean up this handling when called programatically
    network_name = G.name
    if network_name == "":
        network_name = "network"  #default name
    #TODO: return labels_all or modify/append to reference if passed as
    #parameter as this is needed for heatmap labels
    labels_all = []
    lats_all = []
    lons_all = []

    #TODO: look at case where direct link A->B with geocoded co-ords
    # gets overwritten with inferred nodes if multiple paths between A-B

    # Remove any external nodes
    external_nodes = []
    if external_node_scale:
        external_nodes = [n for n, data in G.nodes(data=True)
                          if 'Internal' in data and data['Internal'] == 0]

    geocoded_cities = [ n for n, data in G.nodes(data = True)
                if 'Latitude' in data and 'Longitude' in data]

    #TODO make this command line argument and parameter
    if external_node_scale:
        # Find internal nodes connected to an external node
        boundary_nodes = nx.node_boundary(G, external_nodes)
        for bound_node in boundary_nodes:
            if ('Latitude' in G.node[bound_node] and 'Longitude' in
                G.node[bound_node]):
                bound_lat = G.node[bound_node]['Latitude']
                bound_lon = G.node[bound_node]['Longitude']
                # Find external neighbours
                ext_neighbors = [n for n in G.neighbors(bound_node) if n in
                                 external_nodes]
                for index, ext_neigh in enumerate(ext_neighbors):
                    theta = 2*math.pi*(float(index)/len(ext_neighbors))
                    radius = external_node_scale*1.0/10
                    x = radius * math.cos(theta)
                    y = radius * math.sin(theta)
                    G.node[ext_neigh]['Latitude'] = bound_lat + x
                    G.node[ext_neigh]['Longitude'] = bound_lon + y
            else:
                print "no lat/lon for %s (%s)" % (G.node[bound_node]['label'],
                                                  G.node[bound_node]['Network'])
                # Don't plot ext connections from this node
                ext_neighbors = [n for n in G.neighbors(bound_node) if n in
                                 external_nodes]
                for ext_neigh in ext_neighbors:
                    external_nodes.remove(ext_neigh)

    #TODO: make sure external nodes don't interfere with subsequent calculations
    # on geocoded nodes

    # Sanity check don't try to infer location if no geocoded places
    if len(geocoded_cities) == 0:
        print "No geocoded nodes in {0}, skipping".format(network_name)
        return 

    hyperedge_nodes = [ n for n, data in G.nodes(data = True)
                    if (('hyperedge' in data and data['hyperedge'] == 1)
                    and ('Internal' in data and data['Internal'] == 1))]

    # Handle non geocoded places, as give misleading appearance of disconnected
    # graph
    non_geocoded_cities = [n for n in G.nodes() 
                           if (n not in geocoded_cities
                               and n not in hyperedge_nodes
                              and n not in external_nodes)]
    for n in non_geocoded_cities:
        if G.degree(n) < 2:
            # Single node eg ---0
            #TODO: handle this
            G.remove_node(n)
        elif G.degree(n) == 2:
            # Node in chain eg ----0-----
            # replace with edge from neighbors
            neigh = G.neighbors(n)
            # Keep track of removed nodes along this edge eg if had
            # --0--0--0-- then 3 nodes removed which need to interpolate later
            #TODO: this may re-order nodes along a link - may be bad if using
            # labels
            removed_nodes = {} 
            for s, t, data in G.edges(n, data=True):
                if 'removed_nodes' in data:
                    removed_nodes.update(data['removed_nodes'])
            # Store the node data for this node also
            removed_nodes[n] = G.node[n] 
            #TODO: mark as diff type so can dash accordingly when plotting
            G.add_edge(neigh[0], neigh[1], inferred=True,
                    removed_nodes = removed_nodes)
            G.remove_node(n)
        elif G.degree(n) > 2:
            for neigh in G.neighbors(n):
                G[n][neigh]['inferred'] = True
            # Treat as a hyperedge
            #TODO: mark as normal node rather than hyperedge
            hyperedge_nodes.append(n)

    inferred_hyperedge_present = False

    # work out how to do multiple hyperedges connected to each other eg deltacom
    #toDO: make this option argument command line-able
    # TODO: make this non-geocoded nodes rather than hyperedge nodes
    for node in hyperedge_nodes:
        he_lats = []
        he_lons = []
        for neigh in G.neighbors(node):
            if 'Latitude' in G.node[neigh]:
                he_lats.append( G.node[neigh]['Latitude'] )
            if 'Longitude' in G.node[neigh]:
                he_lons.append( G.node[neigh]['Longitude'] )
        if len(he_lats) > 0 and len(he_lons) > 0:
            G.node[node]['Latitude'] = np.mean(he_lats)
            G.node[node]['Longitude'] = np.mean(he_lons)
        else:
            #TODO: this isn't used, comment out, give warning for now
            # Replace with inferred edges
            # This occurs when two deg(>2) nodes are connected and have no
            # geo-information
            inferred_hyperedge_present = True
            neigh_list = G.neighbors(node)
            for n_a in neigh_list:
                for n_b in neigh_list:
                    if n_a != n_b:
                        G.add_edge(n_a, n_b, inferred=True)
            G.remove_node(node)

    #TODO: note that if node removed, it loses its label which won't appear if
    # using --labels command line argument
    # add back in the nodes
    inferred_nodes = []
    for s, t, edge_data in G.edges(data=True):
        if 'removed_nodes' in edge_data:
            removed_nodes = edge_data['removed_nodes']
            removed_count = len(removed_nodes)
            # now interpolate
            # eg if 2 nodes, want to space 1/(2+1) = 1/3 of way along
            del_x = (G.node[s]['Latitude'] -
                    G.node[t]['Latitude'])/(removed_count+1)
            del_y = (G.node[s]['Longitude'] - 
                    G.node[t]['Longitude'])/(removed_count+1)

            # Add node(s) back in
            index = 1
            for node, data in removed_nodes.items():
                data['Latitude'] = G.node[s]['Latitude'] - (index * del_x)
                data['Longitude'] = G.node[s]['Longitude'] - (index * del_y)
                G.add_node(node, data)
                index += 1
                # Plot as an inferred node
                inferred_nodes.append(node)

            # Remove edge, will be replaced by edge between nodes
            G.remove_edge(s,t)
            # List of nodes to reconnect, form s--rem[0]--rem[1]--....--t
            reconnect_list = [s] + removed_nodes.keys() + [t]
            # Join as pairs 
            # based on http://stackoverflow.com/q/2829887
            for (a, b) in zip(reconnect_list, reconnect_list[1:]):
                G.add_edge(a, b, edge_data) 


    if inferred_hyperedge_present:
        # Awaiting feedback from supervisor meeting
        print "Inferred hyperedges present"
        #return

    # Remove cities with no co-ords
    #TODO: see if this makes a difference
    #G = G.subgraph(geocoded_cities)
    # Get only hyperedge nodes in graph
    # TODO: see if can use nbunch in networkx
    hyperedge_nodes = [n for n in hyperedge_nodes if n in G]

    #TODO: make sure that moving nodes won't wreck stats in lats/lons

    #TODO: check why no showing up as multiple inferred hyperedges in Uninett
    if explode_scale:
        lat_long = defaultdict(list)
        for n, data in G.nodes(data=True):  
            lat_long[(data['Latitude'], data['Longitude'])].append(n)
        coincident_nodes = [n for n in lat_long.values() if len(n) > 1]
        lons = [lon for (lat, lon) in lat_long.keys()]
        # Shrink explode scale
        scale_factor = float(explode_scale)/100 * (max(lons) - min(lons))
        for nodes in coincident_nodes:
            # Add to the lat/long accordingly
            # Keep first node in same place
            # Sort by label
            nodes = sorted(nodes, key = lambda x: G.node[x]['label'])
            for index, n in enumerate(nodes):
                G.node[n]['Longitude'] += index * scale_factor
                G.node[n]['Latitude'] -= index * scale_factor

    lats = []
    lons = []     
    latlon_node_index = []
    for n, data in G.nodes(data=True):  
        lats.append(data['Latitude'])                 
        lons.append(data['Longitude'])
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
        return

    # store for heatmap
    # TODO: check on fastest way to do the append to list
    lats_all += lats
    lons_all += lons

    # corner lats and lons
    llcrnrlon = min(lons)       
    llcrnrlat = min(lats) 
    urcrnrlon = max(lons)
    urcrnrlat = max(lats)          

    # expand so have frame around border nodes   
    # expand by % of distance between border nodes
    width_expand_factor = height_expand_factor = 1.0 * expand_scale/10

    # Zoom out if small map (threshold found empirically)
    map_width = abs(urcrnrlon - llcrnrlon)
    map_height = abs(urcrnrlat - llcrnrlat)

    # make minimum width so don't get long skinny map
    if 4*map_width < map_height:
        width_expand_factor = 4*width_expand_factor
    elif 2*map_width < map_height:
        width_expand_factor = 2*width_expand_factor
    elif 4*map_height < map_width:
        height_expand_factor = 4*height_expand_factor
    # removed this as can cause some US maps to be not plotted
    #elif 2*map_height < map_width:
    #    print 4
    #    height_expand_factor = 2*height_expand_factor

    image_scale = 0.5
    if (map_width * map_height ) < 1:
        width_expand_factor =  width_expand_factor*10
        height_expand_factor = height_expand_factor*10
        image_scale = 0.9
    elif (map_width * map_height ) < 5:
        width_expand_factor =  width_expand_factor*4
        height_expand_factor = height_expand_factor*4
        image_scale = 0.8
    elif (map_width * map_height ) < 10:
        width_expand_factor =  width_expand_factor*3
        height_expand_factor = height_expand_factor*3
        image_scale = 0.7
    elif (map_width * map_height ) < 20:
        width_expand_factor =  width_expand_factor*2.5
        height_expand_factor = height_expand_factor*2.5
        image_scale = 0.65
    elif (map_width * map_height ) < 50:
        width_expand_factor =  width_expand_factor*2
        height_expand_factor = height_expand_factor*2
        image_scale = 0.65
    elif (map_width * map_height ) < 100:
        width_expand_factor =  width_expand_factor*1.5
        height_expand_factor = height_expand_factor*1.5
        image_scale = 0.6
    elif (map_width * map_height ) < 200:
        width_expand_factor =  width_expand_factor*1
        height_expand_factor = height_expand_factor*1
        image_scale = 0.5

    # Quality scaling factor for large blue marble maps
    #TODO: merge with above scaling
    if use_bluemarble or back_image:
        if (map_width * map_height) > 4000:
            image_scale = 0.25
        elif (map_width * map_height) > 1000:
            image_scale = 0.4
    
    # manual boundaries for geant
    # left, bottom, right, top
    if G.graph.get('Network') == 'European NRENs':
        (llcrnrlon, llcrnrlat, urcrnrlon, urcrnrlat) = (-6, 35, 35, 58)

    # over write if specified manually
    if manual_image_scale:
        image_scale = manual_image_scale

    margin_lon = width_expand_factor * abs(urcrnrlon - llcrnrlon)
    margin_lat = height_expand_factor * abs(urcrnrlat - llcrnrlat)      

    # and expand
    llcrnrlon -= margin_lon      
    llcrnrlat -= margin_lat
    urcrnrlon += margin_lon
    urcrnrlat += margin_lat

    # Stop wrapping around at date-line, due to expansions above
    llcrnrlon = max(llcrnrlon, -179)
    urcrnrlon = min(urcrnrlon, 179)

    # ensure that lat fits within (-90,90) once expanded
    # Use 85 as boundary, as mercator becomes very distorted when near poles
    # We don't have many (ant)artic maps
    llcrnrlat = max(llcrnrlat, -70)
    urcrnrlat = min(urcrnrlat, 85)

    lat_1 = (urcrnrlat + llcrnrlat)/2
    lon_0 = (urcrnrlon + llcrnrlon)/2
    #TODO: see if can clear old figures rather than new one each time
    #TODO: see if stil need this try catch block now do stdev check

    try:
        # Draw the map 
        plt.clf()
        fig = plt.figure()
        # Create axes to allow adding of text relative to map
        ax = fig.add_subplot(111)
        m = Basemap(resolution = basemap_resolution,
                    projection='merc', llcrnrlat = llcrnrlat,
                    urcrnrlat = urcrnrlat,  llcrnrlon = llcrnrlon,
                    urcrnrlon = urcrnrlon, lat_ts = lat_1)

        """
        TODO: remove cache code
        map_id = hash(
            hash(basemap_resolution) ^ hash(llcrnrlat) ^ hash(llcrnrlon) ^
            hash(urcrnrlat) ^ hash(urcrnrlon) ^ hash(lat_1))
        # remove - from filename
        if map_id < 0:
            map_id = -1*map_id
        pickle_file = "{0}/{1}.pickle".format(pickle_dir, map_id)
        if (os.path.isfile(pickle_file)):
            # Pickle file exists
            m = cPickle.load(open(pickle_file,'rb'))
        else:
            m = Basemap(resolution = basemap_resolution,
                        projection='merc', llcrnrlat = llcrnrlat,
                        urcrnrlat = urcrnrlat,  llcrnrlon = llcrnrlon,
                        urcrnrlon = urcrnrlon, lat_ts = lat_1)
            if pickle_dir:
                # See if cached basemap for this file
                cPickle.dump(m,open(pickle_file,'wb'))
        """

    except ZeroDivisionError, e:
        print "Error {0}".format(e)
        # Do the next map
        return
        
    # Convert lats and lons into x,y for plotting
    mx, my = m(lons, lats)

    pos = {} 
    for index in range(len(mx)):
        # Extract the converted lat lons for each node
        node = latlon_node_index[index]
        pos[node] = (mx[index], my[index])
                                
    # and labels
    if use_labels or numeric_labels:
        labels = {}
        for n, data in G.nodes(data = True):
            #TODO: make numeric labels an option
            """
            if 'Network' in data and data['Network'] == 'GEANT':
                use_labels = True
                labels[n] = data['label']

            continue
        """
            if numeric_labels:
                labels[n] = n
            else:
                labels[n] =  data['label']

    if numeric_labels:
        print "Labels:"
        for n, data in G.nodes(data = True):
            print "%s: %s" % (n, data['label'])

    if not country_color.startswith("#"):
        country_color = "#" + country_color

    if use_bluemarble:
        m.bluemarble(scale = image_scale)
        #m.bluemarble()
    elif back_image:
        #TODO: while loop to catch memory error and try with lower scale
        #image_scale = 0.8
        print "warp scale %s " % image_scale
        m.warpimage(image = back_image,
                   scale = image_scale)
    else:
        #TODo: fix bug where if using straight line it gets drawn under
        # continent fill
        if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
            m.fillcontinents(color='#9ACEEB')
        elif G.graph.get('Network') == 'European NRENs':
            m.fillcontinents(color='#9ACEEB')
        else:
            m.fillcontinents(color=country_color)

        #m.fillcontinents()

    # Colours depending on if using bluemarble image or white background
    if use_bluemarble or back_image:
        node_color ="#FF8C00"
        font_color = "w"
        default_edge_color = "#bbbbbb"
        title_color = "w"
        caption_color = 'w'
        colormap = cm.autumn
        #colormap = cm.jet
        #country_color = '#666666'
    else:
        if use_labels:
            node_color = 'gray'
        else:
            node_color = "k"
        font_color = "k"
        default_edge_color = "#338899"
        default_edge_color = "#777777"
        #default_edge_color = "0.8"
        title_color = "k"
        caption_color = 'gray'
        colormap = cm.jet
        #colormap = cm.autumn
        #colormap = cm.winter
        #colormap = cm.summer
        #country_color = '#AAAAAA'
        #country_color = '#DDDDDD'
        if G.graph.get('Network') == 'GEANT':
            default_edge_color = '0.3'
        elif G.graph.get('Network') == 'European NRENs':
            default_edge_color = '0.1'

    if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
        pass
    else:
        m.drawcountries(linewidth = 0.05, zorder=1.5, color = country_color)
        #m.drawcoastlines(linewidth = 0.1, zorder=1, color = country_color)

    #TODO: look at pickling and using
    """
        states = LineCollection(self.statesegs,antialiaseds=(antialiased,))
        states.set_color(color)
        states.set_linewidth(linewidth)
        states.set_label('_nolabel_')
        if zorder is not None:
            states.set_zorder(zorder)
        ax.add_collection(states)
        # set axes limits to fit map region.
        self.set_axes_limits(ax=ax)
        return states         
    """

    """
    Colormap based on
    http://www.packtpub.com/article/plotting-data-using-matplotlib-part2
    and http://fatweasel.com/tutorials/python-tutorials/
        matplotlib/histogram-with-normalized-data/
    and
    http://stackoverflow.com/q/4700614/
    """

    render_legend = False
    legend = {}
    legend['shapes'] = []
    legend['labels'] = []

    if opt_edge_speeds:
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
            elif 'LinkSpeedRaw' not in data:
                no_speed_edge_present = True

        # Normalize to use in color map
        #TODO: look at logarithmic scale here
        #legend = None
        speed_colors = {}

        if len(speeds) > 0:
            render_legend = True
            speeds_norm = colors.normalize(0, len(speeds))

            for index, raw_speed in enumerate(sorted(speeds)):
                edge_color = colormap(speeds_norm(index))
                # store color for use when plotting
                speed_colors[raw_speed] = edge_color

                #TODO: make legend size relate to node size so don't have big
                # legend

                legend['shapes'].append( plt.Rectangle((0, 0), 0.51, 0.51, 
                                                    fc = edge_color))
                legend['labels'].append( speed_labels[raw_speed])


            if no_speed_edge_present:
                legend['shapes'].append( plt.Rectangle((0, 0), 0.51, 0.51, 
                                                    fc = default_edge_color)) 
                legend['labels'].append( "Unknown")
    """

    #TODO: add hyperedge and inferred nodes to list if present
    #TODO: work out why plt.scatter(0,0) gives 3 points in the legend
    #if len(inferred_nodes) > 0 :
    #    render_legend = True
    #    legend['shapes'].append(plt.scatter([1], [1], s=10, marker='s'))
    #    #legend['shapes'].append( plt.Line2D([0,5], [0,5], marker='d',
    #    #                                   markevery=2) )
    #    legend['labels'].append( "Inferred position")
    """

    if render_legend:
        # Smaller legend font
        fontP = FontProperties()
        fontP.set_size('small')

        #p1 =    p2 = plt.Rectangle((0, 0), 0.51, 0.51, fc="g")
        #p3 = plt.Rectangle((0, 0), 0.51, 0.51, fc="r")
        legend = plt.legend(legend['shapes'], legend['labels'],
                fancybox=True,
                ncol=4,
                prop = fontP,
                loc='upper center', bbox_to_anchor=(0.5, -0.05),
                )

    # Try Great Circle plot
    #TODO: also handle multigraphs - with key also
    """TODO: remove this
    if opt_straight_line:
        #TODO: also handle colormap for non gc edges
        nx.draw_networkx_edges(G, pos, 
                            arrows = False, alpha = 0.8, width = 1,
                            linewidths = (0,0), font_weight = "bold",
                            font_size = 10, font_color = "w",
                            edge_color = default_edge_color) 
                            """
    if False:
        pass
    else:
        delta_colors = { 
                'added': '#339966', 'removed': 'r', 'modified': '#3366ff', 
                '': node_color} # used default color if no delta
        for src, dst, data in G.edges(data=True):
            if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
                # Hacky way to not plot edge for IL and RU
                edge_skips_nodes = set(["RU", "IL"])
                if (G.node[src]['label'] in edge_skips_nodes 
                    or G.node[dst]['label'] in edge_skips_nodes):
                    continue

            lon1 = G.node[src]['Longitude']
            lat1 = G.node[src]['Latitude']
            lon2 = G.node[dst]['Longitude']
            lat2 = G.node[dst]['Latitude']
            #print '\n' + G.node[src]['label'] + ' to ' + G.node[dst]['label']
            #print "{0} {1}, {2} {3}".format(lon1, lat1, lon2, lat2)

            # Set color based on normalised link speeds
            #TODO: allow user to turn off using edge_color from the source graph
            if 'edge_color' in data:
                edge_color = data['edge_color']
            elif opt_edge_speeds and 'LinkSpeedRaw' in data:
                edge_color = speed_colors[data['LinkSpeedRaw']]
            else:
                #TODO: handle this in legend
                edge_color = default_edge_color

            if 'delta' in data:
                edge_color = delta_colors[data['delta']]

            if 'zorder' in data:
                zorder = data['zorder']
            else:
                zorder = 1
            
            if 'edge_width' in data:
                # Multiplier not absolute width
                #line_width = line_width * int(data['edge_width'])
                curr_line_width = line_width * int(data['edge_width'])
            else:
                curr_line_width = line_width

            # Mark inferred links clearl
            linestyle = 'solid'
            if 'inferred' in data and data['inferred']:
                linestyle = 'dotted'

            if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
                #linestyle = 'dashed'
                pass

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
                                  linewidth=curr_line_width,
                                  #alpha = 0.6,
                                  linestyle = linestyle,
                                  #dashes=(4,1),
                                  zorder=zorder)
                """
                m.drawgreatcircle(lon1, lat1, lon2, lat2, color='w',
                                    linewidth=2.5,
                                    alpha=0.1,
                                    linestyle = linestyle,
                                    zorder=1.2)
                                    """
            else:
                """
                if opt_edge_label:
                    #TODO: put label along edge from a datapoint in the
                    #middle
                    print ("Warning: edge label not currently supported for"
                            " links that wrap around")
                            """
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
                if len(break_index) > 1:
                    print "Error: edge crosses map boundary more than once"
                    continue
                m.plot(x[:break_index],y[:break_index], color = edge_color,
                       linewidth = curr_line_width,
                       linestyle=linestyle, zorder=zorder)
                m.plot(x[break_index:],y[break_index:], color = edge_color,
                       linewidth = curr_line_width,
                       linestyle=linestyle, zorder=zorder)
    # Appears to be zorder issue with plot, if edge draw with zorder>1 it
    # will appear on top of the nodes despite node zorder provided


    # Use non integer zorder for the edges, as netx draw forces zorder to be
    # 2 for nodes

    # Faded background node for glow-like effect
    """
    plotted_nodes = nx.draw_networkx_nodes(G, pos, 
                        nodelist = geocoded_cities,
                        node_size = 25, 
                        alpha = 0.2, linewidths = (0,0),
                        node_color = node_color)
                        """

    if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
        node_size = 50 
        node_color = 'k'
        # Custom labels
        # also do custom legend
        router_nodes = [n for n, d in G.nodes_iter(data=True)
                        if d['type'] in set(['Fully Featured',
                                             'IP/MPLS only',
                                             'Off fibre net'])]
        nx.draw_networkx_nodes(G, pos, nodelist = router_nodes,
                               node_size = node_size, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = 'r',
                               node_shape='s',) 

        routerless_nodes = [n for n, d in G.nodes_iter(data=True)
                            if d['type'] in set(['Routerless',
                                                 'Routerless Off Fibre net'])]
        nx.draw_networkx_nodes(G, pos, nodelist = routerless_nodes,
                               node_size = node_size, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = 'b',
                               node_shape='o',) 

        nren_pop_nodes = [n for n, d in G.nodes_iter(data=True)
                          if d['type'] == "NREN POPs"]
        nx.draw_networkx_nodes(G, pos, nodelist = nren_pop_nodes,
                               node_size = node_size, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = node_color,
                               node_shape='^',) 

        nordu_nodes = [n for n, d in G.nodes_iter(data=True)
                       if d['type'] == "NORDUnet"]
        nodes = nx.draw_networkx_nodes(G, pos, nodelist = nordu_nodes,
                               node_size = node_size, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = 'g',
                               node_shape='d',) 

        # Make sure legend included
        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='s', c='r', label='Routers')
        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='o', c='b', label='Routerless')
        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='^', c='k', label='NREN PoPs')
        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='d', c='g', label='NORDUnet')
        fontP = FontProperties()
        fontP.set_size('small')

        #p1 =    p2 = plt.Rectangle((0, 0), 0.51, 0.51, fc="g")
        #p3 = plt.Rectangle((0, 0), 0.51, 0.51, fc="r")
        plt.legend(ncol=2, loc='lower left', prop = fontP,
                  scatterpoints=1)

    elif 'Network' in G.graph and G.graph['Network'] == 'European NRENs':
        #node_size = 50 
        node_color = 'k'
        # Custom labels
        # also do custom legend
        geant_nodes = [n for n, d in G.nodes_iter(data=True)
                        if d['Network'] == 'GEANT']
        non_geant_nodes = set(G.nodes()) - set(geant_nodes)
        nx.draw_networkx_nodes(G, pos, nodelist = non_geant_nodes,
                               node_size = 2, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = 'g',
                               node_shape='o',) 

        # Ensure on top
        nx.draw_networkx_nodes(G, pos, nodelist = geant_nodes,
                               node_size = 20, 
                               #alpha = 0.8, 
                               linewidths = (0,0),
                               node_color = 'r',
                               node_shape='d',) 

        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='d', c='r', label='GEANT Router PoP')
        plt.scatter(-100000,-1000, s=50, linewidths = (0,0),
                    marker='o', c='g', label='Node')
        fontP = FontProperties()
        fontP.set_size('small')

        #p1 =    p2 = plt.Rectangle((0, 0), 0.51, 0.51, fc="g")
        #p3 = plt.Rectangle((0, 0), 0.51, 0.51, fc="r")
        plt.legend(ncol=2, loc='lower left', prop = fontP,
                  scatterpoints=1)

    else:
        #plot delta colors
        delta_colors = { 
                'added': '#339966', 'removed': 'r', 'modified': '#3366ff', 
                '': node_color} # used default color if no delta
        node_deltas = [d.get("delta") for n,d in G.nodes(data=True)]
        node_color = [delta_colors[delta] for delta in node_deltas]
        plotted_nodes = nx.draw_networkx_nodes(G, pos, 
                                               nodelist = geocoded_cities,
                                               node_size = node_size, 
                                               #alpha = 0.8, 
                                               linewidths = (0,0),
                                               node_color = node_color)



        nx.draw_networkx_nodes(G, pos, nodelist = hyperedge_nodes,
                                node_size = node_size, 
                                #alpha = 0.8, 
                                linewidths = (0,0),
                                node_color = node_color,
                                node_shape='d',) 

        if external_node_scale:
            nx.draw_networkx_nodes(G, pos, nodelist = external_nodes,
                                node_size = node_size, 
                                #alpha = 0.8, 
                                linewidths = (0,0),
                                node_color = 'r',
                                node_shape='^',) 

        nx.draw_networkx_nodes(G, pos, nodelist = inferred_nodes,
                            node_size = node_size, 
                            #alpha = 0.8, 
                            linewidths = (0,0),
                            node_color = node_color,
                            node_shape='s')  

    if use_labels:
        if 'Network' in G.graph and G.graph['Network'] == 'GEANT':
            # hacky way to put labels on left or right
            label_pos = dict( (key, (x+140000, y)) for key, (x,y)
                             in pos.items())
            # Move the left labels to the other side
            left_side_labels = set(['IE', 'RU', 'SE', 'NO',
                                    'UK', 'PT', 'IT', 'SL',
                                    'ME', 'MT', 'AT', 'FR', 'LU'])
            for index, label in labels.items():
                if label in left_side_labels:
                    (curr_x, curr_y) = label_pos[index]
                    label_pos[index] = (curr_x - 2*160000, curr_y)

            # extra custom offsets
            extra_custom_offsets = {
                'AT': (-50000, 0),
                'BG': (70000, 20000),
                'CZ': (50000, 0),
                'DE': (0, 80000),
                'DK': (50000, 0),
                'ES': (50000, 0),
                'GR': (-120000, -150000),
                'HR': (-200000, -120000),
                'HU': (80000, -10000),
                'IT': (100000, -150000),
                'LU': (150000, -120000),
                'ME': (0, -50000),
                'MK': (0, -60000),
                'NL': (-250000, 140000),
                'PL': (10000, -2000),
                'RO': (50000, 0),
                'RS': (50000,0),
                'SK': (0, 60000),
                'SL': (50000, 0),
                'UK': (-5000, 0),
                'UK': (0, -5000),
            }
            for index, label in labels.items():
                if label in extra_custom_offsets:
                    (curr_x, curr_y) = label_pos[index]
                    label_pos[index] = (curr_x + extra_custom_offsets[label][0],
                                        curr_y + extra_custom_offsets[label][1])


        elif 'Network' in G.graph and G.graph['Network'] == 'GARR' and False:
            #TODO: see if this is still needed
            # hacky way to put labels on left or right
            label_pos = dict( (key, (x+40000, y)) for key, (x,y)
                             in pos.items())
            # Move the left labels to the other side
            left_side_labels = set(['NA', 'RM-1',
                                   'PL', 'CA', 'PA', 'PD', 'MI',
                                   ])
            remove_labels = set(['RM-2', 'CA-1', 'MI-2', 'MI-3',
                                 'MI-4', 'CO', 'PD-2', 'AQ-1'])
            custom_relabels = {'RM-1': 'RM',
                               'MI-1': 'MI',
                               'Fi': '',
                               'TS-1': 'TS'}
            for index, label in labels.items():
                if label in left_side_labels:
                    (curr_x, curr_y) = label_pos[index]
                    label_pos[index] = (curr_x - 2*40000, curr_y)
                if label in remove_labels:
                    labels[index] = ''
                if label in custom_relabels:
                    labels[index] = custom_relabels[label]

        else:
            label_pos = pos

        nx.draw_networkx_labels(G, label_pos, 
                                labels=labels,
                                font_size = label_font_size,
                                font_color = font_color,
                               )

        #TODO: rewrite using 
#edge_labels = dict( ((s,t), d.get(edge_label_attribute)) for s,t,d in G.edges(data=True))
    if edge_label_attribute:
        edge_labels = {}
        for s, t, d in G.edges_iter(data=True):
            if edge_label_attribute in d:
                edge_labels[(s,t)] = d[edge_label_attribute]
            else:
                # blank entry
                edge_labels[(s,t)] = ""

        # NetworkX will automatically put a box behind label, make invisible
        # by setting alpha to zero
        bbox = dict(boxstyle='round',
                    ec=(1.0, 1.0, 1.0, 0),
                    fc=(1.0, 1.0, 1.0, 0.0),
                    )       
        
        nx.draw_networkx_edge_labels(G, pos, edge_labels, font_size=edge_font_size,
                                    bbox = bbox)

        """
        nx.draw_networkx_edges(g_outliers, pos, labels = labels, 
                            with_labels = False,
                            arrows = False, alpha = 1, width = 0.3,
                            linewidths = (0,0), font_weight = "bold", 
                            font_size = 10, font_color = "w", 
                            edge_color ='#336699') 
                                    """
    if no_watermark:
        ax.text(0.99, 0.01, 'www.topology-zoo.org',
                horizontalalignment='right',
                fontsize=8, color=caption_color, alpha=0.7,
                verticalalignment='bottom',
                transform=ax.transAxes)

    # Add title
    if title:
        network_date = ""
        network_date = G.graph.get('NetworkDate').replace("_", " ")
        place_date_string = "%s\n%s" % (G.graph.get("GeoLocation"),
                                        network_date)

        network_title = ax.text(0.02, 0.98, 
                G.graph['Network'],
                horizontalalignment='left',
                weight='heavy',
                fontsize=16, color=title_color,
                verticalalignment='top',
                transform=ax.transAxes)

        ax.text(0.98, 0.98, 
                place_date_string,
                horizontalalignment='right',
                fontsize=10, color=title_color,
                verticalalignment='top',
                transform=ax.transAxes)

    out_file = "{0}/{1}".format(output_path, network_name.replace(" ", "_"))
    #logger.info( "Plotting to %s " % out_file)
    # legend code from
    # http://www.mail-archive.com/matplotlib-users@lists.sourceforge.net/msg15262.html
    if pdf:
        plt_file_pdf = open(out_file + ".pdf", "w")
        #logger.info( "Plotting %s.pdf" % out_file)
        if render_legend:
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
                        pad_inches=0,
                    )
    """
    if opt_eps:
        plt_file_eps = open(out_file + ".eps", "w")
        plt.savefig( plt_file_eps, format = 'eps', bbox_inches='tight',
                    facecolor = "w", dpi = 300, pad_inches=0)

    if opt_jpg:
        plt_file_jpg = open(out_file + ".jpg", "w")
        plt.savefig( plt_file_jpg, bbox_inches='tight',
                    facecolor = "w", dpi = 300, pad_inches=0)

    """
    if png:
        plt_file_png = open(out_file + ".png", "w")
        #logger.info( "Plotting %s.png" % out_file)
        plt.savefig( plt_file_png, format = 'png',
                    bbox_inches='tight',
                    facecolor = "w", dpi = 400,
                    # If pad_inches set to 0, often countries and edges
                    # not shown
                    #TODO: make command line argument to set pad inches
                    # to work around plot error - and document 
                    pad_inches=0.001,
                )

    if show_figure:
        plt.show()

    #plt.close()


#TODO: replace magic numbers with constants
def main():
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

    # Don't want to write into root directory
    path = os.path.abspath(path)

    if options.output_dir:
        output_path = options.output_dir 
    else:
        output_path = path + "/plotted"
        # And append the map type to the directory
        if options.bluemarble:
            output_path += "_bm"
        elif options.back_image:
            output_path += "_img"
        else:
            output_path += "_flat"

    if not os.path.isdir(output_path):
        os.mkdir(output_path)  

    pickle_dir = path + os.sep + "cache"
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    #toDO: See if can warn about nodes outside map area

    #TODO: add logger debug

    #TODO: add support for legend through options

    lats_all = []
    lons_all = []
    labels_all = []
    #TODO: place hyperedges on map halfway between the nodes they relate to

    for index, net_file in enumerate(sorted(network_files)):
        # Extract name of network from file path
        path, filename = os.path.split(net_file)
        network_name, extension = os.path.splitext(filename)
        print "Plotting: %s (%s/%s)"%(network_name,
                                      # Index starts at 0, offset else get 0/1
                                      index+1,
                                      len(network_files))
        print "reading %s " % net_file

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
        #TODO: check what effect this has on link speeds if multiple edges between
        # node pair
        G = nx.Graph(G)
        G.name = network_name

        plot_graph(G, output_path,
                   title=options.title, 
                   use_bluemarble=options.bluemarble,
                   back_image = options.back_image,
                   explode_scale=options.explode_scale,
                   expand_scale = options.expand_scale,
                   use_labels=options.labels,
                   edge_label_attribute=options.edge_label,
                   external_node_scale=options.external_node_scale,
                   numeric_labels = options.numeric_labels,
                   opt_edge_speeds = options.edge_speeds,
                   basemap_resolution_level = options.res,
                   node_size = options.node_size,
                   line_width = options.line_width,
                   manual_image_scale = options.image_scale,
                   label_font_size = options.label_font_size,
                   pdf=options.pdf,
                   country_color = options.country_color,
                   no_watermark = options.no_watermark,
                   pickle_dir=pickle_dir,
                   png=options.png,
            )

    # try heatmap
    if options.heatmap:
        print "Plotting heatmap"
        plt.clf()
        fig = plt.figure()
        # Create axes to allow adding of text relative to map
        ax = fig.add_subplot(111)

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

        llcrnrlon = max(llcrnrlon, -179)
        urcrnrlon = min(urcrnrlon, 179)

        llcrnrlat = max(llcrnrlat, -60)
        urcrnrlat = min(urcrnrlat, 75)

        lat_1 = (urcrnrlat + llcrnrlat)/2
        lon_0 = (urcrnrlon + llcrnrlon)/2

        # Draw the map 


        #TODO: work out why errors happening
        try:
            m = Basemap(resolution ='c', projection='merc', 
                        llcrnrlat = llcrnrlat,
                        urcrnrlat = urcrnrlat,  llcrnrlon = llcrnrlon,
                        urcrnrlon = urcrnrlon, lat_ts = lat_1, ax=ax)
        except ZeroDivisionError, e:
            print "Error {0}".format(e)

        if options.bluemarble:
            m.bluemarble(scale = 0.8)
        else:
            m.drawlsmask(land_color='coral',ocean_color='aqua',lakes=True)
            m.drawcountries(linewidth = 0.2) 
            m.drawcoastlines(linewidth = 0.2)
            m.fillcontinents(color='k')

        lons, lats = m(lons_all, lats_all)

        m.scatter(lons, lats, s = 0.8, color='#FF8C00', 
                zorder=5,alpha=0.8, linewidths = (0,0))

        # and labels
        #for index, label in enumerate(labels_all):
        #    plt.text(lons[index], lats[index], label, size=1)


        #ax.text(0.02, 0.98, 
        #        "Coverage",
        #        horizontalalignment='left',
        #        weight='heavy',
        #        fontsize=16, color='w',
        #        verticalalignment='top',
        #        transform=ax.transAxes)

        ax.text(0.99, 0.01, 'www.topology-zoo.org',
                horizontalalignment='right',
                fontsize=8, color='w', alpha=0.7,
                verticalalignment='bottom',
                transform=ax.transAxes)


        out_file = "{0}/{1}".format(output_path, 'heatmap')
        if options.pdf:
            plt_file_pdf = open(out_file + ".pdf", "w")
            plt.savefig( plt_file_pdf, format = 'pdf',
                        bbox_inches='tight',
                        facecolor = "w", dpi = 300,
                        pad_inches=0.001,
                    )


        if options.png:
            plt_file_png = open(out_file + ".png", "w")
            plt.savefig( plt_file_png, format = 'png',
                        bbox_inches='tight',
                        facecolor = "w", dpi = 400,
                        # If pad_inches set to 0, often countries and edges
                        # not shown
                        pad_inches=0.001
                    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
