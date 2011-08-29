#! /usr/bin/env python

import networkx as nx

import os
import glob
import sys

import math

import numpy as np

import xml.etree.ElementTree as ET

import sqlite3

import urllib

import csv
import re
import optparse
import time

import pprint as pp

import urllib2

#TODO: could also store the SVN revision of the class.csv file

#TODO: add option to mark outliers

#TODO: run profile to work out where can speed up

#ToDO: also allow country to be set in {{}} format
# and handle appropriately in cleanup

import logging   
import logging.handlers   

logger = logging.getLogger("merge") 
logger.setLevel(logging.DEBUG)        
formatter = logging.Formatter('%(levelname)-6s %(message)s')
ch = logging.StreamHandler()  
ch.setLevel(logging.INFO)   
ch.setFormatter(formatter)  
logging.getLogger('').addHandler(ch)
log_dir = "."
LOG_FILENAME =  os.path.join(log_dir, "merge.log")
LOG_SIZE = 2097152 # 2 MB
fh = logging.handlers.RotatingFileHandler(
    LOG_FILENAME, maxBytes=LOG_SIZE, backupCount=5)
fh.setLevel(logging.DEBUG)   
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")  
fh.setFormatter(formatter)
logging.getLogger('').addHandler(fh)    

#TODO fix bug wher eneed to append / to dir name for convert and merge
#TODO be careful not to apply colour legend classification to stop sign nodes 
# which are used to represent hyperedges - but is ok for stop sign nodes
# with labels, which may just have doubted labels
#TODO catch errors in converter script, print, continue on to next network
#TODO: If used fuzzy match then store geo matched name in the node


#TODO: simplify the country detection, make user manually specify the country if
# it is outside of the csv tag, eg tokyo outside for an Australian network

 # Converts from yED traced file into cleaned GML
    # with optional CSV parameters

# another option: if map really doesn't work, then try specifying some cities
# using geocode_id co-ordinates, from which the script can calculate
# approx lat/long -> km distances to use for rest of calcs
# ie not just histogram bins

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

opt.add_option('--dataset_only', action="store_true",
                dest="dataset_only", default=False,
                help="Only use entries marked True in dataset column of CSV")


opt.add_option('--tagged_only', action="store_true",
                dest="tagged_only", default=False,
                help="Only use networks present in the CSV")

opt.add_option('--keep_graphics', action="store_true",
                dest="keep_graphics", default=False,
                help="Keep yEd graphics data")


opt.add_option('--check_geocode', action="store_true",
                dest="check_geocode", default=False,
                help="Try to spot and fix incorrect geocode matches")

#TODO: remove dest from all of these
opt.add_option('--fit_thresh', 
                dest="fit_thresh", default=20,
                help="Fitness threshold to look for new node at")

options = opt.parse_args()[0]

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
    graph =  graph.to_undirected(graph)

    return nx.MultiGraph(graph)

def extract_edge_data(label):

    #TODO: also do the raw speed for edges (optional command line  argument
    # eg 10M is 10 * 10^6 = 10,000,000 in the GML

    # Note starts off as label and is trimmed as data is extracted
    data = {'LinkLabel': label, 'LinkNote': str(label), 'LinkSpeedUnits': '',
            'LinkType': '', 'LinkSpeed': '', 'time': ''}

    extract_speed = True

    #TODO: try with regex compile to see if faster
    # (and move out of function back into main and compile outside loop)

    # Note: Could likely optimise the following into a single regexp
    # Skip speed if ambiguities, ie if - or + present and not in STM- or OC-
    if re.search(r"-+", label):
        # Contains + or -
        # Remove OC- and STM- and check if still contains - 
        temp = re.sub(r"OC-\d+|STM-\d+", "", label)
        if re.search(r"-+", temp):
            # label contains - or + not inside OC- or STM-, so skip speed
            extract_speed = False

    num_units = "(\d+\.*\d*)\s*(k|K|m|M|g|G|t|T)(b?p?/?s?)"
    speed_invalidators = "[>|<|<=|>=|=>|=<|*|x|-]"
    if extract_speed:
        if re.search( speed_invalidators+"\s*"+num_units, label, re.IGNORECASE):
            # eg >45Mbps, <=1gbps, 2x>2.5G, etc
            # Ambiguous as to actual speed -> skip
            pass
        elif len(re.findall(num_units, label, re.IGNORECASE)) > 1:
            # Speed should only occur once
            pass
        else:
            # Try and extract units if present
            re_m = re.search(num_units, label, re.IGNORECASE)
            if re_m:
                speed = re_m.group(1)
                units = re_m.group(2)
                # Get the bps bp/s bit as well to remove from note 
                units_extra = re_m.group(3)
                data['LinkNote'] = data['LinkNote'].replace(speed, "")
                data['LinkNote'] = data['LinkNote'].replace(units, "")
                data['LinkNote'] = data['LinkNote'].replace(units_extra, "")
                #TODO: convert speed to float (can't be int as may have 3.2
                data['LinkSpeed'] = speed
                data['LinkSpeedUnits'] = units.upper()
                #print "extract: {0} {1}".format(m.group(1), m.group(2))

    #TODO: remove the above from ongoing label/note

    # Try to extract Type
    edge_types = "OC-\d+|T\d|E\d|DS-\d+|STM-\d+|SDH|Fib[e|r]{2}|Optical|GIGE|FE"
    edge_types += "|Ethernet|Satellite|Wireless|Microwave|Serial|ATM|MPLS"

    if len(re.findall(edge_types, label, re.IGNORECASE)) > 1:
        # Should only occur once -> ignore
        # see if occurs as type (over|via) type,
        # eg Ethernet over SDH, Ethernet over Satellite, etc
        over_via =  r"({0})\s(over|via)\s({0})".format(edge_types)
        re_m = re.search(over_via, label, re.IGNORECASE)
        if re_m:
            temp = re_m.group(0)
            data['LinkNote'] = data['LinkNote'].replace(temp, "")
            data['LinkType'] = temp
        else:
            # skip this entry as invalid
            pass
    else:
        re_m = re.search(edge_types, label, re.IGNORECASE)
        if re_m:
            temp = re_m.group(0)
            data['LinkNote'] = data['LinkNote'].replace(temp, "")
            # pretty up the type
            # eg ethernet -> Ethernet, ETHERNET -> Ethernet, STM -> STM ie same
            if not temp.isupper():
                # Not upper case, tidy up
                # Ignore all upper case, eg avoid ATM -> Atm, SDH -> Sdh
                temp = temp.title()
            data['LinkType'] = temp


    # Try to extract temporal information
    temporal = "Future|Planned|Current|Proposed"
    temporal += "|Planning|Under Construction|Under Development"
    re_m = re.search(temporal, label, re.IGNORECASE)
    if re_m:
        temp = re_m.group(0)
        data['LinkNote'] = data['LinkNote'].replace(temp, "")
        data['LinkStatus'] = temp.title()

    # Tidying up: remove note if it is just punctuation
    # eg () or + left over from extraction
    if ( (len(data['LinkNote']) > 0) and
        (len(re.findall("\w+", data['LinkNote'], re.IGNORECASE)) == 0) ):
        # Non-trivial length and no alphanumeric characters present so wipe note
        data['LinkNote'] = ''

    # Remove any empty fields
    for key, val in data.items():
        if val == '': 
            del data[key]

    # Don't include note if same as label - redundant
    if ('LinkNote' in data) and (data['LinkNote'] == data['LinkLabel']):
        del data['LinkNote']

    return data

def city_cleanup(city):
    # cleans up various data from city name
    city = city.rstrip("123456789?")

    # Slices are faster than regular expressions
    # Expand eg Ft Lauderdale to Fort Lauderdale
    if city[:3].lower() == "ft ":
        city = "Fort " + city[3:]

    if city[:3].lower() == "pt ":
        city = "Port " + city[3:]

    if city[:3].lower() == "mt ":
        city = "Mount " + city[3:]

    if city[:3].lower() == "st ":
        city = "Saint " + city[3:]

    # Ending in Mt -> Mountain
    if city[-3:].lower() == " mt":
        city = city[:-3] + " Mountain"
    return city

def geocode_sql(city=None, country=None, continent=None,
                geonameid=None, neighbours = False, conn=None):
    if conn is None:
        return

    logger.debug("SQL lookup {0} {1} {2}".format(city, country, neighbours)) 
    c = conn.cursor()
    results = None
    #TODO docstring on parameter logic
    if city:
        # Some cleanup
        city = city_cleanup(city )
        city = city.title()
        # look up city
        # new better query to search alternate names also
        if country and ((isinstance(country, str)
                         or isinstance(country, unicode))):
            # Country specified, use in query to restrict cities to that country
            query = ('SELECT latitude,longitude FROM geoname WHERE asciiname=?'
                     'AND country=? ORDER BY population DESC')
            c.execute(query, [city, country])
            results = c.fetchone()
        elif country and isinstance(country, list):

            query = ('SELECT latitude, longitude FROM geoname WHERE country IN'
                     '(%s) AND asciiname=? ORDER BY population'
                     'desc') % ','.join('?'*len(country))
            values = [i for i in country]
            values.append(city)
            c.execute(query, values)

            results = c.fetchone()
        else:
            query = ('SELECT latitude, longitude FROM geoname WHERE'
                      'asciiname=? ORDER BY population DESC')
            c.execute(query, [city])
            results = c.fetchone()
        #TODO: see if want to use multiple results -
        # use count option similar to web geocode function
        if results:
            data = {'lat': results[0], 'lng': results[1]}
            return data
        else:
            return None
    elif geonameid:
        # look up place based on id
        query = 'SELECT latitude, longitude FROM geoname WHERE geonameid=?' 
        c.execute( query, [geonameid])
        results = c.fetchone()
        
        if results:
            data = {'lat': results[0], 'lng': results[1]}
            return data
    elif country:
        if neighbours:
            # find the neighbors of this country
            if country and ((isinstance(country, str) 
                             or isinstance(country, unicode))):
                # Country specified, restrict cities to that country
                c.execute('SELECT neighbours FROM countryinfo WHERE ISO=?',
                          [country])
                results = c.fetchone()
                
                if results:
                    return results[0]
                
            elif country and isinstance(country, list):
                # convert into tuple so in correct format for psycopg (lists are
                # converted to python arrays in psycopg)
                country = tuple(country)
                # Country specified, use to restrict cities to that country
                # based on http://stackoverflow.com/questions/1309989
                query = ('SELECT neighbours FROM countryinfo WHERE ISO '
                         'IN (%s)') % ','.join('?'*len(country)) 
                c.execute(query, country)
                results = c.fetchall()
                # Returns country codes as a comma-seperate string, inside tuple
                #eg [('ES,FR',), ('MK,GR,CS,ME,RS,XK',), etc
                # convert strings inside tuple into list of codes
                results = [ x[0].split(",") for x in results if x[0] != None]
                # Convert from nested lists into single list of codes
                results = [ a for x in results for a in x]
                # unique codes only
                results = list(set(results))
                return results
            else:
                return None
        else:
            # look up country code
            c.execute('SELECT ISO FROM countryinfo WHERE country=?', [country])
            results = c.fetchone()
            
            if results:
                return results[0]
            else:
                # no results may use abbreviation eg USA so try ISO3 column also
                c.execute('SELECT ISO from countryinfo WHERE ISO3=?', [country])
                # see if this gave results
                results = c.fetchone()
                if results:
                    return results[0]
    elif continent:
        # Lookup countries in this continent
        if isinstance(continent, str) or isinstance(continent, unicode):
            query = 'SELECT ISO FROM countryinfo WHERE continent = ?' 
            c.execute(query, [continent])
            results = c.fetchall()

        elif isinstance(continent, list):
            # based on http://stackoverflow.com/questions/1309989
            c.execute('SELECT ISO FROM countryinfo WHERE continent IN (%s)' %
                           ','.join('?'*len(continent)), continent)
            results = c.fetchall()
        if results:
            # convert from the SQL result tuple into country codes
            results = [ x[0] for x in results]
            return results
        else:
            return None

def web_fetch(url, cache):
    #TODO: move the fetch code into seperate function with cache handling
    if cache:
        c = cache.cursor()
        c.execute('SELECT result FROM cachegeoname WHERE query=?', [url])
        geocode_result = c.fetchone()
        c.close()

        if geocode_result:
            # Remove from tuple
            geocode_result = geocode_result[0]
            # and re-encode for XML parsing
            geocode_result = geocode_result.encode("utf-8")
            logger.debug("Cache hit")
        else:
            logger.debug("Cache miss")
 
    #TODO: check on closing sql connection
    if not geocode_result:
        # Either not using cache, or cache miss
        logger.debug("Querying web service")
        overloaded_message = ("free servers are currently "
                              "overloaded with requests" )
        for i in range(10):
            geocode_url = urllib2.urlopen(url)
            geocode_result = geocode_url.read()
            geocode_url.close()
            if overloaded_message in geocode_result:
                logger.debug("Overloaded server, trying again in 0.5 seconds")
                time.sleep(0.5)
            else:
                # Result is fine
                break

        if cache:
            logger.debug("Caching result")
            # Cache miss occured - cache used, but no query found, store
            cur = cache.cursor()
            cur.execute('INSERT INTO cachegeoname (query, result) VALUES(?, ?)',
                        [url, geocode_result.decode("utf-8")])
            cache.commit()   
            cur.close()

    return geocode_result

def geocode_web(place=None, count=1, country=None, fuzzy=0,
                geoname_id=None, cache=None):

    logger.debug("Web lookup {0} {1}".format(place, country)) 
    results = []

    if geoname_id and geoname_id.isdigit():
        params = {}
        params['geonameId'] = geoname_id
        params['style'] = "SHORT" 
        params = urllib.urlencode(params)
        url = "http://ws.geonames.org/get?{0}".format(params)
    else:
        # Construct search query
        params = {}
        params['q'] = place
        # P is featureClass for towns/villages/cities (not lakes, rivers, hills)
        params['maxRows'] = count
        if country:
            # if list convert into AU,US,GB format
            if isinstance(country, list):
                country = ",".join(country)
            # use this country code
            params['country'] = country 
        if fuzzy > 0:
            params['fuzzy'] = fuzzy 
        params['featureClass'] = "P" 
        params = urllib.urlencode(params)
        url = "http://ws.geonames.org/search?{0}".format(params)
    
    logger.debug("Web lookup {0}".format(url)) 
    # Get the query
    # Check if got overloaded - try 10 times
    geocode_result = web_fetch(url, cache)
        
    # Now extract the result
    dom = ET.XML(geocode_result)
    if geoname_id:
        # geoname_id result 
        # Only single result returned, <geoname>...</geoname>
        # Pass this to the iteration as a list to be processed as per normal
        geonames = [dom]
    else:
        geonames =  dom.findall("geoname")

    for entry in geonames:
        #if g is None:
        #    logger.debug( "No result for {0}".format(g))
        #   return
        place_data = {}
        for attr in [ "name", "countryCode", "lat", "lng",
                  "adminName1", "population", "countryName", "geonameId" ]:
            try:
                value = entry.find(attr)
                if value is not None:
                    value = value.text
                    value = value.encode('utf-8')
            except AttributeError, e:
                # Problem with attribute, use default
                logger.debug("AttributeError {0}".format(e))
                value = ""

            place_data[attr] = value
        results.append(place_data)
    # return only the entry for single results
    if len(results) == 1:
        results = results[0]
    return results 

def check_geocode(input_graph, country_code=None, cache=None):
    # fitness function for a node is (bin index xy) - (bin index latlong) for
    # each edge of the node (even edges to to non-outliers)

    fit_thresh = float(options.fit_thresh)
    graph = input_graph.copy()
    geocoded_cities = [ n for n, data in graph.nodes(data=True)
                  if 'Latitude' in data and 'Longitude' in data]
    graph = graph.subgraph(geocoded_cities)

    #TODO: convert from multi_edge to single edge for working
    # as otherwise multi edges bias towards nodes with multiple edges as edge
    # based calculation
    # and update keys bit of edge loop appropriately

    # remove any nodes which have the geonameId manually set
    graph.remove_nodes_from( [n for n, data in graph.nodes(data=True) if
                              'geoname_id' in data])

    if graph.number_of_edges() == 0:
        # No edges to verify
        return input_graph

    #TODO: see if can remove these
    geo_lengths = []
    xy_lengths = []

    geo_keys = []
    xy_keys = []
        
    def distance((x_1, y_1), (x_2, y_2)):
        # Return distance squared, as sqrt expensive, uncessary for comparisons
        #TODO: clean up the handling of floats
        return (math.pow((float(x_1)-float(x_2)), 2) 
                + math.pow((float(y_1)-float(y_2)), 2))

    def angle ((x_1, y_1), (x_2, y_2)):
        opp = (float(x_2) - float(x_1))
        adj = (float(y_2) - float(y_1))
        if adj == 0:
            # can't divide by zero
            return 0
        return np.arctan(opp/adj) * (180/np.pi)

    for src, dst, key, data in graph.edges(data=True, keys=True):
        # Traced length of link
        xy_length = distance(
            (graph.node[src]['graphics']['x'],
             graph.node[src]['graphics']['y']),
            (graph.node[dst]['graphics']['x'],
             graph.node[dst]['graphics']['y']))
        graph[src][dst][key]['xy_length'] = xy_length 
        xy_lengths.append(xy_length)

        xy_angle = angle(
            (graph.node[src]['graphics']['x'],
             graph.node[src]['graphics']['y']),
            (graph.node[dst]['graphics']['x'],
             graph.node[dst]['graphics']['y']))
        print xy_angle    
        graph[src][dst][key]['xy_angle'] = xy_angle 
    xy_bin_edges = np.histogram(xy_lengths, normed = True, bins=100)[1]

    geo_bin_edges = []

    #TODO: may be able to optimise this
    def calc_geo_bin_edges():
        geo_lengths = []
        for src, dst, key, data in graph.edges(data=True, keys=True):
            # Geographical length of link
            # Note this uses flat distance, doesn't account for Earth spherical
            geo_length = distance(
                (graph.node[src]['Latitude'], graph.node[src]['Longitude']),
                (graph.node[dst]['Latitude'], graph.node[dst]['Longitude']))
            graph[src][dst][key]['geo_length'] = geo_length 
            geo_lengths.append(geo_length)
            # assumes xy length calculated already - as this shouldn't change

        geo_bin_edges = np.histogram(geo_lengths, normed = True, bins=100)[1]
        return geo_bin_edges


    geo_bin_edges = calc_geo_bin_edges()

    #TODO: may need to have logarithmic bins again

    #geo_cdf = np.cumsum(geo_pdf)
    #print "pdf {0} cdf {1}".format(len(geo_pdf), len(geo_cdf))
    #xy_cdf = np.cumsum(xy_pdf)
    #print "pdf {0} cdf {1}".format(len(xy_pdf), len(xy_cdf))

    #toDo: need to look at the histogram density also
    # if all links close ie evenly distributed then the threshold can get
    # messy

    # Now store the xy histogram for each edge
    for src, dst, key, data in graph.edges(data=True, keys=True):
        # Digitize takes array as first argument
        xy_ind = np.digitize([data['xy_length']], xy_bin_edges)

        #TODO: remove geo bit
        geo_ind = np.digitize([data['geo_length']], geo_bin_edges)

        #print "{0} {1} {2} {3}".format(graph.node[src]['label'],
        #                           graph.node[dst]['label'],
        #                           xy_ind, geo_ind)

        graph[src][dst][key]['xy_ind'] = xy_ind

    #ToDO: merge geo_length and fitness function to use each other
    def calc_geo_length(n):
       for (_, dst, _, data) in graph.edges(n, data=True, keys=True):
            # Calculate index for provided lat and long 
            geo_length = distance( (lat, lng), 
                                  (graph.node[dst]['Latitude'],
                                   graph.node[dst]['Longitude']))


    #TODO: see if fitness best name, or if invert and call cost etc
    def fitness(n, lat=None, lng=None):
        # if lat lng not set then use those already set for this node
        if not lat:
            lat = graph.node[n]['Latitude']
        if not lng:
            lng = graph.node[n]['Longitude']
        # if geo then use geo co-ords, else use xy coords
        scores = []

        if graph.degree(n) == 0:
            # No edges for this node - possibly connected to hypernodes
            return 0 

        for (_, dst, _, data) in graph.edges(n, data=True, keys=True):
            #print graph.node[n]['label'] + ' ' + graph.node[dst]['label']
            # Calculate index for provided lat and long 
            geo_length = distance( (lat, lng), 
                                  (graph.node[dst]['Latitude'],
                                   graph.node[dst]['Longitude']))
            geo_ind = np.digitize([geo_length], geo_bin_edges)
            # Compare this to stored xy index 
            xy_ind = data['xy_ind']
            scores.append( abs(geo_ind - xy_ind))
            geo_angle = angle( (lat, lng), 
                              (graph.node[dst]['Latitude'],
                               graph.node[dst]['Longitude']))
            xy_angle = data['xy_angle']
            #logger.debug( "ind {0} angle {1} to {2}".format(xy_ind, xy_angle,
            #                                                graph.node[dst]['label']))
            # Note need to append as array []
            scores.append( [0.6 * abs(geo_angle - xy_angle)])
        return np.mean(scores)

    for n in graph.nodes():
        node_fitness = fitness(n)
        graph.node[n]['fitness'] = node_fitness

    def add_result(result_list, result):
        """ Handles if dict or list"""
        if isinstance(result, dict):
            result_list.append(result)
        elif isinstance(result, list):
            result_list.extend(result)
        return result_list

    def find_better_match(n):
        # returns True if better match found
        better_match_found = False

        # See if place has geocode id manually set
        if 'geoname_id' in graph.node[n]:
            # Don't look for better match as ID forced
            return
        if 'geocode_name' in graph.node[n]:
            # Manually set city to use
            city = graph.node[n]['geocode_name']
        else:
            city = graph.node[n]['label']

        city = city_cleanup(city)

        other_matches = []
        # If query with too high a max result get nothing back
        for i in [5, 3, 2, 1]:
            match_result = geocode_web(city, fuzzy=1, country=country_code,
                                       cache=cache, count=i)
            if match_result:
                # Found result for this count
                break
            # otherwise no result, try with smaller query
        
        other_matches = add_result(other_matches, match_result)

        # also try fuzzy
        match_result = geocode_web(city, fuzzy=0.8, country=country_code,
                                   cache=cache, count=3)
        other_matches = add_result(other_matches, match_result)

        # and without country
        match_result = geocode_web(city, cache=cache, count=3)
        other_matches = add_result(other_matches, match_result)
       
        if not other_matches:
            logger.debug( "no result for {0}".format(city))
            # No result found
            return better_match_found
 
        #best_score = graph.node[n]['fitness']
        #TODO: see if can cut storing fitness as can be out of date
        best_score = fitness(n)
        #print "\n\ncurrent fitness {0} {1}".format(city, best_score)

        for match in other_matches:
            #print match
            match_lat = float(match['lat'])
            match_lng = float(match['lng'])
            # Work out fitness score
            node_fitness = fitness(n, lat=match_lat, lng=match_lng)
            logger.debug("Fitness {0}: match {1}".format(node_fitness, match))
           
            # only use if better than threshold, if not then keep current
            # and also only if better than current best score
            # This uses highest up from retrieved list first
            if (node_fitness < fit_thresh) and (node_fitness < best_score):
                logger.debug("Moving {0} to {1}".format(city,
                                                        match['geonameId']))
                logger.debug( "old {0} vs new {1}".format(best_score,
                                                          node_fitness))
                graph.node[n]['Latitude'] = match_lat
                graph.node[n]['Longitude'] = match_lng
                # New best match to beat
                best_score = node_fitness
                better_match_found = True

        # and update fitness of neighbours
        #TODO: only do if node has changed
        if best_score != graph.node[n]['fitness']:
            # New best match was found, update node fitness
            graph.node[n]['fitness'] = best_score
            # and updated neighbor's scores 
            for neigh in graph.neighbors(n):
                graph.node[neigh]['fitness'] = fitness(neigh)

        return better_match_found

    # also store the found name if fuzzy matched
    #TODO: also want to detect nodes of which all neighbours are out
    # as they are likely in the middle
    outlier_list = [n for n, data in graph.nodes(data=True)
                    if data['fitness'] > fit_thresh]

    g_outliers = graph.subgraph(outlier_list) 
    # Sort by fitness - do worst first
    graded = [ (graph.node[n]['fitness'], n) for n in outlier_list]
    graded = [ n[1] for n in sorted(graded, reverse=True)]    
    #TODO: see if graded used

    logger.debug("Outliers {0}".format(",".join([graph.node[n]['label']
                                                 for n in outlier_list])))
    g_outliers = graph.subgraph(outlier_list) 

    # look at connected subgraphs
    for g_sub in nx.connected_component_subgraphs(g_outliers):
        # Look at worst node first as this may distort the other, correct, nodes
        graded = [(data['fitness'], n) for n, data in g_sub.nodes(data=True)]
        graded = [ n[1] for n in sorted(graded, reverse=True)] 
        for n in graded:
            # Check still needs to be changed - neighbor may have fixed
            if fitness(n) > fit_thresh:
                logger.debug("Try {0} as fit is {1}".format(
                    graph.node[n]['label'], fitness(n)))
                if find_better_match(n):
                    # better match found, recalc edges
                    geo_bin_edges = calc_geo_bin_edges() 


    #done?

    # mark any problems still
    #toDO: what is going on here?
    for n in graph.nodes():
        continue
        node_fitness = fitness(n)
        graph.node[n]['fitness'] = node_fitness
    outlier_list = [n for n, data in graph.nodes(data=True)
                    if data['fitness'] > fit_thresh]

    # Mark outliers for plot
    for n in outlier_list:
        continue
        # See if place has geocode id manually set
        if 'geoname_id' in graph.node[n]:
            # Don't look for better match as ID forced
            continue 
        if 'geocode_name' in graph.node[n]:
            # Manually set city to use
            city = graph.node[n]['geocode_name']
        else:
            city = graph.node[n]['label']
        
        city = city_cleanup(city)

        # new simple search - city geoextent
        # No good match, choose the best out of the first worldwide result,
        # and the best for the country
        result_a = geocode_web(city, fuzzy=1, country=country_code,
                               cache=cache)
        result_b = geocode_web(city, fuzzy=1, cache=cache)
       
        if result_a and result_b:
            if (fitness(n, result_a['lat'], result_a['lng']) 
                < fitness(n, result_b['lat'], result_b['lng'])):
                # Use country
                match_result = result_a
            else:
                match_result = result_b
            
            graph.node[n]['Latitude'] = match_result['lat']
            graph.node[n]['Longitude'] = match_result['lng']
        input_graph.node[n]['outlier'] = 1

    # Extract lat/lng from working graph and put into input_graph for returning
    for n, data in graph.nodes(data=True):
        if 'Latitude' in data:
            input_graph.node[n]['Latitude'] = float(data['Latitude'])
        if 'Longitude' in data:
            input_graph.node[n]['Longitude'] = float(data['Longitude'])

    return input_graph

# do geocode_country wrapped to look up country code from the web

def geocode(graph, sql_conn=None):
    # How fuzzy we accept results to be [0,1], lower = accept 'fuzzier' answers 
    #TODO: pass this on to appropriate functions 
    fuzzy = 0.8

    country_name = None
    country_code = None
    #TODO: skip if external node, or if all caps
    if 'GeoExtent' in graph.graph:
        geoextent = graph.graph['GeoExtent']
        geolocation = graph.graph['GeoLocation']
        continent_codes = {
            'North America':    'NA',
            'Europe':           'EU',
            'Latin America':    'SA',
            'Africa':           'AF',
            'Middle East':      'AF',
            'Asia-Pacific':      ['AS', 'OC'],
        }

        if geoextent == 'Region':
            # Extract the last bit of the comma seperated region as the country
            # eg Washington State, USA -> extract 'USA'
            fragments = geolocation.split(",")
            country_name = fragments[len(fragments)-1].strip()
        elif geoextent == 'Country':
            country_name = geolocation
        elif geoextent == 'Country+':
            # convert into array and remove spaces
            country_name = [x.strip() for x in geolocation.split(",") ]
            country_code = []
            for cou in country_name:
                if cou in continent_codes:
                    # May have continent, eg Europe in "USA, Europe"
                    country_name.remove(cou)
                    continent = continent_codes[cou]
                    # Note need to concatenate lists here
                    country_code += (geocode_sql(continent=continent,
                                                    conn=sql_conn))
                else:
                    # look up country code - as continent search returns codes
                    # and can't have a mix of country names and cont codes
                    country_code.append(geocode_sql(country=cou,
                                                    conn=sql_conn))
        elif geoextent == 'Continent':
            # get the countries in this continent
            if geolocation in continent_codes:
                continent = continent_codes[geolocation]
                # Fetch list of countries in this continent:
                country_code = geocode_sql(continent=continent, conn=sql_conn)
            else:
                logger.warn(("Continent {0} not"
                             "in list of continents").format(geolocation))
        elif geoextent == 'Continent+':
           # convert into array and remove spaces
            cont_name = [x.strip() for x in geolocation.split(",") ]
            cont_name = []
            for con in cont_name:
                if con in continent_codes:
                    cont_name.remove(con)
                    continent = continent_codes[con]
                    # Note need to concatenate lists here
                    country_code += (geocode_sql(continent=continent,
                                                    conn=sql_conn))

    if country_name and not country_code:
        # Country has been set, but no country code, try and look up code
        # lookup country through sql
        if isinstance(country_name, str) or isinstance(country_name, unicode):
            # Just look up the name
            country_code = geocode_sql(country=country_name, conn=sql_conn)
        if isinstance(country_name, list):
            # Multiple countries, eg from Country+
            country_code = []
            for cou in country_name:
                country_code.append(geocode_sql(country=cou, conn=sql_conn))
  
    neighbours = geocode_sql(country = country_code,
                             neighbours=True, conn=sql_conn)

    def geocode_place(n):
        geodata = None
        # See if place has geocode id manually set
        if 'geoname_id' in graph.node[n]:
            geodata = geocode_web(geoname_id = graph.node[n]['geoname_id'],
                                  cache=sql_conn)
        if 'geocode_name' in graph.node[n]:
            # Manually set city to use
            city = graph.node[n]['geocode_name']
        else:
            city = graph.node[n]['label']

        city = city_cleanup(city)
       
        #Testing
        #ToDO: merge this with the above - if region, city, country search on that
        # if continent or continent+ expand to countries, if global no country 
        if graph.graph['GeoExtent'] not in ['Global', 'Continent', 'Continent+']:
            city_extent = city + ' ' + graph.graph['GeoLocation']
            geodata = geocode_web(city_extent, cache=sql_conn)


        for i in range(3):
            if geodata:
                # Match has been found
                break
            elif i == 0:
                # Try exact (non-fuzzy) match
                geodata = geocode_web(city, fuzzy = 1,
                                      country = country_code, cache=sql_conn)
            elif i == 1:
                # Try fuzzy match
                geodata = geocode_web(city, fuzzy = fuzzy,
                                      country = country_code, cache=sql_conn) 
            elif i == 2:
                # try for neighbouring countries
                geodata = geocode_web(city, fuzzy = fuzzy,
                                      country = neighbours, cache=sql_conn)

        # Store result
        if geodata:
            graph.node[n]['Latitude'] =  float(geodata['lat'])
            graph.node[n]['Longitude'] = float(geodata['lng'])
        else:
            logger.info( "No match found for " + city)

    lookup_places = []

    for n, nodedata in graph.nodes(data=True):
        city = graph.node[n]['label']
        # Some nodes are skipped
        if (city == '' or 
            ('hyperedge' in nodedata and nodedata['hyperedge'] == 1)):
            # blank city
            continue
        elif ((len(city) <= 4 and city.isupper())
              and
             ('geocode_name' not in nodedata and 'geocode_id' not in nodedata)):
            # Skip short capitalized names, as most likely an abbreviation
            # unless manually set geocode place or id
            #TODO: document this
            continue
        elif 'Internal' in nodedata and nodedata['Internal'] == 0:
            # Skip external nodes as likely to be networks rather than places
            continue
        else:
            lookup_places.append(n)
     
    for place in lookup_places:
        geocode_place(place)

    if options.check_geocode:
        graph = check_geocode(graph, country_code=country_code, cache=sql_conn)

    # and remove any geoname_id or geocode_name set now complete geocoding
    #for n,data in graph.nodes(data=True):
    #    if 'geoname_id' in data:
    #        del graph.node[n]['geoname_id']
    #    if 'geocode_name' in data:
    #        del graph.node[n]['geocode_name']

    return graph

def extract_svn_version(path):
    # Tries to obtain SVN version from path
    # http://code.djangoproject.com/browser/django/trunk/django/utils/version.py
    #TODO make more robust

    if os.path.isfile(path):
        # likely a single file passed in
        path = os.path.split(path)[0]

    entries_file = "{0}/.svn/entries".format(path)
    # check exists
    if not os.path.exists(entries_file):
        return None
    f_entries = open(entries_file, 'r')
    # line after 'dir' is the revision number
    next_line = False
    for line in f_entries:
        if next_line:
            # this line has the version number
            return int(line)
        if line.strip() == 'dir':
            next_line = True
    f_entries.close()

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
   
    if options.output_dir:
        output_path = options.output_dir 
    else:
        output_path = path + "/merged"
   
    if options.dataset_only:
        output_path += "_dataset_only"
    if options.geocode:
        output_path += "_geocode"

    #output_path += strftime("%Y%m%d_%H%M%S")
    if not os.path.isdir(output_path):
        os.mkdir(output_path) 

    # and create cache directory for pickle files
    pickle_dir = path + "/cache"
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    #TODO: make this a parameter
    sql_conn = sqlite3.connect('/Users/sk2/zoo/geonames')

    # sanity check
    if options.dataset_only and not options.csv:
        logger.warn("Can only use dataset_only if CSV file specified")

    metadata = {}
    # Metadata to use from CSV
    metadata_headings = ["Network", "Geo Extent", "Geo Location", 
                         "Type", "Classification", "Last Access",
                        "Source", "Layer", "Date Obtained",
                        "Network Date", "Developed", "Note"]
    
    if options.csv:
        #TODO: check csv file exists
        # OpenOffice
        #csv_file = open( options.csv, "rb" )
        #csv_reader = csv.DictReader( csv_file, delimiter=",")
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

    # try and get SVN version
    svn_version = extract_svn_version(path)

    def process_network(source_file):

        # Extract name of network from file path
        source_dir, filename = os.path.split(source_file)
        net_name = os.path.splitext(filename)[0]
        logger.info( "{0}".format(net_name))

        skip_net = False
        if options.dataset_only:
            if net_name not in metadata:
                logger.warn( ("{0} not in metadata, skipping as assume "
                              "not in dataset").format(net_name))
                skip_net = True
            if ( net_name in metadata 
                and 'Dataset' in metadata[net_name]):
                true_vals = "Yes|True|1"
                value = metadata[net_name]['Dataset']
                m = re.search(true_vals, value, re.IGNORECASE)
                if not m:
                    # Network not in dataset
                    logger.warn( "{0} not in dataset skipping".format(net_name))
                    skip_net = True 

        if options.csv and options.tagged_only:
            if net_name not in metadata:
                skip_net = True

        if skip_net:
            return 

        #TODO: check order here when converting to multi graph and undirected 
        pickle_file = "{0}/{1}.pickle".format(pickle_dir, net_name)
        if (os.path.isfile(pickle_file) and
            os.stat(source_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_gml(source_file)
            nx.write_gpickle(graph, pickle_file)
        
        graph = convert_to_undirected(graph)
        #TODO: if original file modified after pickle file use original
        # Check for self loops
        for n, data in graph.nodes(data=True):
            if n in graph.neighbors(n):
                logger.warn( "Self loop {0} {1}".format(data['label'], n))

        

        category_mappings = {
            #TODO: document these
            #TODO: clarify these vs previous minutes notes
            "ellipse": "internal",
            "triangle": "external",
            "rectangle": "internal",
            "roundrectangle": "internal", # also used as normal rectangle
            "diamond": "switch",
            "octagon": "uncertain",
            "hexagon": "uncertain",
        }

        #TODO: use different shape for legend

        #*********************************
        # Editing
        #*********************************
        #TODO: convert 1 and 0 into True and False once 
        # netx writing bug resolved (booleans text but not quoted)

        # Set category for node
        for node, data in graph.nodes(data=True):
            # assume internal unless set otherwise
            graph.node[node]['Internal'] = 1
            # remove whitespace
            label = data['label'].strip()
            # check if location manually forced
            # ie contains {} - assumed that {} occurs at end
            # todo: document this
            if label[-2:] == "}}" and "{{" in label:
                pos = label.find("{{")
                # geocode_id is from just after {{ to one before }}
                geocode_id = label[pos+2:-2]
                # label is bit up to the {country}, and remove any whitespace
                label = label[:pos].strip()
                # set country and label
                graph.node[node]['label'] = label
                if geocode_id.isdigit():
                    # Numeric, assume is geoname_id
                    graph.node[node]['geoname_id'] = geocode_id
                else:
                    # Assume is place name to try
                    #TODO: see if set country also - eg "San Jose, USA"
                    # ie if comma present then split on it, "place, country"
                    graph.node[node]['geocode_name'] = geocode_id


            if "graphics" in data and "type" in data["graphics"]:
                shape = data["graphics"]["type"]
                #TODO: remove all diamonds from source files
                if shape == "diamond":
                    logger.warn( "Node {0}".format(data['label']))
                    logger.warn( "Warning: diamond shape no longer supported")
                
                if shape == "triangle":
                    graph.node[node]['Internal'] = 0 

                #TODO: replace octagons for hyperedges with diamonds
                # As currently have overloaded the doubt and hyperedge symbols
                if shape == "octagon" or shape == "hexagon":
                    # Check if hyperedge or doubted node
                    if data['label'] == '':
                        graph.node[node]['hyperedge'] = 1 
                    else:
                        # Unsure about this node
                        graph.node[node]['doubted'] = 1


        # if all nodes in network are internal, remove redundant internal tag
        if all(data['Internal'] == 1 for n,data in graph.nodes(data=True)):
            # all internal, remove tag
            for n in graph.nodes():
                del graph.node[n]['Internal']

        # yEd import cleanup
        # remove directed and hiearchich from the yEd import
        # netx gml writer will write as directed if appropriate
        del graph.graph['directed']
        del graph.graph['hierarchic']

        # Add SVN version
        if svn_version:
            graph.graph['SvnVersion'] = svn_version

        #*********************************
        # Apply key/legend nodes
        #*********************************
        # Check to see if any legend nodes, if so extract the shape/color combo
        legend_keys = {}
        for node, data in graph.nodes(data=True):
            label = data['label']
            if label.find('Legend') != -1:
                # Find returns position of search string, -1 if not found
                # Format the label
                legend_key = label.replace("Legend:", "").strip()
                # Color corresponding to this key
                color = data['graphics']['fill']
                shape = data['graphics']['type']
                if shape not in legend_keys:
                    legend_keys[shape] = {}
                legend_keys[shape][color] = legend_key 
                # Remove the legend node from graph as not a network node
                graph.remove_node(node)

        #TODO: document how to use "Legend:" and colours
        # Apply the legend to other nodes in graph
        if len(legend_keys) > 0:
            # Using legend keys, set node details appropriately
            for node, data in graph.nodes(data=True):
                if 'hyperedge' in data:
                    # Don't apply color class to hyperedges
                    continue
                color = data['graphics']['fill']
                shape = data['graphics']['type']

                #TODO: check/catch color key being present in
                if shape in legend_keys and color in legend_keys[shape]:
                    legend_label = legend_keys[shape][color]
                    graph.node[node]['type'] = legend_label
                else:
                    # Ignore default color FFCC00 (yellow), and external node shape
                    if color != "#FFCC00" and shape != "triangle":
                        logger.warn(("Color {0} for node {1} has no legend "
                                     "associated").format(color, data['label']))
       
        #*********************************
        # Add metadata 
        #*********************************
       
        if net_name in metadata:
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
                    #TODO: remove classification from final dict (leave now
                    # as Hung's stats scripts depend on it in full)
                    if key == "Classification":
                        network_tags = value.split()
                        for tag in valid_class_tags:
                            if tag in network_tags:
                                # Network has this tag
                                graph.graph[tag] = 1
                            else:
                                # Network doesn't have this tag
                                graph.graph[tag] = 0
                    graph.graph[key] = value
        elif options.csv:
            # Not found, and using csv, give error
            print "Warning: {0} not found in metadata file".format(net_name)


        #*********************************
        # Geocoding (optional)
        #*********************************
        if options.geocode:
            graph = geocode(graph, sql_conn) 

        #*********************************
        # Remove graphics data 
        #*********************************
       
        # TODO: make this a command line option
        if options.keep_graphics is False:
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
        
        # Will be overwritten with name from metadata if present
        network_label = net_name

        # extract edge data
        for src, dst, key, data in graph.edges(data=True, keys=True):
            if 'label' in data:
                label = data['label']
                extracted_data = extract_edge_data(label)
                # Replace the edge data with extracted
                graph.edge[src][dst][key] = extracted_data

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
        gml_file =  "{0}/{1}.gml".format(output_path, filename)
        nx.write_gml(graph, gml_file)
    
    for network in network_files:
        process_network(network)
        
#TODO: move the nested functions out to be main functions - and pass in
 #appropriate args (setup args in prev line to nested call)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass    
