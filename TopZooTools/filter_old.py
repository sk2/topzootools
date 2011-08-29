#! /usr/bin/env python

import networkx as nx

import os
import glob
import sys

import math

import traceback

#import numpy as np

import xml.etree.ElementTree as ET

import cPickle

import urllib

import csv
import re
import optparse
import time

import pprint as pp

import calendar

import threading
from Queue import Queue

import urllib2
import urlparse

from collections import defaultdict

#toDO; expand regex to capture LinkLabel "OC-192c"

#TODO: Move options and args parsing out of body of code, and make them pass
#params yo main body of code which is a function - so can call programatixally
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

class FileGetter(threading.Thread):
    # based on http://www.artfulcode.net/articles/multi-threading-python/
    def __init__(self, node, url ):
        self.url = url
        self.result = None
        self.node = node
        self.has_exceeded_count = False
        threading.Thread.__init__(self)

    def get_result(self):
        return self.result

    def get_node(self):
        return self.node

    def get_url(self):
        return self.url

    def exceeded_count(self):
        return self.has_exceeded_count

    def process_result(self, web_result):
        results = []
        for entry in web_result:
            place_data = {}
            for attr in [ "name", "countryCode", "lat", "lng", "adminName1",
                         "population", "countryName", "geonameId"]:
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
        elif len(results) == 0:
            results = None

        return results

    def run(self):
        overloaded_message = ("free servers are currently "
                                "overloaded with requests" )
        exceeded_message = ("Please throttle your requests "
                            "or use the commercial service")
        for i in range(10):
            try:
                logger.debug("Fetching {0}".format(self.url))
                geocode_url = urllib2.urlopen(self.url)
                geocode_result = geocode_url.read()
                geocode_url.close()
                if overloaded_message in geocode_result:
                    logger.debug("Overloaded server, trying again in 0.5 seconds")
                    time.sleep(0.5)
                elif exceeded_message in geocode_result:
                    logger.warn("Exceeded limit for geonames server")
                    #TODO: make sure don't cache this result
                    self.has_exceeded_count = True
                    return None
                else:
                    # Result is fine
                    break
            except urllib2.HTTPError, e:
                logger.debug(e)
                time.sleep(0.5)
    
        dom = ET.XML(geocode_result)
        #TODO: look at more programatic way - eg look at first tag
        if "http://api.geonames.org/get" in self.url:
            # geoname_id result
            # Only single result returned, <geoname>...</geoname>
            # Pass this to the iteration as a list to be processed as per normal
            geonames = [dom]
        else:
            geonames =  dom.findall("geoname")

        self.result = self.process_result(geonames)

opt = optparse.OptionParser()
opt.add_option('--file', '-f',
            help="Load data from FILE")

opt.add_option('--directory', '-d',
            help="process directory")


opt.add_option('--output_dir', '-o',
            help="process directory")

# Dataset Options
opt.add_option('--unique_networks', action="store_true",
               default=False, help="Remove all but most recent entry for "
                                    "time series networks")

opt.add_option('--ip_only', action="store_true",
               default=False, help="Only keep IP level networks")
opt.add_option('--non_ixp', action="store_true",
               default=False, help="Remove Internet Exchange networks")

#TODO: change logic on tagged_only
opt.add_option('--tagged_only', action="store_true",
                dest="tagged_only", default=True,
                help="Only use networks present in the CSV")

options = opt.parse_args()[0]

#TODO: work out best way to put in function but make globally accessible
logger = logging.getLogger("merge")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)-6s %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
if options.debug:
    ch.setLevel(logging.DEBUG)
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
    graph =  graph.to_undirected(graph)

    return nx.MultiGraph(graph)


def extract_speed(data, label):
    speed_multipliers = {
        'K': 1e3,
        'M': 1e6,
        'G': 1e9,
    }
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
                data['LinkSpeed'] = speed
                units = units.upper()
                data['LinkSpeedUnits'] = units
                data['LinkSpeedRaw'] = speed_multipliers[units] * float(speed)

    return data

def extract_type(data, label):
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
    return data

def extract_temporal(data, label):
    temporal = "Future|Planned|Current|Proposed"
    temporal += "|Planning|Under Construction|Under Development"
    re_m = re.search(temporal, label, re.IGNORECASE)
    if re_m:
        temp = re_m.group(0)
        data['LinkNote'] = data['LinkNote'].replace(temp, "")
        data['LinkStatus'] = temp.title()
    return data

def extract_edge_data(label):

    #TODO: also do the raw speed for edges (optional command line  argument
    # eg 10M is 10 * 10^6 = 10,000,000 in the GML

    # The "Note" starts off as label and is trimmed as data is extracted
    data = {'LinkLabel': label, 'LinkNote': str(label), 'LinkSpeedUnits': '',
            'LinkType': '', 'LinkSpeed': '', 'time': ''}



    do_extract_speed = True

    # Note: Could likely optimise the following into a single regexp
    # Skip speed if ambiguities, ie if - or + present and not in STM- or OC-
    if re.search(r"-+", label):
        # Contains + or -
        # Remove OC- and STM- and check if still contains -
        temp = re.sub(r"OC-\d+|STM-\d+", "", label)
        if re.search(r"-+", temp):
            # label contains - or + not inside OC- or STM-, so skip speed
            do_extract_speed = False

    if do_extract_speed:
        data = extract_speed(data, label)
    data = extract_type(data, label)
    data = extract_temporal(data, label)

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


    # and for dot instead of space
    if city[:3].lower() == "ft.":
        city = "Fort" + city[3:]

    if city[:3].lower() == "pt.":
        city = "Port" + city[3:]

    if city[:3].lower() == "mt.":
        city = "Mount" + city[3:]

    if city[:3].lower() == "st.":
        city = "Saint" + city[3:]

    # Ending in Mt -> Mountain
    if city[-3:].lower() == " mt":
        city = city[:-3] + " Mountain"

    # Datacentre as DC
    if city[-3:].lower() == " dc":
        if city.lower() != "washington dc":
            city = city[:-3]

    # U eg for University, both at start and end
    if city[:2].lower() == "u ":
        city = city[2:]
    if city[-2:].lower() == " u":
        city = city[:-2]


    return city

def country_key(country, country_info):
    for key, val in country_info.iteritems():
        if ( val['Country'] == country
                or val['ISO3'] == country
                or val['fips'] == country):
            return key

def countries_for_continent(continent, country_info):
    results = []
    # Lookup countries in this continent
    if isinstance(continent, str) or isinstance(continent, unicode):
        results = [key for (key, val) in country_info.iteritems()
                   if val['Continent'] == continent]

    elif isinstance(continent, list):
        results = [key for (key, val) in country_info.iteritems()
                   if val['Continent'] in continent]

    return results

def country_code(graph, country_info=None):
    name = None
    code = None
    if 'GeoExtent' in graph.graph:
        geoextent = graph.graph['GeoExtent']
        geolocation = graph.graph['GeoLocation']
        continent_codes = {
            'North America':    'NA',
            'Europe':           'EU',
            # Mexico, Honduras, etc are in North America
            'Latin America':      ['SA', 'NA'],
            'Africa':           'AF',
            'Middle East':      'AF',
            'Asia-Pacific':      ['AS', 'OC'],
        }

        if geoextent == 'Region':
            # Extract the last bit of the comma seperated region as the country
            # eg Washington State, USA -> extract 'USA'
          #TODO: look at doing for eg South Australia, Victoria, Australia
            fragments = geolocation.split(",")
            name = fragments[len(fragments)-1].strip()
        elif geoextent == 'Country':
            name = geolocation
        elif geoextent == 'Country+':
            # convert into array and remove spaces
            name = [x.strip() for x in geolocation.split(",") ]
            code = []
            # find continents and countries present (can have both, eg USA,
            # Europe)
            continents = [continent_codes[con] for con in name if con in
                    continent_codes]
            for cont in continents:
                code += countries_for_continent(cont, country_info)
            if 'Latin America' in name:
                if 'US' in code:
                    code.remove('US')
                if 'CA' in code:
                    code.remove('CA')
            # And key for countries
            code += [country_key(cou, country_info) for cou in name if cou not
                    in continent_codes]

        elif geoextent == 'Continent':
            code = []
            # get the countries in this continent
            if geolocation in continent_codes:
                continent = continent_codes[geolocation]
                # Fetch list of countries in this continent:
                code = countries_for_continent(continent, country_info)
                if geolocation == 'Latin America':
                    if 'US' in code:
                        code.remove('US')
                    if 'CA' in code:
                        code.remove('CA')
            else:
                logger.warn(("Continent {0} not"
                             "in list of continents").format(geolocation))
        elif geoextent == 'Continent+':
            code = []
            # convert into array and remove spaces
            name = [x.strip() for x in geolocation.split(",") ]
            # Find continent code
            continents = [continent_codes[con] for con in name if con in
                    continent_codes]
            # Find countries for continent codes
            for cont in continents:
                code += countries_for_continent(cont, country_info)

            # If Latin-America, remove USA and Canada from the North America
            # join
            if 'Latin America' in name:
                if 'US' in code:
                    code.remove('US')
                if 'CA' in code:
                    code.remove('CA')
    if name and not code:
        # Country has been set, but no country code, try and look up code
        if isinstance(name, str) or isinstance(name, unicode):
            # Just look up the name
            code = country_key(name, country_info)
        if isinstance(name, list):
            # Multiple countries, eg from Country+
            code = []
            for cou in name:
                code.append(country_key(cou, country_info))

    return code

def geocode(graph, web_cache=None, country_info=None, geonames_username = None):
    # How fuzzy we accept results to be [0,1], lower = accept 'fuzzier' answers
    fuzzy = 0.8

    c_code = country_code(graph, country_info)

    if isinstance(c_code, list):
        # Ensure same order for URL even if csv etc changes
        c_code = sorted(c_code)

    def create_url(node, iteration):
        # iteration is the pass, first try with query, then with different
        # country, etc
        params = {
            'maxRows': 1,
        }

        # See if place has geocode id manually set
        if 'geocode_id' in graph.node[node]:
            # use this instead of country code
            params['geonameId'] = graph.node[node]['geocode_id']
            params['style'] = "SHORT"
            params = urllib.urlencode(params)
            url = "http://api.geonames.org/get?{0}".format(params)
            # Different URL style, form here and return
            return url

        # See if place is specified as a country
        if ('geocode_country' in graph.node[node]):
            # search on this instead
            params['q'] = graph.node[node]['geocode_country']
            # Feature class for country, region, etc
            params['featureClass'] = 'A'
            params = urllib.urlencode(params)
            url = "http://api.geonames.org/search?{0}".format(params)
            # Different URL style, form here and return
            return url

        if 'geocode_name' in graph.node[node]:
            # Manually set city to use
            city = graph.node[node]['geocode_name']
        else:
            city = graph.node[node]['label']

        city = city_cleanup(city)

        #tODO: rename city to place

        if i == 0:
            params['featureClass'] = 'P'
            # Primary search query
            if 'geocode_extent' in graph.node[node]:
                # use this instead of country code
                city_extent = city + ' ' + graph.node[node]['geocode_extent']
                params['q'] = city_extent
            elif 'geocode_append' in graph.node[node]:
                # use this instead of country code
                city_appended = city + ' ' + graph.node[node]['geocode_append']
                params['q'] = city_appended
                if isinstance(c_code, list):
                    params['country'] = ",".join(c_code)
                elif c_code == None:
                    # Don't lookup with &country=None
                    pass
                else:
                    params['country'] = c_code
            elif ('GeoExtent' in graph.graph
                and graph.graph['GeoExtent'] == 'Region'):
                city_extent = city + ' ' + graph.graph['GeoLocation']
                params['q'] = city_extent
            else:
                params['q'] = city
                if isinstance(c_code, list):
                    params['country'] = ",".join(c_code)
                elif c_code == None:
                    # Don't lookup with &country=None
                    pass
                else:
                    params['country'] = c_code
        elif i == 1:
            params['featureClass'] = 'P'
            # Try fuzzy match
            params['q'] = city
            params['fuzzy'] = fuzzy
            if isinstance(c_code, list):
                params['country'] = ",".join(c_code)
            elif c_code == None:
                # Don't lookup with &country=None
                pass
            else:
                params['country'] = c_code
        elif i == 2:
            # try for other feature classes - eg hill, area, etc
            params['q'] = city
            if isinstance(c_code, list):
                params['country'] = ",".join(c_code)
            elif c_code == None:
                # Don't lookup with &country=None
                pass
            else:
                params['country'] = c_code


        params = urllib.urlencode(params)
        url = "http://api.geonames.org/search?{0}".format(params)
        return url


    #TODO: cut out any University 1, Institution etc

    lookup_places = []


    for node, nodedata in graph.nodes(data=True):
        city = graph.node[node]['label']
        # Some nodes are skipped
        if 'skip_geocode' in nodedata and nodedata['skip_geocode'] == True:
            continue
        elif (city == '' or
            ('hyperedge' in nodedata and nodedata['hyperedge'] == 1)):
            # blank city
            continue
        elif ((len(city) <= 4 and city.isupper())
              and
             ('geocode_name' not in nodedata and 'geocode_id' not in nodedata
             and 'geocode_country' not in nodedata)):
            # Skip short capitalized names, as most likely an abbreviation
            # unless manually set geocode place or id
            continue
        elif ((len(city) <= 4 and city.isdigit())
              and
              ('geocode_name' not in nodedata and 'geocode_id' not in nodedata
              and 'geocode_country' not in nodedata)):
            # Skip numeric names unless manually set geocode place or id
            continue
        elif city == "?":
            continue
        elif ''.join([s for s in city if s.isalpha()]) in ["University",
                                                           "Institution"]:
            # Skip if only "Institution" or "University" as these are not
            # location specific, and may lead to spurious results
            # The first bit of code keeps only letters (ie remove numbers)
            # and is faster than using a regex
            continue
        elif 'Internal' in nodedata and nodedata['Internal'] == 0:
            # Skip external nodes as likely to be networks rather than places
            continue
        else:
            lookup_places.append(node)

    # Produced and Consumer function for thread
    def producer(q, urls):
        for node, url in urls.items():
            # Add the Geonames Username here before querying URL
            # if put in earlier then would complicate cache handling
            # It is only part of the way geonames is queried, not part of the
            # query
            url = "{0}&username={1}".format(url, geonames_username)
            thread = FileGetter(node, url)
            thread.start()
            q.put(thread, True)

    def consumer(q, total_urls, web_result):
        while (len(web_result) < total_urls):
            thread = q.get(True)
            thread.join()
            result = thread.get_result()
            if not result and thread.exceeded_count():
                # Exceeded tries, don't store result
                pass
            else:
                # either got result, or got no result, store either
                node_id = thread.get_node()
                web_result[node_id] = result

    geocoded = {}

    #TODO: need better handling for exceeded warning

    for i in range(3):
        # Find nodes that still need to be looked up
        need_geocode = []
        for n in lookup_places:
            if (n not in geocoded) or (n in geocoded and geocoded[n] == None):
                need_geocode.append(n)

        # Generate lookup URLs
        urls_to_fetch = {}
        if len(urls_to_fetch) > 0:
            logger.debug("Lookup iteration {0}".format(i))
        for node in need_geocode:
            url = create_url(node, i)
            use_from_cache = False
            if ('skip_cache' in graph.node[node] and
                graph.node[node]['skip_cache'] == True):
                use_from_cache = False
            elif url in web_cache:
                use_from_cache = True

            if options.skip_cache:
                # User specified to not use cache
                use_from_cache = False

            if use_from_cache:
                # Already cached, no need to look up
                geocoded[node] = web_cache[url]
            else:
                # Need to lookup
                urls_to_fetch[node] = url
                
        # Now lookup the Urls
        web_result = {}
        #TODO: make queue parameter for max web threads at once
        q = Queue(10)
        prod_thread = threading.Thread(target=producer,
                                       args=(q, urls_to_fetch))
        cons_thread = threading.Thread(target=consumer,
                                       args=(q, len(urls_to_fetch), web_result))
        prod_thread.start()
        cons_thread.start()
        prod_thread.join()
        cons_thread.join()

        # extract and cache results fetched from web
        for node, result in web_result.items():
            # Find the URL used for this node
            url = urls_to_fetch[node]
            # and store
            web_cache[url] = result
            # and store result
            geocoded[node] = result

    for node, data in geocoded.items():
        if data:
            graph.node[node]['Latitude'] =  float(data['lat'])
            graph.node[node]['Longitude'] = float(data['lng'])
            graph.node[node]['Country'] = data['countryName']
        else:
            logger.info( "No geocode match for " + graph.node[node]['label'])

    # and remove any geoname_id or geocode_name set now complete geocoding
    for node, data in graph.nodes(data=True):
        if 'geoname_id' in data:
            del graph.node[node]['geoname_id']
        if 'geocode_name' in data:
            del graph.node[node]['geocode_name']
        if 'geocode_extent' in data:
            del graph.node[node]['geocode_extent']

    return graph

def extract_svn_version(path):
    # Tries to obtain SVN version from path
    # http://code.djangoproject.com/browser/django/trunk/django/utils/version.py

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
        if len(path) == 0:
            # If running in current directory
            # TODO: clean this up, don't want to save to /merged in root fs
            output_path = path + "gml"
        else:
            output_path = path + "/gml"


    if options.dataset_only:
        output_path += "_dataset_only"
    if options.geocode:
        output_path += "_geocode"
    if options.unique_networks:
        output_path += "_unique"
    if options.ip_only:
        output_path += "_iponly"
    if options.non_ixp:
        output_path += "_nonixp"


    #output_path += strftime("%Y%m%d_%H%M%S")
    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    logger.info("Saving to folder: %s" % output_path)
    # and create cache directory for pickle files
    pickle_dir = path + "/cache"
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)

    # sanity check
    if options.dataset_only and not options.csv:
        logger.warn("Can only use dataset_only if CSV file specified")

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

    # try and get SVN version
    svn_version = extract_svn_version(path)


    web_cache = {}
    # Still load cache for writing, even if skipping cache for lookup
    cache_file = 'cache.data'
    if (os.path.isfile(cache_file)):
        f_web_cache = open(cache_file, 'rb')
        web_cache = cPickle.load(f_web_cache)
        f_web_cache.close()

    country_info = {}
    country_info_file = 'country_info.data'
    if (os.path.isfile(country_info_file)):
        country_info = cPickle.load(open(country_info_file, 'rb'))
    else:
        logger.debug("Downloading country information file from GeoNames")
        #TODO: rename this to be geocode_data as csv_data is used elsewhere
        csv_data = urllib2.urlopen("http://download.geonames.org/export/dump/countryInfo.txt")
        fieldnames = ["ISO", "ISO3", "ISONumeric", "fips", "Country", "Capital",
                      "Area", "Population", "Continent", "tld", "CurrencyCode",
                      "CurrencyName", "Phone", "PostalCodeFormat",
                      "PostalCodeRegex", "Languages", "geonameid", "Neighbors",
                      "EquivalentFipsCode"]
        csv_reader = csv.DictReader( csv_data, fieldnames = fieldnames,
                                    delimiter="\t")
        for line in csv_reader:
            c_iso = line['ISO']
            # Skip comment lines
            if c_iso[0] != "#":
                country_info[c_iso] = {
                        'ISO3': line['ISO3'],
                        'Country': line['Country'],
                        'fips': line['fips'],
                        'Continent': line['Continent'],
                        'Neighbors': line['Neighbors'],
                        }
        csv_data.close()
        # And store pickled result
        logger.debug("Saving processed country information file")
        f = open(country_info_file, 'wb')
        cPickle.dump(country_info, f, -1)
        f.close()

    non_unique_networks = []
    if options.unique_networks:
        # Look through csv data
        if not options.csv:
            logger.warn("Unique networks requires CSV data to be provided")
            sys.exit(0)
        else:
            # Look at CSV network entries
            freq_dict = defaultdict(list) 
            # Dict format of 'Network Name': 'Unique entries'
            # eg 'Renater': ['renater_2004', 'renater_2006']
            for filename, data in metadata.items():
                network_name = data['Network']
                freq_dict[network_name].append(filename)
            # Keep only those with multiple entries
            for key, val in freq_dict.items():
                if len(val) <= 1:
                    del freq_dict[key]
            for network, entries in freq_dict.items():
                # Find most recent date
                entries_dated = [ (metadata[file]['Network Date'], file) for
                                 file in entries]
                # Sorting using string comparison is fine as year is listed
                # first then month
                entries_dated.sort()
                # Remove most recent (ie last after sorted) entry
                entries_dated.pop()
                # And mark the rest to be skipped as old versions
                # Only want to list the network name, not the date used for
                # sorting
                non_unique_networks += [e[1] for e in entries_dated]

    #ToDO: check why sorting
    for source_file in sorted(network_files):
        try:
            # Extract name of network from file path
            filename = os.path.split(source_file)[1]
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
                            logger.warn(("{0} not in dataset, "
                                         "skipping").format(net_name))
                            skip_net = True

            if options.csv and options.tagged_only:
                if net_name not in metadata:
                    skip_net = True
                
            #TODO: make next two check if csv set also
            if options.ip_only:
                if (net_name in metadata and 'Layer' in metadata[net_name] and 
                    metadata[net_name]['Layer'] == 'IP'):
                    # Keep this network
                    pass
                else:
                    logger.info("{0} is a non-IP network".format(net_name))
                    skip_net = True

            if options.non_ixp:
                if (net_name in metadata and 'IX' in metadata[net_name] and 
                    metadata[net_name]['IX'] == 'IX'):
                    logger.info("{0} is an IX network".format(net_name))
                    skip_net = True


            if options.unique_networks:
                if net_name in non_unique_networks:
                    logger.info("{0} is older version of network".format(net_name))
                    skip_net = True

            if skip_net:
                logger.info("Skipping {0}".format(net_name))
                continue

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
            # Check for self loops
            for n, data in graph.nodes(data=True):
                if n in graph.neighbors(n):
                    logger.warn( "Self loop {0} {1}".format(data['label'], n))

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
            # remove directed and hiearchic from the yEd import
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
            # TODO check 'fill' in node for merge as else throw error if no fill set
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
                for node, data in graph.nodes(data=True):
                    if 'hyperedge' in data:
                        # Don't apply color class to hyperedges
                        continue
                    color = data['graphics']['fill']
                    shape = data['graphics']['type']

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
                        if key == "Classification":
                            network_tags = value.split()
                            for tag in valid_class_tags:
                                if tag in network_tags:
                                    # Network has this tag
                                    graph.graph[tag] = 1
                                else:
                                    # Network doesn't have this tag
                                    graph.graph[tag] = 0
                        # Split "developed" into true/false
                        if key == 'Developed':
                            if value == 'developed':
                                value = 1
                            else:
                                value = 0
                        if key == "NetworkDate":
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
                                    value = "{0}-{1}".format(year, month)

                                except Exception, e:
                                    print e
                                    print "WARNING: No known date for " + network_label 
                                    value = "Unknown"
                            elif len(value) == 7:
                                # correct length, see if fits date pattern
                                # partly neater way for stackoverflow.com/q/447086 
                                re_m = re.search("(\d{4})-(\d{2})", value) 
                                if re_m:
                                    year = re_m.group(1)
                                    month = re_m.group(2)

                                value = "{0}-{1}".format(year, month)
                        # Write the metadata value to the graph 
                        graph.graph[key] = value

            elif options.csv:
                # Not found, and using csv, give error
                logger.warn("{0} not found in metadata file".format(net_name))


            #*********************************
            # Geocoding (optional)
            #*********************************
            if options.geocode:
                cache_length = len(web_cache)
                geonames_username = options.geonames_username
                graph = geocode(graph, web_cache, country_info,
                                geonames_username)
                # Store cache if has changed
                if len(web_cache) != cache_length:
                    f_web_cache = open(cache_file, 'wb')
                    cPickle.dump(web_cache, f_web_cache, -1)
                    f_web_cache.close()

            #*********************************
            # Remove graphics data
            #*********************************

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
        except Exception, e:
            logger.error(e)
            traceback.print_exc(file=sys.stdout)
            


    # Write cache again to make sure any other changes stored
    f_web_cache = open(cache_file, 'wb')
    cPickle.dump(web_cache, f_web_cache, -1)
    f_web_cache.close()


#TODO: move the nested functions out to be main functions - and pass in
 #appropriate args (setup args in prev line to nested call)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
