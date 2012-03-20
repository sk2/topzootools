#! /usr/bin/env python

import networkx as nx

import os
import glob
import sys

import traceback

#import numpy as np

import xml.etree.ElementTree as ET

import cPickle

import urllib

import csv
import re
import time

import pprint as pp

import threading
from Queue import Queue

import urllib2

#TODO: move cache and country handling into script

#ToDO: fix logging handling now in seperate script
import logging
import logging.handlers

#TODO: make logging passed in for Class for geocode

#TODO: redo caching based on decorator using tuple of arguments passed in
# look at tuple of arguments/url?
class FileGetter(threading.Thread):
    # based on http://www.artfulcode.net/articles/multi-threading-python/
    def __init__(self, node, url ):
        self.url = url
        self.result = None
        self.node = node
        self.has_exceeded_count = False
        self.logger = logging.getLogger("geocoder")
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
                    self.logger.debug("AttributeError %s" % e)
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
                self.logger.debug("Fetching %s"% self.url)
                geocode_url = urllib2.urlopen(self.url)
                geocode_result = geocode_url.read()
                geocode_url.close()
                if overloaded_message in geocode_result:
                    self.logger.debug("Overloaded server, "
                                 "trying again in 0.5 seconds")
                    time.sleep(0.5)
                elif exceeded_message in geocode_result:
                    self.logger.warn("Exceeded limit for geonames server")
                    #TODO: make sure don't cache this result
                    self.has_exceeded_count = True
                    return None
                else:
                    # Result is fine
                    break
            except urllib2.HTTPError, e:
                self.logger.debug(e)
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

class Geocoder(object):

    def __init__(self, geonames_username, skip_cache):
        self.skip_cache = skip_cache
        if not geonames_username:
            #TODO: raise error
            print "Username must be specified for GeoNames"
            return
        self.geonames_username = geonames_username


        #TODO: work out best way to put in function but make globally accessible
        self.logger = logging.getLogger("geocoder")
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(levelname)-6s %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logging.getLogger('').addHandler(ch)
        log_dir = "."
        LOG_FILENAME =  os.path.join(log_dir, "geocoder.log")
        LOG_SIZE = 2097152 # 2 MB
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILENAME, maxBytes=LOG_SIZE, backupCount=5)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(formatter)

        self.web_cache = {}
        self.cache_file = 'cache.data'
        if (os.path.isfile(self.cache_file)):
            f_web_cache = open(self.cache_file, 'rb')
            self.web_cache = cPickle.load(f_web_cache)
            f_web_cache.close()

        self.last_written_cache_length = len(self.web_cache)

        self.country_info = {}
        country_info_file = 'country_info.data'
        if (os.path.isfile(country_info_file)):
            self.country_info = cPickle.load(open(country_info_file, 'rb'))
        else:
            self.logger.debug("Downloading country information "
                              "file from GeoNames")
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
                    self.country_info[c_iso] = {
                            'ISO3': line['ISO3'],
                            'Country': line['Country'],
                            'fips': line['fips'],
                            'Continent': line['Continent'],
                            'Neighbors': line['Neighbors'],
                            }
            csv_data.close()
            # And store pickled result
            self.logger.debug("Saving processed country information file")
            f = open(country_info_file, 'wb')
            cPickle.dump(self.country_info, f, -1)
            f.close()

    def save_cache(self):
        # Only write if changed length since last time
        if len(self.web_cache) != self.last_written_cache_length:
            f_web_cache = open(self.cache_file, 'wb')
            cPickle.dump(self.web_cache, f_web_cache, -1)
            f_web_cache.close()
            self.last_written_cache_length = len(self.web_cache) 

    def city_cleanup(self, city):
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

    def country_key(self, country):
        for key, val in self.country_info.iteritems():
            if ( val['Country'] == country
                    or val['ISO3'] == country
                    or val['fips'] == country):
                return key

    def countries_for_continent(self, continent):
        results = []
        # Lookup countries in this continent
        #TODO: use general string type checking str and unicode inherit from
        if isinstance(continent, str) or isinstance(continent, unicode):
            results = [key for (key, val) in self.country_info.iteritems()
                    if val['Continent'] == continent]

        elif isinstance(continent, list):
            results = [key for (key, val) in self.country_info.iteritems()
                    if val['Continent'] in continent]

        return results

    def country_code(self, graph):
        name = None
        code = None
        if 'GeoExtent' in graph.graph:
            geoextent = graph.graph['GeoExtent']
            geolocation = graph.graph['GeoLocation']
            continent_codes = {
                'North America':    'NA',
                'Europe':           'EU',
                # Mexico, Honduras, etc are in North America in Geonames
                'Latin America':      ['SA', 'NA'],
                'Africa':           'AF',
                'Middle East':      'AF',
                # Asia-Pacific from zoo is two continents in Geonames
                'Asia-Pacific':      ['AS', 'OC'],
            }

            if geoextent == 'Region':
                # Extract the last bit of the comma seperated region as country
                # eg Washington State, USA -> extract 'USA'
            #TODO: look at doing for eg South Australia, Victoria, Australia
                fragments = geolocation.split(",")
                #TODO: use slice notation here
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
                    code += self.countries_for_continent(cont)
                if 'Latin America' in name:
                    if 'US' in code:
                        code.remove('US')
                    if 'CA' in code:
                        code.remove('CA')
                # And key for countries
                code += [self.country_key(cou) for cou in name if cou not
                        in continent_codes]

            elif geoextent == 'Continent':
                code = []
                # get the countries in this continent
                if geolocation in continent_codes:
                    continent = continent_codes[geolocation]
                    # Fetch list of countries in this continent:
                    code = self.countries_for_continent(continent)
                    if geolocation == 'Latin America':
                        if 'US' in code:
                            code.remove('US')
                        if 'CA' in code:
                            code.remove('CA')
                else:
                    self.logger.warn("Continent %s not"
                                "in list of continents"%geolocation)
            elif geoextent == 'Continent+':
                code = []
                # convert into array and remove spaces
                name = [x.strip() for x in geolocation.split(",") ]
                # Find continent code
                continents = [continent_codes[con] for con in name if con in
                        continent_codes]
                # Find countries for continent codes
                for cont in continents:
                    code += self.countries_for_continent(cont)

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
                code = self.country_key(name)
            if isinstance(name, list):
                # Multiple countries, eg from Country+
                code = []
                for cou in name:
                    code.append(self.country_key(cou))

        return code

    def geocode(self, graph):
        # How fuzzy we accept results to be [0,1], lower = accept 'fuzzier' answers
        fuzzy = 0.8

        c_code = self.country_code(graph)

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

            city = self.city_cleanup(city)

            #tODO: rename city to place

            if i == 0:
                params['featureClass'] = 'P'
                # Primary search query
                if 'geocode_extent' in graph.node[node]:
                    # use this instead of country code
                    if graph.node[node]['geocode_extent'] == "*":
                        # Wildcard, don't append extra, but don't use country
                        # code
                        params['q'] = city
                    else:
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
            if 'Latitude' in nodedata and 'Longitude' in nodedata:
                self.logger.warn("Lat/Lon set for %s, skipping" % city)
                continue
            elif not city:
                # Blank city
                continue
            elif (city == '' 
                  or 'hyperedge' in nodedata and nodedata['hyperedge'] == True):
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
            elif nodedata.get('Internal') is False:
                # Skip ext nodes as likely to be networks rather than places
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
                url = "%s&username=%s"%(url, self.geonames_username)
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
                self.logger.debug("Lookup iteration %s" % i)
            for node in need_geocode:
                url = create_url(node, i)
                use_from_cache = False
                if ('skip_cache' in graph.node[node] and
                    graph.node[node]['skip_cache'] == True):
                    use_from_cache = False
                elif url in self.web_cache:
                    use_from_cache = True

                if self.skip_cache:
                    # User specified to not use cache
                    use_from_cache = False

                if use_from_cache:
                    # Already cached, no need to look up
                    geocoded[node] = self.web_cache[url]
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
                                        args=(q, len(urls_to_fetch), 
                                              web_result))
            prod_thread.start()
            cons_thread.start()
            prod_thread.join()
            cons_thread.join()

            # extract and cache results fetched from web
            for node, result in web_result.items():
                # Find the URL used for this node
                url = urls_to_fetch[node]
                # and store
                self.web_cache[url] = result
                # and store result
                geocoded[node] = result

        for node, data in geocoded.items():
            if data:
                graph.node[node]['Latitude'] =  float(data['lat'])
                graph.node[node]['Longitude'] = float(data['lng'])
                graph.node[node]['Country'] = data['countryName']
            else:
                self.logger.info( "No geocode match for " + 
                            graph.node[node]['label'])

        # and remove any geoname_id or geocode_name set now complete geocoding
        for node, data in graph.nodes(data=True):
            if 'geoname_id' in data:
                del graph.node[node]['geoname_id']
            if 'geocode_name' in data:
                del graph.node[node]['geocode_name']
            if 'geocode_extent' in data:
                del graph.node[node]['geocode_extent']

        return graph

def main():
    import optparse
    opt = optparse.OptionParser()
    opt.add_option('--file', '-f', help="Load data from FILE")
    opt.add_option('--directory', '-d', help="process directory")
    opt.add_option('--geonames_username', '-u', 
                default=False,
                help="Username for Geonames web service")
    opt.add_option('--skip_cache',  action="store_true",
                default=False,
                help="Don't use cache for geocoding")

    options = opt.parse_args()[0]

    if not options.geonames_username:
        print("Please enter a Geonames username to use geocoding")
        sys.exit(0)

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

    output_path = path + os.sep + "geocoded" 
    if not os.path.isdir(output_path):
        os.mkdir(output_path)
    print("Saving to folder: %s" % output_path)

    # and create cache directory for pickle files
    pickle_dir = path + os.sep + "cache"       
    if not os.path.isdir(pickle_dir):
        os.mkdir(pickle_dir)
    
    geocoder = Geocoder(options.geonames_username, options.skip_cache)
    
    for net_file in network_files:
        path, filename = os.path.split(net_file)
        network_name = os.path.splitext(filename)[0]
        print "Reading: %s" % network_name
        #TODO: add cache support
        pickle_file = "%s%s%s.pickle"% (pickle_dir, os.sep, network_name)
        if (os.path.isfile(pickle_file) and
            os.stat(net_file).st_mtime < os.stat(pickle_file).st_mtime):
            # Pickle file exists, and source_file is older
            graph = nx.read_gpickle(pickle_file)
        else:
            # No pickle file, or is outdated
            graph = nx.read_gml(net_file)
            nx.write_gpickle(graph, pickle_file)


        graph = geocoder.geocode(graph)
        geocoder.save_cache()
        gml_file =  "%s%s%s.gml"%s(output_path, os.sep, network_name)
        nx.write_gml(graph, gml_file)
        print("Wrote to %s" % gml_file)
   
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
