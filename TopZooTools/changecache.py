import os
import cPickle


cache_file = 'cache.data'
web_cache = {}
if (os.path.isfile(cache_file)):
    f_web_cache = open(cache_file, 'rb')
    web_cache = cPickle.load(f_web_cache)
    f_web_cache.close()



import pprint as pp


all_params = {}

#for key, val in web_cache.items():
#    #print key
#    key2 = key.replace("ws.geonames.org", "api.geonames.org")
#    web_cache[key2] = val
#    del web_cache[key]


pp.pprint(web_cache)
f_web_cache = open(cache_file, 'wb')
cPickle.dump(web_cache, f_web_cache, -1)
f_web_cache.close()
