#!/usr/bin/env python
from setuptools import setup, find_packages

setup (
     name = "topzootools",
     version = "0.3.25",
     description = "Processing and conversion tools for Internet Topology Zoo",
     long_description="Processing and conversion tools for Internet Topology Zoo",
               
     # simple to run 
     entry_points = {
         'console_scripts': [
             'yed2zoo = TopZooTools.yed2zoo:main',    
             'zooconvert = TopZooTools.converter:main',         
             'zooplot = TopZooTools.geoplot:main',
             'zoofilter = TopZooTools.filter:main',
             'zoodiff = TopZooTools.diff:main',
             'zoostats= TopZooTools.zoostats:main',
             'zoogeocode= TopZooTools.geocoder:main',
             'rocketfuel2zoo= TopZooTools.rocketfuel2zoo:main',
         ],
     },

     author='Simon Knight',
     author_email="simon.knight@adelaide.edu.au",
     url="http://www.topology-zoo.org",
     packages=['TopZooTools'],  
    
        #TODO: update this package data to reflect all scripts called
     package_data = {'': ['converter.py', 'geoplot.py', 'yed2zoo.py',
         'geocoder.py', "templates/*.mako", "gitversion.py",
         'rocketfuel2zoo.py', 'filter.py', 'geocoder.py']},
     
     install_requires=['mako', 'networkx', 
         #'matplotlib', #'numpy', 
         'pyparsing'],

     classifiers = [
         "Programming Language :: Python",
         "Development Status :: 3 - Alpha",
         "Intended Audience :: Science/Research",
         "Intended Audience :: System Administrators",
         "Intended Audience :: Telecommunications Industry",
         "License :: Other/Proprietary License",
         "Operating System :: MacOS :: MacOS X",
         "Operating System :: POSIX :: Linux",
         "Topic :: System :: Networking",
         "Topic :: System :: Software Distribution",
         "Topic :: Scientific/Engineering :: Mathematics",
         ],     
 
)
