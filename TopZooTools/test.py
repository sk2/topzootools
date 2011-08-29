import networkx as nx
G = nx.Graph()
G.add_node('a', x=True, y=False)
nx.write_graphml(G, 'test.graphml')
H = nx.read_graphml('test.graphml')
print H.nodes(data=True)



import sys
sys.exit(0)

import collections

class testSet(collections.Set):

    def __init__(self):
        pass


a = testSet('a')
a.add(1)
print a

class Test(object):
    def __init__(self, a):
        self.a = a

    def __call__(self, *args):
        return "args %s and self.a %s" % (args, self.a)

    def __getattr__(self, name):
        return self.name

    def __setattr__(self, name, val):
        pass



q = Test(4)
print q(55)

print q.a
