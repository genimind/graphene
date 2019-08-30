import unittest
import networkx as nx
from graphene import graphgen

class TestClique(unittest.TestCase):

    def setUp(self):
        node_mapper = {
                }
        clique_mapper = {
                }
        data = [{
                }]

    def test_genNodes(self):
        g = nx.MultiGraph()
        g = graphgen.create_graph(g, graph_mapper = node_mapper, data_provider = data
        self.assertTrue(nx.number_of_nodes(g), 3)

# if __name__ == '__main__':
#     unittest.main()
