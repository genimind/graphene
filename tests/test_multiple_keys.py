import unittest
import networkx as nx
from graphene import graphgen

class TestMultipleKeys(unittest.TestCase):

    def setUp(self):
        self.node_mapper = {
            'nodes': [
                {
                    'type': 'TypeA',
                    'path': '/',
                    'type_in_key': True,
                    'key' : [
                        {'name': 'attr2', 'raw': '/type_a_attr2'}
                        ],
                    'attributes': [
                        {'name': 'attr1', 'raw': '/type_a_attr1'},
                        {'name': 'attr2', 'raw': '/type_a_attr2'}
                        ]
                },
                {
                    'type': 'TypeB',
                    'path': '/path1',
                    'type_in_key': True,
                    'key' : [
                        {'name': 'attr2', 'raw': '/path1/type_b_attr2'},
                        {'name': 'attr1', 'raw': '/path1/type_b_attr1'}
                        ],
                    'attributes': [
                        {'name': 'attr1', 'raw': '/path1/type_b_attr1'},
                        {'name': 'attr2', 'raw': '/path1/type_b_attr2'}
                        ]
                },
                {
                    'type': 'TypeC',
                    'path': '/path1/path2',
                    'type_in_key': False,
                    'key' : [
                        {'name': 'attr1', 'raw': '/path1/path2/type_c_attr'}
                        ]
                },
            ]               
        }
        self.clique_mapper = {
            'cliques': [
                {
                    'type': '__default__',
                    'nodes': [
                        {
                            'type': 'TypeA',
                            'path': '/',
                            'type_in_key': True,
                            'key' : [
                                {'name': 'attr2', 'raw': '/type_a_attr2'}
                                ],
                        },
                        {
                            'type': 'TypeB',
                            'path': '/path1',
                            'type_in_key': True,
                            'key' : [
                                {'name': 'attr2', 'raw': '/path1/type_b_attr2'},
                                {'name': 'attr1', 'raw': '/path1/type_b_attr1'}
                                ]
                        },
                        {
                            'type': 'TypeC',
                            'path': '/path1/path2',
                            'type_in_key': False,
                            'key' : [
                                {'name': 'attr1', 'raw': '/path1/path2/type_c_attr'}
                                ]
                        },
                    ]   
                }
            ]
        }
        self.data = [
            {
                "type_a_attr1" : "type_a_val1",
                "type_a_attr2" : "type_a_val2",
                "path1" : {
                    "type_b_attr1" : "type_b_val1",
                    "type_b_attr2" : "type_b_val2",
                    "path2" : {
                        "type_c_attr" : "type_c_val",
                        }
                    } 
            }
        ]

    def test_genNodes(self):
        g = nx.MultiGraph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data)
        self.assertTrue(nx.number_of_nodes(g), 3)
        # get node with key.
        key1 = ('TypeA', 'type_a_val2')
        key2 = ('TypeB', 'type_b_val2', 'type_b_val1')
        self.assertTrue(key1 in g.nodes)
        self.assertTrue(key2 in g.nodes)
        print(g.node[key1])
        print(g.node[key2])
        keyU = ('TypeA', '_UNKNOWN_')
        self.assertFalse(keyU in g.nodes)
    
    def test_genClique(self):
        g = nx.MultiGraph()
        g = graphgen.create_graph(g,
                graph_mapper = self.clique_mapper,
                data_provider = self.data)
        self.assertTrue(nx.number_of_edges(g), 3)
        # locate an edge
        key1 = ('TypeA', 'type_a_val2')
        key2 = ('TypeB', 'type_b_val2', 'type_b_val1')
        self.assertTrue(g.has_node(key1))
        self.assertTrue(key2 in g)
        self.assertTrue(g.has_edge(key1, key2))
        key3 = ('TypeC', 'ABCDEF') # invalid node key
        self.assertFalse(key3 in g)
        self.assertFalse(g.has_edge(key1, key3))


# if __name__ == '__main__':
#     unittest.main()
