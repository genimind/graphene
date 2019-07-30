#
# code that generate the graph (networkx) and other utilities
#

import pandas as pd

#from enum import Enum
#
#class GraphEdges(Enum):
#    UNIDIR = 1
#    BIDIR  = 2
#    

def check_attributes(the_type, raw_data, attributes):
    # get list of raw_data columns (raw_data is a pandas dataframe)
    columns = raw_data.columns
    for attr in attributes:
        if attr['raw'] not in columns:
            print("mismatch between raw data and mapper: \n \
                 - {} \n \
                 - columns: {} \n \
                 - attribute: {}".format(the_type, columns.values, attr['raw']))
            return False
    return True

def create_graph(graph, graph_mapper, data_provider, update = True):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: dataframe where attributes and graph objects are created from
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''
#    # graph object
#    gObj = None
#
#    if graph_edges == GraphEdges.BIDIR:
#        gObj = nx.MultiDiGraph()
#    else:
#        gObj = nx.MultiGraph()
#        
    assert (graph != None),"Graph object wasn't constructed correctly"
    assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of node types and edge types
    node_types = []
    edge_types = []

    if 'nodes' in graph_mapper.keys():
        node_types = graph_mapper['nodes']
    if 'edges' in graph_mapper.keys():
        edge_types = graph_mapper['edges']

    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_type in node_types:
        assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        node_key = node_type['key'][0]
        id_name = node_key['name']
        id_raw_name = raw_data.columns.get_loc(node_key['raw'])

        attr_dict = {}
        for a in node_type['attributes']:
            attr_dict[a['name']] = raw_data.columns.get_loc(a['raw'])

        attr = dict()
        # count = 0
        node_type_name = node_type['type']
        for row in raw_data.itertuples(index = False):
            node_id = '{}_{}'.format(node_type_name, row[id_raw_name])
            if not update and graph.has_node(node_id):
                continue

            attr['_type_'] = node_type_name
            for k,v in attr_dict.items():
                attr[k] = row[v]
            graph.add_node(node_id, **attr)
            # count += 1
        # print(count)

        
    for edge_type in edge_types:
        
        assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        src = edge_type['from']['key'][0]['raw']
        src_type_name = edge_type['from']['type']
        src_index = raw_data.columns.get_loc(src)

        dst = edge_type['to']['key'][0]['raw']
        dst_index = raw_data.columns.get_loc(dst)
        dst_type_name = edge_type['to']['type']

        attr_dict = {}
        for a in edge_type['attributes']:
            attr_dict[a['name']] = raw_data.columns.get_loc(a['raw'])
        
        attr = dict()
        for row in raw_data.itertuples(index = False):
            attr['_type_'] = edge_type['type']
            for k,v in attr_dict.items():
                attr[k] = row[v]
            from_id = '{}_{}'.format(src_type_name, row[src_index])
            to_id   = '{}_{}'.format(dst_type_name, row[dst_index])
            graph.add_edge(from_id, to_id, **attr)
        
        
    return graph

