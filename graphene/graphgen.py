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
    mapper_types = ['nodes', 'edges', 'cliques']
    assert (graph != None),"Graph object wasn't constructed correctly"
    assert (any(x in mapper_types for x in graph_mapper.keys())), "Graph mapper is missing one of the mapper types: {}".format(mapper_types)
            
    if isinstance(data_provider, pd.DataFrame):
        graph = create_graph_from_pandas(graph, graph_mapper, data_provider, update)
        
    elif isinstance(data_provider, list) and isinstance(data_provider[0], dict):
        # we have a list of dict object, which could be from JSON file reader.
        graph = create_graph_from_json(graph, graph_mapper, data_provider, update)
    
    return graph




def create_graph_from_pandas(graph, graph_mapper, data_provider, update = True):
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
            node_id = (node_type_name, str(row[id_raw_name]))
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
            from_id = (src_type_name, str(row[src_index]))
            to_id   = (dst_type_name, str(row[dst_index]))
            graph.add_edge(from_id, to_id, **attr)
        
        
    return graph


def create_graph_from_json(graph, graph_mapper, data_provider, update = True):
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

    assert (isinstance(data_provider, list) and isinstance(data_provider[0], dict)), "The data provider should be a list of dict"
    
    # get list of node types and edge types
    node_types = []
    edge_types = []

    if 'nodes' in graph_mapper.keys():
        graph = create_graph_nodes_from_json(graph, graph_mapper, data_provider, update)
    if 'edges' in graph_mapper.keys():
        graph = create_graph_edges_from_json(graph, graph_mapper, data_provider, update)
    if 'cliques' in graph_mapper.keys():
        graph = create_graph_clique_from_json(graph, graph_mapper, data_provider, update)
            
    return graph


def extract_node_attrs_from_json(jdata, type_path, attr_list):
#     print('>>> looking for:', type_path)
#     print('>>> looking for attrs:', attr_list)
    out = []

    # make sure our type_path end with '/'
    if type_path[-1] != '/':
        type_path += '/' 
    
    
    def extract_data(jdata, cur_path = '/', cur_obj = None):
        if type(jdata) is dict:            
            if cur_path == type_path:
#                 print('<<< MATCHED_TYPE >>>')
#                 print('@path:', cur_path)
                obj = {}
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', obj)
#                 print('>>> got obj', obj)
                out.append(obj)
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/')
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path)
        else:
#             print('cur_path: {} - type_path: {}'.format(cur_path, type_path))
            if cur_obj != None and cur_path in attr_list:
#                 print('<<< MATCHED_ATTR >>>')
                cur_obj[cur_path] = jdata
    
    extract_data(jdata)
    return out
                
                
def create_graph_nodes_from_json(graph, graph_mapper, data_provider, update = True):
    '''
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of node types and edge types
    node_list = []
 
    nodes = graph_mapper['nodes']
 
    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_info in nodes:
        # TBD... assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
#         print('node_type to process:', node_type)
        if node_info['path'][-1] != '/':
            node_info['path'] += '/' 
 
        for key in node_info['key']:
            if key['raw'][-1] != '/':
                key['raw'] += '/'


        attr_dict = {}
        if 'attributes' in node_info:
            for a in node_info['attributes']:
                raw_attr = a['raw']
                if raw_attr[-1] != '/':
                    raw_attr += '/'
                attr_dict[a['name']] = raw_attr

        node_info['attributes'] = attr_dict
        
        # construct attribute mapping between raw_attrib_name -> attrib_name
        lookup_attr_list = []
    
        for k, v in attr_dict.items():
            if v[-1] != '/':
                v += '/'
            lookup_attr_list.append(v)
        
        # make sure we have the key_raw_name in the list of attributes
        for key in node_info['key']:
            if key['raw'] not in lookup_attr_list:
                lookup_attr_list.append(key['raw'])

        node_info['lookup_attr_list'] = lookup_attr_list

        node_list.append(node_info)

    attr = dict()
    count = 0


    # iterate and collect.  
    for j in raw_data:
        for node in node_list:
            jelem = extract_node_attrs_from_json(j, node['path'], node['lookup_attr_list'])
            node['extracted_elem'] = jelem

        for node in node_list:
            jelem = node['extracted_elem']
            for e in jelem:
                # print('{} - type_found: {} - attr: {}'.format(count, node['type'], e))
                node_id = tuple(get_key_list(node, e, '_UNKNOWN_'))
                if not update and graph.has_node(node_id):
#                         print('graph has node', node_id)
                    continue

                attr['_type_'] = node['type']
                for k,v in attr_dict.items():
                    attr[k] = e[v] if v in e else ''
                # print('>> adding node: ', node_id)
                graph.add_node(node_id, **attr)
                count += 1
        
        # print('type: {} - {}'.format(node_type_path, count))
        
    return graph
            

            
def extract_edge_attrs_from_json(jdata, src_type_path, dst_type_path, attr_list):
#     print('>>> looking for:', type_path)
#     print('>>> looking for attrs:', attr_dict)
    out = []

    # make sure our type_path end with '/'
    if src_type_path[-1] != '/':
        src_type_path += '/' 
    
    if dst_type_path[-1] != '/':
        dst_type_path += '/' 
        
    def extract_data(jdata, cur_path = '/', cur_obj = None, got_from = False, got_to = False):
        if type(jdata) is dict:            
            if cur_path == src_type_path or cur_path == dst_type_path:
                if cur_path == src_type_path:
                    got_from = True
                else:
                    got_to = True
#                 print('<<< MATCHED_TYPE >>>')
#                 print('@path:', cur_path)
                obj = {}
                if cur_obj != None: # we're still collecting
                    obj = cur_obj
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', obj, got_from, got_to)
#                     print('>>> got edge data:', obj)
#                 print('got_from:', got_from, ' - got_to:', got_to)
                if got_from and got_to:
                    out.append(obj)
                    got_from = got_to = False
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, got_from, got_to)
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path, cur_obj, got_from, got_to)
        else:
#             print('cur_path: {} - src_type_path: {} - dst_type_path: {}'.format(cur_path, 
#                                                                                 src_type_path, dst_type_path))
            if cur_obj != None and cur_path in attr_list:
#                 print('<<< MATCHED_ATTR >>>')
                cur_obj[cur_path] = jdata

    extract_data(jdata)
    return out
                
            
def create_graph_edges_from_json(graph, graph_mapper, data_provider, update = True, verbose = False):
    '''
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of edge types and edge types
    edge_types = []

    if 'edges' in graph_mapper.keys():
        edge_types = graph_mapper['edges']

    raw_data = data_provider
    
#     print(edge_types)
    for edge_type in edge_types:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        edge_type_name = edge_type['type']

        # source node metadata
        src_type_name = edge_type['from']['type']
        src_type_path = edge_type['from']['path']
        if src_type_path[-1] != '/':
            src_type_path += '/' 

        src_key = edge_type['from']['key'][0]
        src_key_name = src_key['name']
        src_key_raw_name = src_key['raw']
        if src_key_raw_name[-1] != '/':
            src_key_raw_name += '/'
        
        # destination node metadata
        dst_type_name = edge_type['to']['type']
        dst_type_path = edge_type['to']['path']
        if dst_type_path[-1] != '/':
            dst_type_path += '/' 

        dst_key = edge_type['to']['key'][0]
        dst_key_name = dst_key['name']
        dst_key_raw_name = dst_key['raw']
        if dst_key_raw_name[-1] != '/':
            dst_key_raw_name += '/'
    

        attr_dict = {}
        if 'attributes' in edge_type:
            for a in edge_type['attributes']:
                v = a['raw']
                if v[-1] != '/':
                    v += '/'
                attr_dict[a['name']] = v
            
        attr = dict()
        count = 0
 
        # construct attribute mapping between raw_attrib_name_path -> attrib_name
        lookup_attr_list = []
    
        # TBD: review this code, not sure if it's necessary
        for k, v in attr_dict.items():
            lookup_attr_list.append(v)


        # make sure we have the src_key_raw_name in the list of attributes
        if src_key_raw_name not in lookup_attr_list:
            lookup_attr_list.append(src_key_raw_name)
            
        # make sure we have the dst_key_raw_name in the list of attributes
        if dst_key_raw_name not in lookup_attr_list:
            lookup_attr_list.append(dst_key_raw_name)

#         print('lookup_attr_list:', lookup_attr_list)

        
        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            jelem = extract_edge_attrs_from_json(j, src_type_path, dst_type_path, lookup_attr_list)
            if len(jelem) > 0:
                for e in jelem:
#                     print('{} - src: {} - dest: {} - attr: {}'.format(count, src_type_name, dst_type_name, e))
                    src_key_value = e[src_key_raw_name] if src_key_raw_name in e else '_UNKNOWN_'
                    dst_key_value = e[dst_key_raw_name] if dst_key_raw_name in e else '_UNKNOWN_'
        
                    attr['_type_'] = edge_type_name
                    for k,v in attr_dict.items():
                        attr[k] = e[v] if v in e else ''
                    from_id = (src_type_name, str(src_key_value))
                    to_id = (dst_type_name, str(dst_key_value))
#                     print('adding edge from: {} -> to: {}, attr: {}'.format(from_id, to_id, attr))
                    graph.add_edge(from_id, to_id, **attr)
                    
                    count += 1
        if verbose:
            print('type: {} -> {} - {}'.format(src_type_path, dst_type_path, count))
       
    return graph
            


# def extract_clique_attrs_from_json(jdata, node_list, attr_list):
#     '''
#         For now we'll assume that all attributes are available and there are no
#         multiple group of cliques within the same group. We might need to revist
#         this later (TBD)
#     '''
# #     print('>>> looking for attrs:', attr_list)
#     out = []

#     def extract_data(jdata, cur_path = '/', cur_obj = None):
#         if type(jdata) is dict:            
#             for a in jdata:
#                 extract_data(jdata[a], cur_path + a + '/', cur_obj)
#                 if all(x in attr_list for x in cur_obj.keys()): # we got all attributres
#                     out.append(cur_obj)
#                     cur_obj = {}
#         elif type(jdata) is list:
#             for a in jdata:
#                 extract_data(a, cur_path, cur_obj)
#         else:
# #             print('cur_path: {} '.format(cur_path))
#             if cur_obj != None and cur_path in attr_list:
# #                 print('<<< MATCHED_ATTR >>>', cur_path)
#                 cur_obj[cur_path] = jdata

#     extract_data(jdata, cur_obj={})
#     return out
 

def create_graph_clique_from_json(graph, graph_mapper, data_provider, update=True, verbose=False):

    '''
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''


    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of edge types and edge types
    cliques = []

    if 'cliques' in graph_mapper.keys():
        cliques = graph_mapper['cliques']

    raw_data = data_provider
    
#     print(cliques)
    for clique in cliques:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        clique_type_name = clique['type']

        # clique nodes metadata
        nodes = clique['nodes']
        node_list = []

        # collect node information needed to pick data.
        for node_info in nodes:
            # TBD... assert check_attributes(node_info, raw_data, node_info['attributes'])        
        #         print('node_info to process:', node_info)
            if node_info['path'][-1] != '/':
                node_info['path'] += '/'
            for key in node_info['key']:
                if key['raw'][-1] != '/':
                    key['raw'] += '/'

            attr_dict = {}
            if 'attributes' in node_info:
                for a in node_info['attributes']:
                    raw_attr = a['raw']
                    if raw_attr[-1] != '/':
                        raw_attr += '/'
                    attr_dict[a['name']] = raw_attr
            node_info['attributes'] = attr_dict 
            
            # construct attribute mapping between raw_attrib_name -> attrib_name
            lookup_attr_list = []
        
            for k, v in attr_dict.items():
                if v[-1] != '/':
                    v += '/'
                lookup_attr_list.append(v)

            for key in node_info['key']:
                if key['raw'] not in lookup_attr_list:
                    lookup_attr_list.append(key['raw'])

            node_info['lookup_attr_list'] = lookup_attr_list            

            # print('node_meta:', node_meta)
            node_list.append(node_info)
            
        attr = dict()
        count = 0

        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            for node in node_list:
                jelem = extract_node_attrs_from_json(j, node['path'], node['lookup_attr_list'])
                node['extracted_elem'] = jelem

            # construct the clique
            for src_node in node_list:
                for src_elem in src_node['extracted_elem']:
                    # print('{} - type_found: {} - attr: {}'.format(count, node_type_name, e))
                    src_node_id = tuple(get_key_list(src_node, src_elem))

                    for dst_node in node_list:
                        if src_node == dst_node: # we don't allow same type to link (TBD: we need to reconsider this!)
                            continue
                        for dst_elem in dst_node['extracted_elem']:
                            dst_node_id = tuple(get_key_list(dst_node, dst_elem))

                            attr['_type_'] = clique_type_name
                            # print('adding edge from: {} -> to: {}, attr: {}'.format(src_node_id, dst_node_id, attr))
                            graph.add_edge(src_node_id, dst_node_id, **attr)

                            count += 1
        
                            # print('type: {} - {}'.format(node_type_path, count))
            if verbose:
                print('num cliques creates: {}'.format(count))

        
    return graph

   
def get_key_list(node_info, node_data, invalid_value = None):
    # print('in.... get_key_list() - node_info:', node_info)
    key_list = []
    if 'type_in_key' in node_info and True == node_info['type_in_key']:
        key_list.append(node_info['type'])

    for key in node_info['key']:
        key_raw_name = key['raw']
        key_value = node_data[key_raw_name] if key_raw_name in node_data else invalid_value
        key_list.append(str(key_value))

    return key_list

