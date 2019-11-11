#
# code that generate the graph (networkx) and other utilities
#

import pandas as pd
from .process_utilities import *
from .process_json import ProcessJsonData

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

def create_graph(graph, graph_mapper, data_provider, add_type_to_key = False, update = True):
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
        graph = create_graph_from_pandas(graph, graph_mapper, data_provider, add_type_to_key, update)
        
    elif isinstance(data_provider, list) and isinstance(data_provider[0], dict):
        # we have a list of dict object, which could be from JSON file reader.
        graph = create_graph_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    
    return graph




def create_graph_from_pandas(graph, graph_mapper, data_provider, add_type_to_key, update = True):
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


def create_graph_from_json(graph, graph_mapper, data_provider, add_type_to_key, update = True):
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
    # node_types = []
    # edge_types = []

    if 'nodes' in graph_mapper.keys():
        graph = create_graph_nodes_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    if 'edges' in graph_mapper.keys():
        graph = create_graph_edges_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    if 'cliques' in graph_mapper.keys():
        graph = create_graph_clique_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
            
    return graph


def extract_node_attrs_from_json(jdata, node_info):
    type_path = node_info['path']
    attr_list = node_info['lookup_attr_list']
    is_relative = node_info['is_relative']
    is_list = node_info['is_list']

    # print('>>> looking for:', type_path)
    # print('>>> looking for attrs:', attr_list)

    out = []

    # make sure our type_path end with '/'
    # correct_path(type_path)
    
    def extract_data(jdata, cur_path = '/', cur_obj = None, collect_data = False):
        if type(jdata) is dict:            
            if valid_path_to_pick(cur_path, type_path, is_relative):
                collect_data = True
                # print('<<< MATCHED_TYPE >>>')
                # print('@path:', cur_path)
                cur_obj = {}
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
#                 print('>>> got obj', cur_obj)
                out.append(cur_obj)
                collect_data = False
            # elif collect_data:
            #     for a in jdata:
            #         extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path, cur_obj, collect_data)
        else:
            # print('cur_path: {} - type_path: {}'.format(cur_path, type_path))
            if cur_obj != None:
                attr = attr_in_attrlist(cur_path, attr_list, is_relative)
                if attr != None:
                    # print('<<< MATCHED_ATTR >>>')
                    cur_obj[attr] = jdata
    
    extract_data(jdata)
    return out
                
                
def create_graph_nodes_from_json(graph, graph_mapper, 
                                 data_provider, 
                                 add_type_to_key, 
                                 update = True):
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
    
    # get list of node types and their attributes
    node_list = []
 
    nodes = graph_mapper['nodes']
 
    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_info in nodes:
        # TBD... assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        node_info = configure_node_info(node_info)
 
        attr_dict = {}
        if 'attributes' in node_info:
            for a in node_info['attributes']:
                raw_attr = a['raw']
                raw_attr = correct_path(raw_attr)
                attr_dict[a['name']] = raw_attr

        node_info['attributes'] = attr_dict
        
        # construct attribute mapping between raw_attrib_name -> attrib_name
        lookup_attr_list = list(attr_dict.values())
    
        # make sure we have the key_raw_name in the list of attributes
        lookup_attr_list = append_keys_to_lookup_attributes(node_info, lookup_attr_list)
        node_info['lookup_attr_list'] = lookup_attr_list

        node_list.append(node_info)

    count = 0

    # iterate and collect.  
    for j in raw_data:
        for node in node_list:
            jelem = extract_node_attrs_from_json(j, node)
            node['extracted_elem'] = jelem

        for node in node_list:
            jelem = node['extracted_elem']
            for e in jelem:
                # print('{} - type_found: {} - attr: {}'.format(count, node['type'], e))
                node_id = construct_key(node, e, add_type_to_key, '_UNKNOWN_')
                if not update and graph.has_node(node_id):
                    # print('graph has node', node_id)
                    continue

                attr = dict()
                attr['_type_'] = node['type']
                for k,v in node['attributes'].items():
                    attr[k] = e[v] if v in e else ''
                # print('>>> Adding node: ', node_id)
                graph.add_node(node_id, **attr)
                count += 1
        
        # print('type: {} - {}'.format(node_type_path, count))
        
    return graph
            

            
def extract_edge_attrs_from_json(jdata, from_info, to_info, attr_list):
    # print('>>> looking for from:', from_info['path'])
    # print('>>> looking for to  :', to_info['path'])
    # print('>>> looking for attrs:', attr_list)
    out = []

    # make sure our type_path end with '/'
    # correct_path(src_type_path)
    # correct_path(dst_type_path)
    from_path = from_info['path']
    from_is_relative = from_info['is_relative']
    to_path = to_info['path']
    to_is_relative = to_info['is_relative']
 
    def extract_data(jdata, cur_path = '/', cur_obj = None, collect_data = False, got_from = False, got_to = False):
        if type(jdata) is dict: 
            valid_from = valid_path_to_pick(cur_path, from_path, from_is_relative)
            valid_to   = valid_path_to_pick(cur_path, to_path, to_is_relative)
            if valid_from or valid_to:
                # print('<<< MATCHED_TYPE >>>')
                # print('@path:', cur_path)
                if not collect_data: # we need to start collecting
                    collect_data = True
                    cur_obj = {}
                    got_from = valid_from
                    got_to   = valid_to
                    # start collecting the rest of the object
                    # print('... start collecting (1)', collect_data, got_from, got_to)
                    for a in jdata:
                        extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data, got_from, got_to)
                else: # we are collecting now, but we reached a 'from' or a 'to'
                    # we need a new object to use for collecting based on the original one.
                    if valid_from and not got_from:
                        # print('....... in (2) valid_from is true', collect_data, got_from, got_to)
                        got_from = valid_from
                    elif valid_to and not got_to:
                        # print('....... in (2) valid_to is true', collect_data, got_from, got_to)
                        got_to = valid_to
                    else: # we are still collecting and we reached another valid_from or valid_to
                        # print('....... creating new object', collect_data, got_from, got_to)
                        cur_obj = cur_obj.copy() # setup a new object to collect with

                    if collect_data:
                        # continue collecting
                        # print('... continue collecting (2)', collect_data, got_from, got_to)
                        for a in jdata:
                            extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data, got_from, got_to)
                        if got_from and got_to:
                            # print('>>> collected data (2):', cur_obj)
                            out.append(cur_obj)
                            collect_data = False
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data, got_from, got_to)
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path, cur_obj, collect_data, got_from, got_to)
        else:
            # print('cur_path: {} -- from_path: {} -- to_path: {}'.format(cur_path, 
            #                                                                    from_path, to_path))
            if cur_obj != None:
                attr = attr_in_attrlist(cur_path, attr_list, from_is_relative or to_is_relative)
                if attr != None:
                    # print('<<< MATCHED_ATTR >>>')
                    cur_obj[attr] = jdata
 
    extract_data(jdata)
    return out
                
            
def create_graph_edges_from_json(graph, graph_mapper, data_provider, add_type_to_key, update = True, verbose = False):
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
    
    # get list of edge types and attributes
    edge_list = []

    edges = graph_mapper['edges']

    raw_data = data_provider
    
    for edge_info in edges:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # source node metadata
        edge_info['from'] = configure_node_info(edge_info['from'])
        
        # destination node metadata
        edge_info['to'] = configure_node_info(edge_info['to'])

        attr_dict = {}
        if 'attributes' in edge_info:
            for a in edge_info['attributes']:
                raw_attr = a['raw']
                raw_attr = correct_path(raw_attr)
                attr_dict[a['name']] = raw_attr
            
        edge_info['attributes'] = attr_dict
 
        # construct attribute mapping between raw_attrib_name_path -> attrib_name
        lookup_attr_list = list(attr_dict.values())

        # make sure we have the src_key_raw_name, and dest_key_raw_name in the list of attributes
        # (TBD: currently, we can't have both names the same)
        lookup_attr_list = append_keys_to_lookup_attributes(edge_info['from'], lookup_attr_list)
        lookup_attr_list = append_keys_to_lookup_attributes(edge_info['to'], lookup_attr_list)

        edge_info['lookup_attr_list'] = lookup_attr_list

        edge_list.append(edge_info)
        
    count = 0

    # iterate and collect.  
    for j in raw_data:
        for edge in edge_list:
    #       print('json>> ', j)
            jelem = extract_edge_attrs_from_json(j, edge['from'], edge['to'], edge['lookup_attr_list'])
            edge['extracted_elem'] = jelem
        
        for edge in edge_list:
            jelem = edge['extracted_elem']
            for e in jelem:
#                     print('{} - src: {} - dest: {} - attr: {}'.format(count, src_type_name, dst_type_name, e))
                from_id = construct_key(edge['from'], e, add_type_to_key, '_UNKNOWN_')
                to_id = construct_key(edge['to'], e, add_type_to_key, '_UNKNOWN_')

                attr = dict()
                attr['_type_'] = edge['type']
                for k,v in edge['attributes'].items():
                    attr[k] = e[v] if v in e else ''
#                     print('adding edge from: {} -> to: {}, attr: {}'.format(from_id, to_id, attr))
                graph.add_edge(from_id, to_id, **attr)
                count += 1
                
    # if verbose:
    #     print('type: {} -> {} - {}'.format(src_type_path, dst_type_path, count))
    
    return graph
            


def create_graph_clique_from_json(graph, graph_mapper, data_provider, add_type_to_key, update=True, verbose=False):

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
            # node_info['path'] = correct_path(node_info['path'])
            # for key in node_info['key']:
            #     key['raw'] = correct_path(key['raw'])
            node_info = configure_node_info(node_info)

            attr_dict = {}
            if 'attributes' in node_info:
                for a in node_info['attributes']:
                    raw_attr = a['raw']
                    raw_attr = correct_path(raw_attr)
                    attr_dict[a['name']] = raw_attr
            node_info['attributes'] = attr_dict 
            
            # construct attribute mapping between raw_attrib_name -> attrib_name
            lookup_attr_list = list(attr_dict.values())
        
            # for k, v in attr_dict.items():
            #     v = correct_path(v)
            #     lookup_attr_list.append(v)

            # for key in node_info['key']:
            #     if key['raw'] not in lookup_attr_list:
            #         lookup_attr_list.append(key['raw'])

            lookup_attr_list = append_keys_to_lookup_attributes(node_info, lookup_attr_list)
            node_info['lookup_attr_list'] = lookup_attr_list            

            # print('node_meta:', node_meta)
            node_list.append(node_info)
            
        attr = dict()
        count = 0

        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            for node in node_list:
                jelem = extract_node_attrs_from_json(j, node)
                node['extracted_elem'] = jelem

            # construct the clique
            for src_node in node_list:
                for src_elem in src_node['extracted_elem']:
                    # print('{} - type_found: {} - attr: {}'.format(count, node_type_name, e))
                    src_node_id = construct_key(src_node, src_elem, add_type_to_key)

                    for dst_node in node_list:
                        if src_node == dst_node: # we don't allow same type to link (TBD: we need to reconsider this!)
                            continue
                        for dst_elem in dst_node['extracted_elem']:
                            dst_node_id = construct_key(dst_node, dst_elem, add_type_to_key)

                            attr['_type_'] = clique_type_name
                            # print('adding edge from: {} -> to: {}, attr: {}'.format(src_node_id, dst_node_id, attr))
                            graph.add_edge(src_node_id, dst_node_id, **attr)

                            count += 1
        
                            # print('type: {} - {}'.format(node_type_path, count))
            if verbose:
                print('num cliques creates: {}'.format(count))

        
    return graph

   
