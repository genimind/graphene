'''
Utility functions
'''
def correct_path(str_val):
    if str_val[-1] != '/':
        str_val += '/' 
    return str_val
    
def construct_key(node_info, node_data, add_type_to_key, invalid_value = None):
    # print('in.... get_key_list() - node_info:', node_info)
    key_list = []
    if add_type_to_key:
        key_list.append(node_info['type'])

    for key in node_info['key']:
        key_raw_name = key['raw']
        key_value = node_data[key_raw_name] if key_raw_name in node_data else invalid_value
        key_list.append(str(key_value))

    return tuple(key_list)

