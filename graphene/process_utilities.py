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


def valid_path_to_pick(cur_path, type_path, is_relative):
    # type_path can have two styles, absolute or partial
    # absolute: /a/b/c/ 
    # relative: .../b/c/
    if is_relative:
        # compare from right
        # t_path = type_path.replace('...', '')
        return cur_path.endswith(type_path)
    else:
        return cur_path == type_path

def attr_in_attrlist(cur_path, attr_list, is_relative = False):
    '''
    return the correct attribute name from the mapper model if exist in the path
    we are evaluating, to allow for support of relative path and futher processing
    when we construct the node attributes.
    '''
    ret_attr = None
    if is_relative:
        # check each attribute from the end of the current path
        for attr in attr_list:
            # attr_temp = attr.replace('...','')
            if cur_path.endswith(attr):
                ret_attr = attr
                break
    else:
        if cur_path in attr_list:
            ret_attr = cur_path

    return ret_attr