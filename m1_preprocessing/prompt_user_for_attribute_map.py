def prompt_user_for_attribute(target_attribute:str, list_mineralsite_attributes:list) -> str:
    """
    
    : param: target_attribute = 
    : param: list_mineralsite_attributes = 

    : return: identified_attribute = 
    """
    identified_attribute = input("Attribute representing {target_attribute}: ")

    while identified_attribute not in list_mineralsite_attributes:
        identified_attribute = input("Attribute representing {target_attribute}: ")

    return identified_attribute

def map_schema_attributes_by_user_input(source_name:str, pl_mineralsite) -> dict:
    """
    
    """
    dict_attribute_map = {
        'record id': '',
        'mineral site name': '',
        'latitude': '',
        'longitude': '',
        'crs': '',
        'country': '',
        'state or province': '',
        'deposit type': ''
    }

    list_mineralsite_attributes = list(pl_mineralsite.columns)
    print(f"Columns available for {source_name}: {list_mineralsite_attributes}")

    for target_attribute, _ in dict_attribute_map.items():
        user_identified_attribute = prompt_user_for_attribute(target_attribute, list_mineralsite_attributes)
        dict_attribute_map[target_attribute] = user_identified_attribute  

    return dict_attribute_map