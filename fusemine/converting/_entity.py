from Typing import List, Dict
import polars as pl

def entity2id(list_entity: list,
              dict_entities: Dict[str, str]) -> list:
    """
    Converts natural string entity to the minmod QID
    minmod entity to QID mapping available here: https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/entities

    Arguments
    : list_entity_id: Entity input
    : entity_type: Entity type such as commodity, state, country.(default=None)

    Return
    """
    list_entity_id = []
    list_unidentified = []

    for entity in list_entity:
        try:
            list_entity_id.append(dict_entities[entity.lower()])
        except:
            list_unidentified.append(entity)
    
    return list_entity_id, list_unidentified

# def id2entity(entity_id: str,):
#     """
#     Converts minmod QID to natural string entity
#     minmod entity to QID mapping available here: https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/entities

#     Arguments
#     : fusemine_model:
#     : entity_id: 

#     Return
#     """
#     if not entity_id:
#         return -1
    
#     if entity_id == 'ALL':
#         return 'ALL'
    
#     return 0