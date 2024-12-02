import polars as pl

def entity2id(fusemine_model,
              entity: str,
              entity_type: str=None) -> str:
    """
    Converts natural string entity to the minmod QID
    minmod entity to QID mapping available here: https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/entities

    Arguments
    : fusemine_model:
    : entity: Entity input
    : entity_type: Entity type such as commodity, state, country.(default=None)

    Return
    """
    if not entity:
        return -1

    if entity == 'ALL':
        return 'ALL'
    
    return 0

def id2entity(fusemine_model,
              entity_id: str,):
    """
    Converts minmod QID to natural string entity
    minmod entity to QID mapping available here: https://github.com/DARPA-CRITICALMAAS/ta2-minmod-data/tree/main/data/entities

    Arguments
    : fusemine_model:
    : entity_id: 

    Return
    """
    if not entity_id:
        return -1
    
    if entity_id == 'ALL':
        return 'ALL'
    
    return 0