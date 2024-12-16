from utils.load_kg_data import *

for commodity in ['tungsten', 'lithium']:
    pl_data = load_minmod_kg(commodity=commodity)
    pl_data.write_csv(f'/home/yaoyi/pyo00005/Spatial_Entity_Matching/data/raw/{commodity}_all.csv')