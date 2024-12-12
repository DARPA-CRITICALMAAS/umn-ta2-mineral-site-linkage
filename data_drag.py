from utils.load_kg_data import *

commodity = 'tungsten'
pl_data = load_minmod_kg(commodity='commodity')

pl_data.write_csv('/home/yaoyi/pyo00005/Spatial_Entity_Matching/data/raw/tungsten_all.csv')