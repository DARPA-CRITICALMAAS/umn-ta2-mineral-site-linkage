import polars as pl
import pickle5 as pickle
import numpy as np

doc_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/temporary/ab/pl_document.pkl'
reduced_path = '/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/temporary/ab/pl_umap_set.pkl'

with open(doc_path, 'rb') as handle:
	pl_doc = pickle.load(handle)

with open(reduced_path, 'rb') as handle:
	pl_reduced = pickle.load(handle)

#list_file = pickle.load(file_path)
	
print(pl_doc)
print(pl_reduced)

# new_list = []

# for i in list_file:
# 	new_list.append(i.tolist())

# pl_file = pl.DataFrame(new_list)
# pl_file = pl_file.transpose(include_header=False)

# with open('/home/yaoyi/pyo00005/CriticalMAAS/src/umn-ta2-mineral-site-linkage/temporary/ab/pl_umap_set.pkl', 'wb') as handle:
# 	pickle.dump(pl_file, handle, protocol=pickle.HIGHEST_PROTOCOL)
