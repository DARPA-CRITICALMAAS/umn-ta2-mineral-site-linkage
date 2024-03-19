import logging
from time import perf_counter

import polars as pl

from fusemine.params import *
from fusemine.m0_load_input.load_data import load_file
from fusemine.m0_load_input.save_ckpt import save_ckpt
from fusemine.m3_interlinking.overlapping_region import *

def interlink(list_code, bool_location):
    print("interlinking started")
    seed_code = list_code.pop(0)
    pl_intra_linked = load_file([PATH_TMP_DIR, seed_code],
                                'pl_intra_linked',
                                '.pkl')
    gpd_geom = load_file([PATH_TMP_DIR, seed_code],
                         'df_geometry',
                         '.pkl')

    inter_data, inter_poly = create_polygon_region(pl_intra_linked, gpd_geom, seed_code)

    # print(inter_data)
    # print(inter_poly)
    # inter_poly.to_file('./MRDS.geojson', driver='GeoJSON')

    for i in list_code:
        against_code = i
        pl_intra_linked = load_file([PATH_TMP_DIR, against_code],
                                    'pl_intra_linked',
                                    '.pkl')
        gpd_geom = load_file([PATH_TMP_DIR, against_code],
                             'df_geometry',
                             '.pkl')
        
        against_data, against_poly = create_polygon_region(pl_intra_linked, gpd_geom, against_code)

        # against_poly.to_file('./USMIN.geojson', driver='GeoJSON')
        
        inter_data, inter_poly = location_based_linking(inter_data, inter_poly, against_data, against_poly)

        
        if not bool_location:
            print(bool_location)

    # print(inter_poly)
    
    inter_data = inter_data.select(
        pl.col(['idx', 'GroupID'])
    ).explode(
        'idx'
    ).unique(
        'idx', maintain_order=True, keep='first'
    )

    save_ckpt(data=inter_data,
              list_path=[PATH_TMP_DIR],
              file_name='pl_interlinked')
    
    print("interlinking ended")