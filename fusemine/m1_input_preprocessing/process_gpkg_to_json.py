import argparse
import json
import glob
import os

import geopandas as gpd
import pandas as pd

import fiona
from fiona.model import to_dict

# TA4 gpkg schema 
# https://github.com/DARPA-CRITICALMAAS/ta1-geopackage/blob/47f585a0386dd5db3e7a9d96cc53d1e1b4f2ce10/criticalmaas/ta1_geopackage/fixtures/03-tables.sql

def main(args):
    gpkg_files = glob.glob(os.path.join(args.input_gpkg_path, "*.gpkg"))

    entire_df = pd.DataFrame()
    for gpkg_file in gpkg_files:
        point_geom_df = gpd.read_file(gpkg_file, layer='point_feature')
        point_type_df = gpd.read_file(gpkg_file, layer='point_type')
        point_type_df = point_type_df.rename(columns={'id': 'type_id'})

        merged_df = pd.merge(point_geom_df, point_type_df[['type_id','description']], how='left', left_on='type', right_on='type_id')
        merged_df = merged_df[merged_df['description'].isin(['quarry','mine_shaft','mine_tunnel'])]
        
        if merged_df.empty:
            continue

        merged_df['id'] = merged_df['id'] + "_"+ merged_df['description']
        merged_df = merged_df[['map_id','id','geometry']]
        entire_df = pd.concat([entire_df, merged_df], ignore_index=True)

    entire_df = entire_df.rename(columns={'map_id': 'source_id','id': 'record_id','geometry':'location_info'})
    entire_df['location_info'] = entire_df['location_info'].apply(lambda x: dict({'geometry': f'POINT ({x[0].x} {x[0].y})','crs':'EPSG:3857'}))
    
    out_dict = entire_df.to_dict(orient='records')

    with open(os.path.join(args.output_path, 'ta1_point.json'), 'w') as f:
        json.dump({"MineralSite": out_dict}, f, indent=4, default=str)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ta1_ta2_pt')
    parser.add_argument('--input_gpkg_path', type=str, default='./gpkg/', help='input path for gpkg files')
    parser.add_argument('--output_path', type=str, default='./ta1_output/', help='output path')

    args = parser.parse_args()
    main(args)
