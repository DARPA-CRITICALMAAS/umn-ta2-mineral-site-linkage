import os
import time
import logging
import configparser

import requests
from bs4 import BeautifulSoup as bs
from pyproj import CRS

from utils.convert_dataframe import *

config = configparser.ConfigParser()
config.read('./params.ini')
geo_params = config['geolocation.params']

def get_epsg(search_epsg:str) -> str:
    page = requests.get(f"https://epsg.io/?q={search_epsg}%20%20kind%3AGEOGCRS")
    soup = bs(page.content, "html.parser")

    job_elements = soup.find_all("h4")
    if len(job_elements) == 1: 
        return 'EPSG:' + job_elements[0].find("a")['href'].strip().lstrip('/')
        
    for i in job_elements:
        a_object = i.find("a")
        if a_object.text.strip() == search_epsg:
            return 'EPSG:' + a_object['href'].strip().lstrip('/')
        
def unify_crs(pl_data, crs_column:str):
    list_crs_separated_data = pl_data.partition_by(
        crs_column
    )
    list_data = []

    for d in list_crs_separated_data:
        # If no 'crs' information given, going to infer as 4326
        if not d.item(0, 'crs'):
            d = d.drop('crs').with_columns(
                crs = pl.lit(geo_params['DEFAULT_CRS_SYSTEM'])
            )

        # if d.item(0, 'crs') == geo_params['DEFAULT_CRS_SYSTEM']:
        #     print('here')
        #     continue

        gpd_data = to_geopandas(d, 'pl', 'location')
        if d.item(0, 'crs') != geo_params['DEFAULT_CRS_SYSTEM']:
            gpd_data = gpd_data.to_crs(geo_params['DEFAULT_CRS_SYSTEM'])
        d = to_polars(gpd_data, 'gpd')
        list_data.append(d)

    pl_data = pl.concat(
        list_crs_separated_data,
        how='vertical_relaxed'
    )

    return pl_data

def check_coodinate_range(crs:str, lat:float, long:float) -> bool:
    min_lat, min_lon, max_lat, max_lon = CRS.from_user_input(crs).area_of_use.bounds

    if (min_lat <= lat <= max_lat) & (min_lon <= long <= max_lon):
        return True

    return False