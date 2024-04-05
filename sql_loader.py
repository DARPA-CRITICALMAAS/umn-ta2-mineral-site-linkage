import requests
from bs4 import BeautifulSoup as bs

# import pickle
# import sqlite3
# import pandas as pd
# # import polars as pl

# # Create a SQL connection to our SQLite database
# con = sqlite3.connect("resource/gml.sqlite")

# gml_df = pd.read_sql_query("SELECT * FROM gml", con)


# gml_df.to_csv('tmp.csv')
# print(gml_df)

# # cur = con.cursor()


# # # The result of a "cursor.execute" can be iterated over by row
# # for row in cur.execute('SELECT * FROM gml;'):
# #     print(row)

# # # Be sure to close the connection
# con.close()

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

print(get_epsg('NAD27'))