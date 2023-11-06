import pandas as pd
from sklearn.cluster import DBSCAN, HDBSCAN, Birch

from utils.params import *

def db(arr):
    """
    'db' function

    """
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.045).fit(arr) #0.045
    # clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.007).fit(arr)
    labels = pd.Series(clusters.labels_).rename('GroupID')

    return labels
    
def hdb(length, value, arr):
    clusters = HDBSCAN(min_cluster_size=2, cluster_selection_epsilon=0.007).fit(arr)
    labels = []
    
    for l in clusters.labels_:
        if l == -1:
            labels.append(-1)
        else:
            labels.append(value + ((length+1)**l))

    # print(labels)
    # ser_labels = pd.Series(labels).rename('GroupID')

    return labels

def location_group(dataframe):
    """
    'location_group' function clusters points based on their location
    Goal of the function is to have no false negative

    : input: dataframe = 

    : return: dataframe = 
    """
    # dataframe = dataframe.to_crs(epsg='3857')
    df_combined = pd.concat([dataframe['geometry'].x, dataframe['geometry'].y], axis=1)
    df_combined.columns = ['X', 'Y']
    coords = df_combined[['X', 'Y']].to_numpy()

    labels = db(coords)
    
    # clusters = DBSCAN(eps=EPSILON, min_samples=MIN_SAMPLE_POLYGONS).fit(coords)
    # labels = pd.Series(clusters.labels_).rename('GroupID')
    dataframe = pd.concat([dataframe, labels], axis=1)

    return dataframe