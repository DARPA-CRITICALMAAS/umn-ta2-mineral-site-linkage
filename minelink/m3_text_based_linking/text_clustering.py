import hdbscan

def hdbscan_text_pl(pl_data):
    cluster = hdbscan.HDBSCAN(min_samples=1)
    embedding_to_reduce = pl_data.get_column('reduced_embedding').to_list()
    cluster.fit(embedding_to_reduce)

    pl_data = pl_data.with_columns(
        labels = cluster.labels_,
        probabilities = cluster.probabilities_
    )

    return 0

def determine_cluster_pl(pl_data):
    return 0

def hdbscan_text(df_data):
    cluster = hdbscan.HDBSCAN(min_samples=1)
    cluster.fit(df_data['reduced_embedding'].to_list())

    df_data['labels'] = cluster.labels_
    df_data['probabilities'] = cluster.probabilities_

    # df_data = df_data.loc[df_data['labels'] != -1]              # means individual cluster
    df_data = df_data.loc[df_data['probabilities'] > 0.8]       # only those with high confidence

    # TODO: this will just append the cluster label and probability.
    # After probability step append the indexes of all items that are in the same grouping in the linked or store them as dictionary
    return df_data

def determine_cluster(df_data):
    # TODO: get potential clustering
    # TODO: potential cluster dictionary gives a count of how many singulars and groupings
    # TODO: those with more than 50% grouping counts become part of the group
    # TODO : make a potential cluster mwith column that lists all instances that have over 50% dominancy
    df_data['potential_cluster'] = []

    return df_data

def cosine_similarity_text(df_data):
    
    return 0