from sklearn.metrics.cluster import rand_score, adjusted_rand_score, v_measure_score, completeness_score, homogeneity_score, adjusted_mutual_info_score, normalized_mutual_info_score

def rand_index(label_true, label_test):
    """
    'rand_index' function computes similarity measure between two clustering by considering all pairs of samples. Raw rand index score is:
    RI = (number of agreeing pairs)/(number of pairs)

    : input: label_true = 
    : input: label_test = 

    : return: rand_index = similarity score betwween 0.0 and 1.0, where 1.0 indicates a perfect match
    """

    return rand_score(label_true, label_test), adjusted_rand_score(label_true, label_test)

def mutual(label_true, label_test):

    return adjusted_mutual_info_score(label_true, label_test), normalized_mutual_info_score(label_true, label_test)
    
def cluster_objectives(label_true, label_test):
    """
    'completeness' function measure whether all members of a given class are assigned to the same cluster
    """

    return homogeneity_score(label_true, label_test), completeness_score(label_true, label_test)

def v_score(label_true, label_test):
    """
    'v_score' function computes the harmonic mean between homogeneity and completeness

    : input: label_true = 
    : input: label_test = 

    : return v_measure_score = 
    """

    return v_measure_score(label_true, label_test)

def evaluate_clusters(label_true, label_test):
    rand, a_rand = rand_index(label_true, label_test)
    a_mutual, n_mutual = mutual(label_true, label_test)
    same, complete = cluster_objectives(label_true, label_test)
    v = v_score(label_true, label_test)

    return rand, a_rand, v, same, complete, a_mutual, n_mutual