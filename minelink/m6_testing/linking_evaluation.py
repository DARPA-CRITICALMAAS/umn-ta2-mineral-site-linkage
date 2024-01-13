from tabulate import tabulate
from sklearn.metrics.cluster import normalized_mutual_info_score, adjusted_rand_score

# Calculate accuracy
def calculate_accuracy(list_truth, list_test):
    return 0

# Calculate NMI
def calculate_NMI(list_truth, list_test):
    return normalized_mutual_info_score(list_truth, list_test)

# Calculate ARI
def calculate_ARI(list_truth, list_test):
    return adjusted_rand_score(list_truth, list_test)

def evaluation(list_truth, list_test):
    score_accuracy = calculate_accuracy(list_truth, list_test)
    score_NMI = calculate_NMI(list_truth, list_test)
    score_ARI = calculate_ARI(list_truth, list_test)

    return score_accuracy, score_NMI, score_ARI

def print_result(list_truth, list_test):
    headers = ['method', 'accuracy', 'NMI', 'ARI']
    # baseline method(list_truth, list_test)

    score_accuracy, score_NMI, score_ARI = evaluation(list_truth, list_test)

    table = [
        ['Baseline', 0, 0, 0],
        ['Our Work', score_accuracy, score_NMI, score_ARI],
    ]
    print(tabulate(table, headers, tablefmt='pretty'))