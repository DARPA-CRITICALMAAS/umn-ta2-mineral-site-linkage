import logging
import polars as pl
from tabulate import tabulate
from itertools import combinations
from sklearn.metrics import f1_score, accuracy_score

def perf_accuracy(pl_ground_truth, pl_prediction) -> float:
    list_ground_truth_groups = pl_ground_truth.group_by('GroupID').agg([pl.all()]).with_columns(
        groups = pl.col('record_id')
    )['groups'].to_list()

    ground_truth_combinations = []
    for i in list_ground_truth_groups:
        ground_truth_combinations.extend(combinations(i,2))

    logging.info(f'Count of groups in ground truth: {len(list_ground_truth_groups)}')

    list_prediction_groups = pl_prediction.group_by('GroupID').agg([pl.all()]).with_columns(
        groups = pl.col('record_id'),
    )['groups'].to_list()

    prediction_combinations = []
    for i in list_prediction_groups:
        prediction_combinations.extend(combinations(i,2))

    logging.info(f'Count of groups in prediction: {len(list_prediction_groups)}')

    matching = 0

    for item in list_prediction_groups:
        if item in list_ground_truth_groups:
            matching += 1

    logging.info(f'Count of exactly matching sites: {matching}')

    all_records = pl_prediction['record_id'].to_list()
    all_potential = combinations(all_records, 2)

    quant_truth = []
    quant_predi = []
    for i in all_potential:
        if i in ground_truth_combinations:
            quant_truth.append(1)
        else:
            quant_truth.append(0)
        if i in prediction_combinations:
            quant_predi.append(1)
        else:
            quant_predi.append(0)

    return matching / len(list_ground_truth_groups), f1_score(quant_truth, quant_predi), f1_score(quant_truth, quant_predi, average='weighted')

def print_evaluation_table(fusemine_version:str, pl_ground_truth, pl_prediction):
    list_ground_truth = pl_ground_truth['GroupID'].to_list()
    list_prediction = pl_prediction['GroupID'].to_list()

    accuracy_score, link_acc_score, link_f1_score = perf_accuracy(pl_ground_truth, pl_prediction)
                                    
    headers = ["FuseMine Version", "Accuracy"]
    table = [['Naive', '0.0707'],
            [fusemine_version, accuracy_score]]

    print(tabulate(table, headers, tablefmt="outline", floatfmt=".4f"))
    print(f'Link-level Accuracy: {link_acc_score}')
    print(f'Link-level F1: {link_f1_score}')