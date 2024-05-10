import logging
import polars as pl
from tabulate import tabulate

def perf_accuracy(pl_ground_truth, pl_prediction) -> float:
    list_ground_truth_groups = pl_ground_truth.group_by('GroupID').agg([pl.all()]).with_columns(
        groups = pl.col('record_id')
    )['groups'].to_list()

    logging.info(f'Count of groups in ground truth: {len(list_ground_truth_groups)}')

    list_prediction_groups = pl_prediction.group_by('GroupID').agg([pl.all()]).with_columns(
        groups = pl.col('record_id'),
    )['groups'].to_list()

    logging.info(f'Count of groups in ground truth: {len(list_prediction_groups)}')

    matching = 0
    for item in list_prediction_groups:
        if item in list_ground_truth_groups:
            matching += 1

    logging.info(f'Count of exactly matching sites: {matching}')

    return matching / len(list_ground_truth_groups)

def print_evaluation_table(fusemine_version:str, pl_ground_truth, pl_prediction):
    list_ground_truth = pl_ground_truth['GroupID'].to_list()
    list_prediction = pl_prediction['GroupID'].to_list()

    accuracy_score = perf_accuracy(pl_ground_truth, pl_prediction)
                                    
    headers = ["FuseMine Version", "Accuracy"]
    table = [['Naive', '0.0707'],
            [fusemine_version, accuracy_score]]

    print(tabulate(table, headers, tablefmt="outline", floatfmt=".4f"))