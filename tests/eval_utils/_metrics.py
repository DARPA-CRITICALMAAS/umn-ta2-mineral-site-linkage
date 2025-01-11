from typing import List, Dict

import polars as pl
import numpy as np
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score

def f1_val(pl_data: pl.DataFrame,
           bool_indiv: bool = True,
           bool_macro: bool = True,
           bool_micro:bool = False,
           bool_weighted:bool = False) -> Dict[str, float]:
    
    list_true = pl_data['ground_truth'].to_list()
    list_pred = pl_data['prediction'].to_list()

    dict_f1s = {}
    
    if bool_indiv:
        list_indiv_f1s = f1_score(y_true=list_true, y_pred=list_pred, average=None)

        for idx, i in enumerate(list_indiv_f1s):
            dict_f1s[f'Class {idx} F1'] = float(i) * 100

    if bool_macro:
        dict_f1s['Macroaveraged F1'] = float(f1_score(y_true=list_true, y_pred=list_pred, average='macro')) * 100

    if bool_micro:
        dict_f1s['Microaveraged F1'] = float(f1_score(y_true=list_true, y_pred=list_pred, average='micro')) * 100

    if bool_weighted:
        dict_f1s['Weighted F1'] = float(f1_score(y_true=list_true, y_pred=list_pred, average='weighted')) * 100

    return dict_f1s

def acc_val(pl_data: pl.DataFrame,) -> Dict[str, float]:
    list_true = pl_data['ground_truth'].to_list()
    list_pred = pl_data['prediction'].to_list()

    return {'accuracy': float(accuracy_score(y_true=list_true, y_pred=list_pred) * 100)}

def precision_val(pl_data: pl.DataFrame,) -> Dict[str, float]:
    list_true = pl_data['ground_truth'].to_list()
    list_pred = pl_data['prediction'].to_list()

    return {'precision': float(precision_score(y_true=list_true, y_pred=list_pred, zero_division=0) * 100)}

def recall_val(pl_data: pl.DataFrame,) -> Dict[str, float]:
    list_true = pl_data['ground_truth'].to_list()
    list_pred = pl_data['prediction'].to_list()
    
    return {'recall': float(recall_score(y_true=list_true, y_pred=list_pred, zero_division=0) * 100)}