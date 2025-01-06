from typing import List, Dict

import numpy as np
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score

def f1_val(list_true:list,
           list_pred:list,
           bool_indiv: bool = True,
           bool_macro: bool = True,
           bool_micro:bool = False,
           bool_weighted:bool = False) -> List[Dict[str, float]]:
    
    list_f1s = []

    if bool_indiv:
        list_indiv_f1s = f1_score(y_true=list_true, y_pred=list_pred, average=None)

        for idx, i in enumerate(list_indiv_f1s):
            list_f1s.append({f'Class {idx} F1: ': float(i) * 100})

    if bool_macro:
        list_f1s.append({'Macroaveraged F1': float(f1_score(y_true=list_true, y_pred=list_pred, average='macro')) * 100})

    if bool_micro:
        list_f1s.append({'Microaveraged F1': float(f1_score(y_true=list_true, y_pred=list_pred, average='micro')) * 100})

    if bool_weighted:
        list_f1s.append({'Weighted F1': float(f1_score(y_true=list_true, y_pred=list_pred, average='weighted')) * 100})

    return list_f1s

def acc_val(list_true:list,
            list_pred:list,) -> Dict[str, float]:

    return float(accuracy_score(y_true=list_true, y_pred=list_pred) * 100)

def precision_val(list_true:list,
                  list_pred:list,) -> Dict[str, float]:

    return float(precision_score(y_true=list_true, y_pred=list_pred, zero_division=0) * 100)

def recall_val(list_true:list,
               list_pred:list,) -> Dict[str, float]:
    
    return float(recall_score(y_true=list_true, y_pred=list_pred, zero_division=0) * 100)