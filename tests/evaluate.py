import os
import argparse

import polars as pl
import eval_utils

def main(path_same_as: str,
         ground_truth_type: str,
         specific: bool,) -> None:
    
    # Load prediction (same_as) file
    sameas_name, sameas_mode = os.path.splitext(path_same_as)
    if sameas_mode != '.csv':
        raise ValueError("Same as file must be in CSV format.")
    pl_sameas = eval_utils.load_df(sameas_name, 'csv').select(
        ['ms_uri_1', 'ms_uri_2']
    )
    pl_sameas_reverse = pl_sameas.rename({'ms_uri_1':'ms_uri_2', 'ms_uri_2': 'ms_uri_1'})

    pl_sameas = pl.concat(
        [pl_sameas, pl_sameas_reverse],
        how='diagonal'
    ).with_columns(pl.lit(1).alias('prediction'))   

    # Load ground truth file
    if ground_truth_type not in ['w_idmt', 'w_gbw', 'ni_umidwest']:
        raise ValueError("Not a correct ground truth option."
                         "Select from 'w_idmt', 'w_gbw', and 'ni_umidwest'.")
    gt_name = os.path.join('./eval_data/', ground_truth_type.lower())
    pl_gt = eval_utils.load_df(gt_name, 'csv')

    # Join same as file and ground truth file
    pl_data = eval_utils.combine_dfs(dfgt=pl_gt,
                                     dfpred=pl_sameas)  # Column: ms_uri_1, ms_uri_2, ground_truth, prediction
    pl_data = pl_data.with_columns(pl.col('prediction').fill_null(strategy="zero"))

    # Create metrics dictionary
    dict_metrics = {}
    if specific:
        dict_metrics.update(eval_utils.f1_val(pl_data=pl_data))
    else:
        dict_metrics.update(eval_utils.f1_val(pl_data=pl_data,
                                              bool_indiv=False))
    dict_metrics.update(eval_utils.acc_val(pl_data=pl_data))
    if specific:
        dict_metrics.update(eval_utils.precision_val(pl_data=pl_data))
        dict_metrics.update(eval_utils.recall_val(pl_data=pl_data))
    
    # Print out table
    eval_utils.display_table(dict_data=dict_metrics)  

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FuseMine Evaluation')

    parser.add_argument('--same_as_file', required=True, type=str,
                        help='Directory or file where the mineral site database is located')
    
    parser.add_argument('--ground_truth_type', required=True, type=str, default='w_idmt',
                        help='Options: w_idmt, w_gbw, ni_umidwest')
    
    parser.add_argument('--specific', action='store_true',
                        help='Use tag if you will also like class-wise F1 score, precision, and recall')
    
    args = parser.parse_args()

    main(path_same_as=args.same_as_file,
         ground_truth_type=args.ground_truth_type,
         specific=args.specific,)