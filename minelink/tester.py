import os
from argparse import ArgumentParser
from sklearn.metrics.cluster import rand_score, completeness_score, homogeneity_score

from minelink.load_save import *

# TODO: move the orderby part under evaluate
# def dataframe_orderby(dataframe, col):
#     dataframe = dataframe.sort_values(by=[col])
#     dataframe = dataframe.reset_index(drop=True)

#     list_group = dataframe['GroupID'].tolist()

#     return dataframe, list_group

def evaluate(df_true, df_predict):
    unique_indicator = 'dep_id'

    df_true = df_true.sort_values(by=unique_indicator).reset_index(drop=True)
    df_predict = df_predict.sort_values(by=unique_indicator).reset_index(drop=True)
    
    list_true = df_true['GroupID'].tolist()
    list_predict = df_predict['GroupID'].tolist()

    df_true = df_true.groupby(['GroupID']).agg(lambda x: list(x))
    df_predict = df_predict.groupby(['GroupID']).agg(lambda x: list(x))

    df_match = df_predict[df_predict[unique_indicator].isin(df_true[unique_indicator])]
    count_true_groups = df_true.shape[0]
    count_match_groups = df_match.shape[0]

    return rand_score(list_true, list_predict), completeness_score(list_true, list_predict), homogeneity_score(list_true, list_predict), count_match_groups/count_true_groups

def main(args):
    # loader(path_dir=, file_name=, extension=)
    path_dir, file = os.path.split(args.path_ground_truth)
    file_name, extension = os.path.splitext(file)

    df_truth = loader(path_dir=path_dir, file_name=file_name, extension=extension)
    
    # TODO: ind optional argment parser to input linked data
    print(df_truth)

if __name__ == '__main__':
    parser = ArgumentParser(description='Testing mineral site linking result')
    parser.add_argument('--path_ground_truth', '-p',
                        help='path in which the ground truth labeled data file is saved')
    parser.add_argument('--column_name', '-c',
                        help='column name (header) in the ground truth labeled data file, where the linking result is listed')
    
    main(parser.parse_args())