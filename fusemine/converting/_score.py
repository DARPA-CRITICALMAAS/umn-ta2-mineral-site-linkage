from typing import List

import numpy as np

def oned2twod(oneD_list: List[float],):
    """
    TODO: fill information

    Arguments:

    """
    oneD_list = np.array([oneD_list])
    otherD_list = 1 - oneD_list

    twoD_list = np.hstack((otherD_list.T, oneD_list.T))

    return twoD_list.tolist()

def combined_cosine_scoring(list_all_cosine_scores:List[List[float]]) -> float:
    """
    TODO fille information:

    Argumet
    """
    confidence_0 = [item[0] for item in list_all_cosine_scores if item[0] > 0.99]

    if len(confidence_0) > 0:
        return 1.0
    
    list_mean = np.mean(list_all_cosine_scores, axis=0).tolist()

    return list_mean[0]

def determine_label(input_scores:List[List[List[float]]]|List[List[float]], 
                    text_method:str = 'combined') -> List[int]:
    """
    TODO: fill information

    Arguments:
    
    """
    print(input_scores.shape)
    input_scores = np.array(input_scores)

    # print(input_scores.shape)

    # mean0 = np.mean(input_scores, axis=0)
    # mean1 = np.mean(input_scores, axis=1)
    # print(mean0.shape)
    # print(mean1.shape)
    # mean2 = np.mean(input_scores, axis=2)

    # if text_method == 'combined':
    #     input_scores = np.sum(input_scores, axis=1)

    # return np.argmax(input_scores, axis=1)
    return 0