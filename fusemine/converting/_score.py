from Typing import List

import numpy as np

def oned2twod(oneD_list: List[float],):
    # TODO: check
    oneD_list = np.transpose(np.array(oneD_list))
    otherD_list = np.transpose(np.array(1 - oneD_list))

    return np.hstack(oneD_list, otherD_list).tolist()

def determine_label(input_scores:List[List[List[float]]]|List[List[float]], 
                    text_method:str = 'combined') -> List[int]:
    input_scores = np.array(input_scores)

    if text_method == 'combined':
        input_scores = np.sum(input_scores, axis=1)

    return np.argmax(input_scores, axis=1)