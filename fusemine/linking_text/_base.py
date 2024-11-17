import torch
from torchmetrics.functional.pairwise import pairwise_cosine_similarity

class BaseTextLinker:
    def __init__(self):
        pass

    def calc_cosine_sim(
        self,
        threshold: float=0.9,
    ):
        pass