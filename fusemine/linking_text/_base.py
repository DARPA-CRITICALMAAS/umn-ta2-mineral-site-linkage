import torch
from torchmetrics.functional.pairwise import pairwise_cosine_similarity

class BaseTextLinker:
    def calc_cosine_sim(fusemine_model,
                        threshold: float=0.9,) -> None:
        pass