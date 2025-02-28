import numpy as np
import polars as pl
from typing import List

import torch
from scipy.special import softmax
from torchmetrics.functional.pairwise import pairwise_cosine_similarity
from sklearn.metrics.pairwise import paired_cosine_distances
from datasets import Dataset, DatasetDict
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer

device = 'cuda' if torch.cuda.is_available() else 'cpu'
tokenizer = AutoTokenizer.from_pretrained('roberta-base')

# Classification Version
def text_pair_classification(pl_data:pl.DataFrame,
                             dir_model:str,
                             id2label:dict) -> pl.DataFrame:
    """

    '0' = Yes
    '1' = No
    """
    # Convert data to Dataset dictionary
    tokenized_data = convert_to_datadict(pl_data=pl_data, id2label=id2label)

    # Load model
    trained_model = AutoModelForSequenceClassification.from_pretrained(
        f'{dir_model}', num_labels=2
    )
    trainer = Trainer(trained_model)

    with torch.no_grad():
        # https://stackoverflow.com/questions/63076190/how-to-calculate-the-memory-requirement-of-bert
        # TODO: test if this reduces memory requirement

        # predictions, _, _ = trainer.predict(test_dataset=tokenized_data['test'])
        predictions, _, _ = trainer.predict(test_dataset=tokenized_data['test'])
        # predicted_label = np.argmax(predictions, axis=1)
        confidence = softmax(predictions, axis=1)

    pl_data = pl_data.with_columns(
        # link_text_result = pl.Series(predicted_label),
        classification_confidence = confidence
    ).with_columns(
        pl.struct(pl.col('classification_confidence')).map_elements(lambda x: x['classification_confidence'][0])
    )       # TODO: check if working properly

    return pl_data

def convert_to_datadict(pl_data:pl.DataFrame, 
                        id2label:dict):
    
    dataset_dictionary = DatasetDict()

    pd_data = pl_data.to_pandas()
    dataset_dictionary['test'] = Dataset.from_pandas(pd_data)

    # TODO: preserve the two URIs
    tokenized_data = dataset_dictionary.map(tokenize_function, batched=True)

    return tokenized_data

def tokenize_function(dict_input:dict):
    return tokenizer(dict_input['text'], padding='max_length', truncation=True, max_length=512)

# Cosine Version
def text_embedding_cosine(list_embedding1: List[List[float]],
                          list_embedding2: List[List[float]]=[]) -> List[List[float]]:
    if len(list_embedding2) == 0:
        with torch.no_grad():
            similarity_score = pairwise_cosine_similarity(list_embedding1).numpy(force=True)
        similarity_score = np.triu(similarity_score)

        return similarity_score
    else:
        with torch.no_grad():
            similarity_score = paired_cosine_distances(list_embedding1, list_embedding2)
        
        return similarity_score