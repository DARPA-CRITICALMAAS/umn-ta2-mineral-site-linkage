import numpy as np
import polars as pl

import torch
from datasets import Dataset, DatasetDict
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer

device = 'cuda' if torch.cuda.is_available() else 'cpu'
tokenizer = AutoTokenizer.from_pretrained('roberta-base')

def text_pair_classification(pl_data:pl.DataFrame,
                             dir_model:str,
                             id2label:dict) -> pl.DataFrame:
    """

    0: 'No'
    1: 'Yes'
    """
    # Convert data to Dataset dictionary
    tokenized_data = convert_to_datadict(pl_data=pl_data, id2label=id2label)

    # Load model
    trained_model = AutoModelForSequenceClassification.from_pretrained(
        f'{dir_model}', num_labels=2
    )
    trainer = Trainer(trained_model)

    predictions, label_ids, metrics = trainer.predict(test_dataset=tokenized_data['test'])
    predicted_label = np.argmax(predictions, axis=1)

    pl_data = pl_data.with_columns(
        link_text_result = pl.Series(predicted_label)
    )

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