# Link to huggingface library: https://huggingface.co/mistralai/Mistral-7B-Instruct-v0.3

from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
import configparser
import polars as pl
import itertools
from transformers.utils import logging
import pickle
import time

# model_id = "meta-llama/Meta-Llama-3-8B-Instruct"

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
logging.set_verbosity_error()

model = AutoModelForCausalLM.from_pretrained(
    "mistralai/Mistral-7B-Instruct-v0.3",
    cache_dir = '/home/yaoyi/pyo00005/Entity_Matching/model/mistral_models')
tokenizer = AutoTokenizer.from_pretrained("mistralai/Mistral-7B-Instruct-v0.3")

def match_mistral(entity_A:str, entity_B:str, prompt:str):
    input_prompt = f"A is {entity_A}. B is {entity_B}. {prompt}."
    messages = [
        {"role": "user", "content": input_prompt},
    ]

    encodeds = tokenizer.apply_chat_template(messages, return_tensors="pt")
    model_inputs = encodeds

    generated_ids = model.generate(model_inputs, max_new_tokens=1000, do_sample=True)
    decoded = tokenizer.batch_decode(generated_ids)[0]

    return decoded.split('[/INST]')[1].split('</s>')[0].strip()

def run_w_mistral(list_entities:list, list_grouping:list, prompt:str):
    indexes = list(range(len(list_entities)))
    entity_tuples = list(itertools.combinations(indexes, 2))

    matches = []
    ground_truth = []
    time_calculation = [[0], [0]]
    start_time = time.time()

    for idx, tuple_pair in enumerate(entity_tuples):
        entity_A = list_entities[tuple_pair[0]]
        entity_B = list_entities[tuple_pair[1]]

        time_calculation[0].append(idx+1)

        if list_grouping[tuple_pair[0]] == list_grouping[tuple_pair[1]]:
            ground_truth.append('Yes')
        else:
            ground_truth.append('No')

        matches.append(match_mistral(entity_A, entity_B, prompt))
        time_calculation[1].append(time.time()-start_time)

        with open('./groundtruth_mistral.pkl', 'wb') as handle:
            pickle.dump(ground_truth, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./matches_mistral.pkl', 'wb') as handle:
            pickle.dump(matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./elapsed_time_mistral.pkl', 'wb') as handle:
            pickle.dump(time_calculation, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return matches, ground_truth
        