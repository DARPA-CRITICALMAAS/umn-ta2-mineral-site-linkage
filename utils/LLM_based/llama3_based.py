# Link to huggingface library: https://huggingface.co/meta-llama/Meta-Llama-3-8B-Instruct

import transformers
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

pipeline = transformers.pipeline(
    "text-generation",
    model='/home/yaoyi/pyo00005/Entity_Matching/model/llama3_models/8B-Instruct',
    batch_size=8, 
    model_kwargs={"torch_dtype": torch.bfloat16},
    device_map="auto",
)

def match_llama3(entity_A:str, entity_B:str, prompt:str):
    input_prompt = f"A is {entity_A}. B is {entity_B}. {prompt}."
    messages = [
        {"role": "user", "content": input_prompt},
    ]

    terminators = [
        pipeline.tokenizer.eos_token_id,
        pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>")
    ]

    outputs = pipeline(
        messages,
        max_new_tokens=256,
        eos_token_id=terminators,
    )

    return outputs[0]["generated_text"][-1]['content']

def match_llama3_batch(entity_A_batch:list, entity_B_batch:list, prompt:str):
    input_prompt = f'A is {entity_A_batch[0]}. '
    prompt = ''
    numeric_alpha = ord('A')

    for i in range(len(entity_A_batch)):
        alphabet = chr(numeric_alpha + i + 1)
        input_prompt += f'{alphabet} is {entity_B_batch[i]}. '

        if i == 0:
            continue
    
    input_prompt += f"\nWhich of the description refer to the same real-world mine as A?"

    print(input_prompt)

    messages = [
        {"role": "user", "content": input_prompt},
    ]

    print('here')

    terminators = [
        pipeline.tokenizer.eos_token_id,
        pipeline.tokenizer.convert_tokens_to_ids("<|eot_id|>")
    ]

    print('here')

    outputs = pipeline(
        messages,
        max_new_tokens=256,
        eos_token_id=terminators,
    )

    print(outputs[0]["generated_text"][-1]['content'])

    return outputs[0]["generated_text"][-1]['content']

def run_on_llama(list_entities:list, list_grouping:list, prompt:str):
    indexes = list(range(len(list_entities)))
    entity_tuples = list(itertools.combinations(indexes, 2))

    matches = []
    ground_truth = []
    pairs = []
    time_calculation = [[0], [0]]
    start_time = time.time() 

    for idx, tuple_pair in enumerate(entity_tuples):
        ### Batch style
        # if tuple_pair[0] != start_key:
        #     match_llama3_batch(entity_A_batch, entity_B_batch, prompt)

        #     entity_A_batch = []
        #     entity_B_batch = []
        #     start_key = tuple_pair[0]

        # entity_A_batch.append(list_entities[tuple_pair[0]])
        # entity_B_batch.append(list_entities[tuple_pair[1]])

        time_calculation[0].append(idx+1)

        if list_grouping[tuple_pair[0]] == list_grouping[tuple_pair[1]]:
            ground_truth.append('Yes')
        else:
            ground_truth.append('No')

        matches.append(match_llama3(list_entities[tuple_pair[0]], list_entities[tuple_pair[1]], prompt))

        tuple_pair = (tuple_pair[0], tuple_pair[1])
        pairs.append(tuple_pair)

    # print(matches)
    # print(ground_truth)
    #     matches.append(match_llama3(entity_A, entity_B, prompt))
        time_calculation[1].append(time.time()-start_time)

        with open('./groundtruth_llama.pkl', 'wb') as handle:
            pickle.dump(ground_truth, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./matches_llama.pkl', 'wb') as handle:
            pickle.dump(matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./pairs_llama.pkl', 'wb') as handle:
            pickle.dump(pairs, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./elapsed_time_llama.pkl', 'wb') as handle:
            pickle.dump(time_calculation, handle, protocol=pickle.HIGHEST_PROTOCOL)

def run_on_llama2(list_entities:list, prompt:str):
    indexes = list(range(len(list_entities)))
    entity_tuples = list(itertools.combinations(indexes, 2))
    entity_tuples = entity_tuples[750488:]

    with open('./matches_llama_copper.pkl', 'rb') as handle:
        matches = pickle.load(handle)

    with open('./pairs_llama_copper.pkl', 'rb') as handle:
        pairs = pickle.load(handle)

    time_calculation = [[0], [0]]
    start_time = time.time() 

    for idx, tuple_pair in enumerate(entity_tuples):
        time_calculation[0].append(idx+1)

        matches.append(match_llama3(list_entities[tuple_pair[0]], list_entities[tuple_pair[1]], prompt))
        
        tuple_pair = (tuple_pair[0], tuple_pair[1])
        pairs.append(tuple_pair)

        time_calculation[1].append(time.time()-start_time)

        with open('./matches_llama_copper.pkl', 'wb') as handle:
            pickle.dump(matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./pairs_llama_copper.pkl', 'wb') as handle:
            pickle.dump(pairs, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # return matches, ground_truth
        