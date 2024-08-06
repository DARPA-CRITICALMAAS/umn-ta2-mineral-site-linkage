import configparser
import polars as pl
import itertools
import pickle
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    api_key=os.getenv("OPENAI_API_KEY")
)

def match_gpt(entity_A:str, entity_B:str, prompt:str):
    input_prompt = f"A is {entity_A}. B is {entity_B}. {prompt}."

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": input_prompt}
        ]
    )

    return str(completion.choices[0].message.content)

def run_on_gpt(list_entities:list, list_grouping:list, prompt:str):
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

        matches.append(match_gpt(entity_A, entity_B, prompt))
        time_calculation[1].append(time.time()-start_time)

        with open('./groundtruth_gpt.pkl', 'wb') as handle:
            pickle.dump(ground_truth, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./matches_gpt.pkl', 'wb') as handle:
            pickle.dump(matches, handle, protocol=pickle.HIGHEST_PROTOCOL)

        with open('./elapsed_time_gpt.pkl', 'wb') as handle:
            pickle.dump(time_calculation, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return matches, ground_truth
