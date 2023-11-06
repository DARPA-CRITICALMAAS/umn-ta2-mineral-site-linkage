import os
import torch
import requests
from bs4 import BeautifulSoup

from transformers import LineByLineTextDataset, DataCollatorForLanguageModeling, Trainer, TrainingArguments

from utils.params import *
from utils.load_save import *

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def scrape_mines():
    """
    'scrape_mines' is a function that will scrape mine data that will be used to fine tune the BERT model

    : return: 
    """
    url_mines = []

    starting_point = 'https://en.wikipedia.org/wiki/List_of_iron_mines_in_the_United_States'
    response = requests.get (
        url = 'https://en.wikipedia.org/wiki/Jeebropilly_Mine',
    )

    soup = BeautifulSoup(response.content, 'html.parser')

    allLinks = soup.find(id="mw-content-text").find_all("table")
    print(allLinks)


    # allLinks = soup.find(id="bodyContent").find_all("a")

    # for link in allLinks:
    #     if link['href'].find("/wiki/") == -1:
    #         continue

    #     linkToScrape = link
    #     break

    # print(allLinks)
    
    # TODO: Save dataframe in format of textfile to the location DIR_TO_SRC + wiki.txt

    return 0

def scrape_mrds():
    return 0
    
def tune_bert(file_tune, path_save):
    tokenizer, model = model_load('bert-base-uncased')
    file_tune = os.path.join(DIR_TO_SRC, 'wiki.txt')

    dataset = LineByLineTextDataset (
        tokenizer = tokenizer,
        file_path = file_tune,
        block_size = 512,
    )

    data_collator = DataCollatorForLanguageModeling (
        tokenizer = tokenizer,
        mlm = True,
        mlm_probability = 0.2
    )

    training_args = TrainingArguments (
        output_dir = DIR_TO_SRC,
        overwrite_output_dire = True,
        num_train_epochs = 25,
        per_device_train_batch_size = 48,
        save_steps = 500,
        save_total_limit = 2,
        seed = 1,
    )

    trainer = Trainer (
        model = model,
        args = training_args,
        data_collator = data_collator,
        train_dataset = dataset,
    )

    trainer.train()
    trainer.save_model(DIR_TO_SRC)