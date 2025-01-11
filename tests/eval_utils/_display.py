from typing import Dict, List

from tabulate import tabulate
from matplotlib import pyplot as plt

def display_table(dict_data: Dict[str, float]) -> None:
    headers = list(dict_data.keys())
    table = list(dict_data.values())

    print(tabulate(table, headers=headers, tablefmt="presto", floatfmt=".4f"))

def plotting(fusemine_model):
    return 0