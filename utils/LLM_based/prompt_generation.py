def prompting_framework(entity_object:str, bool_simple:bool) -> str:
    prompt_string = f'Do the two {entity_object} descriptions '

    if bool_simple:
        prompt_string += 'match?'
    if not bool_simple:
        prompt_string += f'refer to the same real-world {entity_object}?'

    return prompt_string

def question_framework(bool_free:bool) -> str:
    question_string = ""

    if not bool_free:
        question_string += "Answer with 'Yes' if they do and 'No' if they do not. Answer only in Yes or No."

    return question_string

def design_prompt(bool_simple:bool, bool_free:bool, entity_object='entity'):
    prompt = f'{prompting_framework(entity_object, bool_simple)} {question_framework(bool_free)}'

    return prompt