import json

# carico il risultato del mining
with open('search_code_files_final.json', 'r') as file:
    data = json.load(file)

# calcolo
dict_model_num_files = {
    model: sum(len(repo_details["files"]) for repo_details in repos.values())
    for model, repos in data.items()
}

# salvo
with open('model-num_files.json', 'w') as file:
    json.dump(dict_model_num_files, file, indent=4)