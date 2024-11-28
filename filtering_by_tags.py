import pandas as pd
import json 

# carico il dataset
df = pd.read_csv('./hf_data/ranked_by_downloads_june.csv', sep=',')

df = df.dropna(subset=['tags']) # rimuove righe con 'tags' nulli
df = df[df['downloads'] > 0]  # filtro per download > 0

# prendo il 10%
df = df.iloc[:int(len(df) * 0.1)]

# tag da considerare
tags = ["text-generation", 
        "text-to-image", 
        "image-classification", 
        "text-classification", 
        "text2text-generation",
        "fill-mask",
        "sentence-similarity",
        "question-answering", 
        "summarization", 
        "zero-shot-image-classification", 
        "image-to-text", 
        "object-detection", 
        "image-segmentation"]

# crea un'espressione per cercare i tag
pattern = '|'.join(tags)

# filtra le righe che contengono precisamente uno dei tag nella lista
filtered_df = df[df['tags'].str.match(pattern)]

print(f"dim dataset filtrato per tag {len(filtered_df)}")

# rimuovo 'models/' per avere i nomi dei modelli
df_models_name = filtered_df['model_name'].str.replace('models/', '', regex=False).tolist()

# prendo il risultato del mining
with open('search_code_files.json', 'r') as file:
    dict_model_repos = json.load(file)

# dizionario finale senza 0 risultati e filtrato per tag scelti
dict_model_repos_filtered = {key: value for key, value in dict_model_repos.items() 
              if "0 repositories founded" not in value 
              and key in df_models_name} 

print(f"dim nuovo dizionario {len(dict_model_repos_filtered)}")

# salvo il nuovo dizionario
with open('search_code_files_final.json', 'w') as file:
    json.dump(dict_model_repos_filtered, file, indent=4)

# tag - modelli
dict_tag_models = {tag : list() for tag in tags}

for model in dict_model_repos_filtered.keys():
    model_tag = filtered_df.loc[filtered_df['model_name'] == "models/"+model]['tags'].values[0]

    if model_tag in tags:
        dict_tag_models[model_tag].append(model)

# tag - numero di file
dict_tag_num_of_files = {
    tag: sum(len(dict_model_repos_filtered[model][repo]["files"])
            for model in models_list
            for repo in dict_model_repos_filtered[model])
            for tag, models_list in dict_tag_models.items()
}

print(f"numero totale dei file {sum(dict_tag_num_of_files.values())}")

# salvo
with open("tag-num_files.json", "w") as file:
    json.dump(dict_tag_num_of_files, file, indent=4)

# modello - tag che mi servirà per inserire nel db mongodb
dict_model_tag = {}

for tag, models in dict_tag_models.items():
    for model in models:
        dict_model_tag[model] = tag

# salvo
with open("model-tag.json", "w") as file:
    json.dump(dict_model_tag, file, indent=4)