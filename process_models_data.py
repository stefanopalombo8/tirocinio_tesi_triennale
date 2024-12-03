import pandas as pd
import json 
from search_files_url import TAGS, load_and_prepare_df

# serve per rimuovere dal file minato i 0 risultati
def filter_code_files(file_name: str) -> dict:
    # prendo il risultato del mining iniziale
    with open(file_name, 'r') as file:
        dict_model_repos = json.load(file)

    # dizionario finale senza 0 risultati (0 repository trovare)
    dict_model_repos_filtered = {key: value for key, value in dict_model_repos.items() if "0 repositories founded" not in value } 

    print(f"dim nuovo dizionario {len(dict_model_repos_filtered)}")

    # salvo il nuovo dizionario
    with open(file_name + '_final' + '.json', 'w') as file:
        json.dump(dict_model_repos_filtered, file, indent=4)
    
    return dict_model_repos_filtered

# carica il dizionario finale
def get_final_dict_model_repos(file_name: str) -> dict:
    with open(file_name, 'r') as file:
        dict_model_repos = json.load(file)
    
    return dict_model_repos

# serve come input per le funzioni successive 
def map_tag_models(df: pd.DataFrame, dict_model_repo: dict) -> dict:
    dict_tag_models = {tag : list() for tag in TAGS}

    for model in dict_model_repo.keys():
        model_tag = df.loc[df['model_name'] == "models/"+model]['tags'].values[0]

        if model_tag in TAGS:
            dict_tag_models[model_tag].append(model)
    
    return dict_tag_models

# per ogni tag conto i file, per vederne la distribuzione
def count_and_save_files_per_tag(dict_tag_models: dict, dict_model_repo: dict) -> dict:

    # tag - numero di file
    dict_tag_num_files = {
        tag: sum(len(dict_model_repo[model][repo]["files"])
                for model in models_list
                for repo in dict_model_repo[model])
                for tag, models_list in dict_tag_models.items()
    }

    print(f"numero totale dei file {sum(dict_tag_num_files.values())}")

    # salvo il nuovo dizionario
    with open('file_json/tag-num_files.json', 'w') as file:
        json.dump(dict_tag_num_files, file, indent=4)

# dizionario model - tag, l'output serve verrà inserito del db
def map_and_save_model_tag(dict_tag_models: dict) -> None:
    # modello - tag che mi servirà per inserire nel db mongodb
    dict_model_tag = {}

    for tag, models in dict_tag_models.items():
        for model in models:
            dict_model_tag[model] = tag

    # salvo
    with open("file_json/model-tag.json", "w") as file:
        json.dump(dict_model_tag, file, indent=4)

def count_and_save_files_per_model(dict_model_repos: dict):

    # calcolo
    dict_model_num_files = {
        model: sum(len(repo_details["files"]) for repo_details in repos.values())
        for model, repos in dict_model_repos.items()
    }

    # salvo
    with open('file_json/model-num_files.json', 'w') as file:
        json.dump(dict_model_num_files, file, indent=4)

def merge_two_searchs(final_file: str, new_file: str) -> dict:
    with open(final_file) as f:
        final_dict = json.load(f)
    
    with open(new_file) as f:
        new_dict = json.load(f)
    
    merged_dict = final_dict | new_dict

    # salvo sovrascrivendo il nuovo
    with open(final_file) as f:
        json.dump(merged_dict, final_file, indent=4)
    
    return merged_dict


if __name__ == "__main__":

    # carico e filtro il dataset
    filtered_df = load_and_prepare_df(dataset_path='./hf_data/ranked_by_downloads_june.csv', separator=',', tags=TAGS)

    # da fare solo la prima volta, una volta filtrato importo direttamente quello filtrato
    #new_dict_model_repo = filter_code_files(file_name='search_code_files.json')

    # da fare solo se si hanno nuovi file ricercati
    #new_dict_model_repo = merge_two_searchs(final_file='file_json/search_code_files_final.json', 
    #                                        new_file='file_json/search_code_files_new.json')
    

    # importo direttamente il dizionario finale
    new_dict_model_repo = get_final_dict_model_repos(file_name='file_json/search_code_files_final.json')

    # input delle altre due funzioni
    dict_tag_models = map_tag_models(df=filtered_df, dict_model_repo=new_dict_model_repo)

    # conto e salvo i file per ogni tag
    count_and_save_files_per_tag(dict_tag_models=dict_tag_models, dict_model_repo=new_dict_model_repo)

    # conto e salvo i file per ogni modello
    count_and_save_files_per_model(dict_model_repos=new_dict_model_repo)

    # mappo e salvo su sule JSON
    map_and_save_model_tag(dict_tag_models=dict_tag_models)