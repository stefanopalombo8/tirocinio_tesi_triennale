import json
import re
import time
from github import Github, Auth
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from search_files_url import TOKENS, get_seconds_to_wait

def create_or_load_db() -> tuple:
    MONGODB_URI = "mongodb://localhost:27017/"
    client = MongoClient(MONGODB_URI)
    db = client["github_files"]
    models_collection = db["models"]
    files_collection = db["files"]

    return models_collection, files_collection

def get_file_content_from_url(g: Github, file_url:str):
    
    # pattern per URL con commit hash
    pattern = r"github.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)"
    match = re.search(pattern, file_url)
    if not match:
        print("URL non valido.")
        return None

    owner, repo_name, commit_or_branch, file_path = match.groups()
    
    try:
        rate_limit = g.get_rate_limit()

        if rate_limit.core.remaining <= 2:
            seconds_to_sleep = get_seconds_to_wait(g=g, lower_bound=2)

            time.sleep(seconds_to_sleep) 

        # ottieni il repository e il file specifico
        repo = g.get_repo(f"{owner}/{repo_name}")
        file_content = repo.get_contents(file_path, ref=commit_or_branch)

        # decodifica il contenuto del file
        print("RITORNO IL CONTENT DECODED")
        return file_content.decoded_content.decode("utf-8")

    except Exception as e:
        print(f"Errore nel recupero del file: {e}")
        return None
    
def search_and_save(token: str, models_name: list, dict_model_repos: dict, dict_model_tag: dict, models_collection, files_collection):

    auth = Auth.Token(token=token)
    g = Github(auth=auth)

    for model_name in models_name:

        dict_repo_files = dict_model_repos[model_name]
        model_tag = dict_model_tag[model_name]

        model_id = save_model_to_db(models_collection, model_name, model_tag)

        for _, repo_details in dict_repo_files.items():
            for file_info in repo_details["files"]:
                file_url = file_info["file_url"]
                file_content = get_file_content_from_url(g=g, file_url=file_url)

                # inserisci il file nella collezione `files` con riferimento al modello
                save_file_to_db(files_collection, file_url, file_content, model_id)


# ricerche parallele con più token
def search_with_multiple_tokens(tokens: list, queries: list, dict_model_repos: dict, dict_model_tag: dict, models_collection, files_collection) -> None:
    queries_per_token = len(queries) // len(tokens)

    with ThreadPoolExecutor(max_workers=len(tokens)) as executor:
        futures = []
        for i, token in enumerate(tokens):
            # ogni token si prende un po' di query
            token_queries = queries[i * queries_per_token: (i + 1) * queries_per_token]
            futures.append(executor.submit(search_and_save, token, token_queries, dict_model_repos, dict_model_tag, models_collection, files_collection))

        # attende che tutti i thread terminino
        for future in futures:
            future.result()

def save_model_to_db(collection, model_name : str, tag : str):
    model_document = {"model_name": model_name, "tag": tag}
    try:
        result = collection.insert_one(model_document)
        print(f"Modello salvato")
        return result.inserted_id
    except Exception as e:
        print(f"Errore nel salvataggio su MongoDB: {e}")

def save_file_to_db(collection, file_url : str, content : str, model_id : str):
    file_document = {
        "model_id": model_id,
        "file_url": file_url,
        "content": content
    }
    try:
        collection.insert_one(file_document)
        print(f"File salvato")
    except Exception as e:
        print(f"Errore nel salvataggio su MongoDB: {e}")

if __name__ == "__main__":

    models_collection, files_collection = create_or_load_db()

    # carico il file json finale con gli URL
    with open('file_json/search_code_files_final.json', 'r') as f:
        dict_model_repos = json.load(f)

    # carico il mapping tra modello e tag
    with open('file_json/model-tag.json', 'r') as file:
        dict_model_tag = json.load(file)

    models_to_process = list(dict_model_repos.keys())

    #print(len(models_to_process))

    search_with_multiple_tokens(tokens=TOKENS, 
                                queries=models_to_process, 
                                dict_model_repos=dict_model_repos,
                                dict_model_tag=dict_model_tag, 
                                models_collection=models_collection,
                                files_collection=files_collection)