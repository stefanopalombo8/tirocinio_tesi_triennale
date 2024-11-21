import json
import re
import time
import pytz
from datetime import datetime
from github import Github, Auth, GithubException
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor


def get_file_content_from_url(g, file_url:str):
    
    # pattern per URL con commit hash
    pattern = r"github.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)"
    match = re.search(pattern, file_url)
    if not match:
        print("URL non valido.")
        return None

    owner, repo_name, commit_or_branch, file_path = match.groups()
    
    try:
        rate_limit = g.get_rate_limit()

        # da migliorare per non perdere 1h
        if rate_limit.core.remaining <= 2:
            reset_time = rate_limit.core.reset
            reset_time_utc = reset_time.astimezone(pytz.utc)

            # Converte la data corrente in UTC
            now_utc = datetime.now(pytz.utc)

            # Calcola la differenza
            diff = reset_time_utc - now_utc
            seconds_to_sleep = diff.total_seconds() + 120 # ulteriore attesa di 2 minuti per sicurezza

            time.sleep(seconds_to_sleep) 

        # Ottieni il repository e il file specifico per commit o branch
        repo = g.get_repo(f"{owner}/{repo_name}")
        file_content = repo.get_contents(file_path, ref=commit_or_branch)

        # Decodifica il contenuto del file
        print("RITORNO IL CONTENT DECODED")
        return file_content.decoded_content.decode("utf-8")

    except Exception as e:
        print(f"Errore nel recupero del file: {e}")
        return None
    
def search_and_save(token: str, models_name: list, dict_model_repos: dict, dict_model_tag: dict):

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

                # Inserisci il file nella collezione `files` con riferimento al modello
                save_file_to_db(files_collection, file_url, file_content, model_id)



# Funzione per avviare ricerche parallele con più token
def search_with_multiple_tokens(tokens: list, queries: list, dict_model_repos: dict, dict_model_tag: dict) -> None:
    queries_per_token = len(queries) // len(tokens)

    with ThreadPoolExecutor(max_workers=len(tokens)) as executor:
        futures = []
        for i, token in enumerate(tokens):
            # Distribuisci le query a ogni token
            token_queries = queries[i * queries_per_token: (i + 1) * queries_per_token]
            futures.append(executor.submit(search_and_save, token, token_queries, dict_model_repos, dict_model_tag))

        # Attende che tutti i thread terminino
        for future in futures:
            future.result()

def save_model_to_db(collection, model_name, tag):
    model_document = {"model_name": model_name, "tag": tag}
    try:
        result = collection.insert_one(model_document)
        print(f"Modello salvato")
        return result.inserted_id
    except Exception as e:
        print(f"Errore nel salvataggio su MongoDB: {e}")

def save_file_to_db(collection, file_url, content, model_id):
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


MONGODB_URI = "mongodb://localhost:27017/"
TOKENS_LIST = ["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", 
               "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", 
               "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", 
               "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", 
               "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", 
               "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", 
               "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",
               "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",
               "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",
               "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"
               ]

client = MongoClient(MONGODB_URI)

db = client["github_files"]
models_collection = db["models"]
files_collection = db["files"]

with open('code_files_final.json', 'r') as file:
    dict_model_repos = json.load(file)

with open('model_tag.json', 'r') as file:
    dict_model_tag = json.load(file)

model_to_process = list(dict_model_repos.keys())[:2]

search_with_multiple_tokens(tokens=TOKENS_LIST[:2], 
                            queries=model_to_process, 
                            dict_model_repos=dict_model_repos,
                            dict_model_tag=dict_model_tag)