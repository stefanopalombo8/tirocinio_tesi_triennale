import json
import re
import time
from github import Github, Auth, GithubException
from pymongo import MongoClient


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

        if rate_limit.core.remaining <= 1:
            time.sleep(60) 

        # Ottieni il repository e il file specifico per commit o branch
        repo = g.get_repo(f"{owner}/{repo_name}")
        file_content = repo.get_contents(file_path, ref=commit_or_branch)

        # Decodifica il contenuto del file
        print("RITORNO IL CONTENT DECODED")
        return file_content.decoded_content.decode("utf-8")

    except Exception as e:
        print(f"Errore nel recupero del file: {e}")
        return None


def save_file_content_to_db(collection, document):
    try:
        collection.insert_one(document)
        print(f"Contenuto salvato")
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

auth = Auth.Token(token=TOKENS_LIST[1])
g = Github(auth=auth)

client = MongoClient(MONGODB_URI)

db = client["github_files"]
collection = db["files_content"]

with open('code_files_final.json', 'r') as file:
    dict_model_repos = json.load(file)

model_to_process = list(dict_model_repos.keys())[2:3]

print(model_to_process)

for model in model_to_process:
    document_to_save = {model:{}}

    dict_repo_files = dict_model_repos[model]
    dict_file_content = document_to_save[model]

    for repo in dict_repo_files.keys():
        for file in dict_repo_files[repo]["files"]:
            file_url = file["file_url"]
            #dict_file_content[file_url] = ""
            dict_file_content[file_url] = get_file_content_from_url(g=g, file_url=file_url)
    
    #print(f"numero file {len(dict_file_content)}")

    save_file_content_to_db(collection=collection, document=document_to_save)