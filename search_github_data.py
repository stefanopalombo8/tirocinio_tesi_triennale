import pandas as pd
import json
import time
from github import Github, Auth
from concurrent.futures import ThreadPoolExecutor


def load_and_prepare_df(dataset_path: str, separator: str) -> pd.DataFrame:
    # carico il dataset
    df = pd.read_csv(dataset_path, sep=separator)

    # snellisco il dataset
    df = df.dropna(subset=['tags'])  # Rimuove righe con 'tags' nulli
    df = df[df['downloads'] > 0]    # Filtra righe con 'downloads' uguale a 0

    # calcolo e filtro per la mediana
    download_median = df['downloads'].median()
    df = df[df['downloads'] >= download_median]

    return df

# mostra il rate limit per le richieste API
def print_rate_limit(tokens: list) -> None:

    for token in tokens:
        auth = Auth.Token(token)
        github_instance = Github(auth=auth, per_page=100)
        rate_limit = github_instance.get_rate_limit()

        print(f"Limit: {rate_limit.core.limit}")
        print(f"Remaining: {rate_limit.core.remaining}")
        print(f"Reset: {rate_limit.core.reset}")

# funzione banale di test dei limiti
def search_limit(token: str, queries: list, result: list) -> None:
    auth = Auth.Token(token)
    github_instance = Github(auth=auth, per_page=100)

    for query in queries:
        print(f"TOKEN: {token}, QUERY: {query}")

        try:
            files_founded = github_instance.search_code(query=query)
        except Exception as e:
            print(f"Errore durante la ricerca per la query: {query}. Errore: {e}")
            result.append(query)  # Tieni traccia delle query che causano errori e rimuovile
            continue

        # Se nessun file è stato trovato, aggiungi la query ai risultati da rimuovere
        if files_founded.totalCount <= 0:
            print(f"File non trovati per la query: {query}")
            result.append(query)
            
        time.sleep(1)


# funzione di ricerca 
def search_code_files(token: str, queries: list, dict_model_tags: dict, dict_model_repositories: dict) -> None:

    auth = Auth.Token(token)
    github_instance = Github(auth=auth, per_page=100)

    for query in queries:
        files_founded = github_instance.search_code(query=query)

        if files_founded.totalCount <= 0:
            print(f"File non trovati per la query: {query}")
            continue
        
        model = query.replace(" language:Python", "")
        dict_repository_files = dict_model_repositories[model] # dizionario interno del principale

        for file in files_founded:
            repository_name = file.repository.full_name
            readme = None

            try:
                readme = file.repository.get_readme()
            except:
                print(f"Errore nel recuperare il readme per {repository_name}")

            # controllo per vedere se il repository è già stato processato
            if repository_name not in dict_repository_files:
                try:
                    # popolamento del dizionario
                    dict_repository_files[repository_name] = {
                        "topics": file.repository.get_topics(),
                        "description": file.repository.description,
                        "readme": None,
                        "files": []
                    }
                    
                    # solo se il readme non è none
                    if readme: 
                        readme_content = readme.decoded_content.decode()
                        tag_to_search = dict_model_tags[model]

                        if tag_to_search in readme_content:
                            dict_repository_files[repository_name]['readme'] = readme.url
                        else:
                            readme_error = "README does not include the tag string"
                            dict_repository_files[repository_name]['readme'] = readme_error
                    else:
                        dict_repository_files[repository_name]['readme'] = "README not found"
                        
                except Exception as e:
                    print(f"Errore nel recuperare informazioni per {repository_name}: {e}")
                    continue 
            
            # aggiungo i file nella repository
            dict_repository_files[repository_name]["files"].append({
                "file_name": file.name,
                "file_url": file.html_url,
            })

        print(f"Attesa di 10 secondi prima della prossima query")
        time.sleep(10)
    
    rate_limit = github_instance.get_rate_limit()

    if rate_limit.core.remaining == rate_limit.core.limit:
        github_instance.close()


# Funzione per avviare ricerche parallele con più token
def search_with_multiple_tokens(tokens: list, queries: list, dict_model_tags: dict, dict_model_repositories: dict) -> None:
    queries_per_token = len(queries) // len(tokens)

    with ThreadPoolExecutor(max_workers=len(tokens)) as executor:
        futures = []
        for i, token in enumerate(tokens):
            # Distribuisci le query a ogni token
            token_queries = queries[i * queries_per_token: (i + 1) * queries_per_token]
            futures.append(executor.submit(search_code_files, token, token_queries, dict_model_tags, dict_model_repositories))

        # Attende che tutti i thread terminino
        for future in futures:
            future.result()

# salvataggio dei dati su un file json
def add_json_data(json_file: str, new_data: dict) -> None:
    try:
        with open(json_file, 'r') as file:
            current_data = json.load(file)
    except FileNotFoundError:
        # dizionario vuoto
        current_data = {}

    new_data = current_data | new_data

    with open(json_file, 'w') as file:
        json.dump(new_data, file, indent=4)


df = load_and_prepare_df(dataset_path='./hf_data/ranked_by_downloads_june.csv', separator=',')

# creazione lista contenente i nomi dei modelli espressi chiaramente
models_name = df['model_name'].str.replace('models/', '', regex=False).tolist()

# mapping modello-tag specifico (servirà per cercare nel readme)
dict_model_tags = dict(zip(models_name, df['tags']))

# dizionario di output della ricerca
dict_model_repositories = {nome: {} for nome in models_name[:3]}

# query example: MIT/ast-finetuned-audioset-10-10-0.4593 language:Python
language_filter = " language:Python"
github_queries = [name + language_filter for name in models_name[:3]]

# token personali
tokens_list = ["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", 
               "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", 
               "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", 
               "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",
               "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",
               "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",
               "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",
               "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",
               "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",
               "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"]


# MULTI TOKEN
#search_with_multiple_tokens(tokens=tokens_list[:3], queries=github_queries, dict_model_tags=dict_model_tags, dict_model_repositories=dict_model_repositories)

# SINGLE TOKEN
#search_code_files(token=tokens_list[9], queries=github_queries, dict_model_tags=dict_model_tags, dict_model_repositories=dict_model_repositories)

print_rate_limit(tokens=tokens_list)

file = 'code_files.json'

add_json_data(json_file=file, new_data=dict_model_repositories)