import pandas as pd
import json
import time
from github import Github, Auth, GithubException
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta


def load_and_prepare_df(dataset_path: str, separator: str) -> pd.DataFrame:
    # carico il dataset
    df = pd.read_csv(dataset_path, sep=separator)

    # snellisco il dataset
    df = df.dropna(subset=['tags'])  # Rimuove righe con 'tags' nulli
    df = df[df['downloads'] > 0]    # Filtra righe con 'downloads' uguale a 0

    # prendo il 10%
    df = df.iloc[:int(len(df) * 0.1)]

    return df

# funzione per togliere le query che sono già state elaborate
def preprocessing_queries(models: list) -> list:
    with open('search_code_files.json', 'r') as file:
        data = json.load(file)

    # Estraggo i modelli che ho già processato
    models_with_files = [key for key, value in data.items() if len(dict(value)) != 0]

    # Li tolgo da quelli che devo ancora minare
    models = [item for item in models if item not in set(models_with_files)]

    return models

# mostra il rate limit per le richieste API
def print_rate_limit(tokens: list) -> None:

    for token in tokens:
        auth = Auth.Token(token)
        github_instance = Github(auth=auth, per_page=100)
        rate_limit = github_instance.get_rate_limit()

        print(f"TOKEN: {token}")
        print(f"Limit: {rate_limit.core.limit}")
        print(f"Remaining: {rate_limit.core.remaining}")
        print(f"Reset: {rate_limit.core.reset}\n")


# funzione di ricerca 
def search_code_files(token: str, queries: list, dict_model_tags: dict, dict_model_repositories: dict, last_save_time: datetime) -> None:

    auth = Auth.Token(token)
    github_instance = Github(auth=auth, per_page=100)

    request_count = 0
    max_requests_per_minute = 8 # appena fa la decima fa backoff quindi da 0 a 8 sono 9

    for query in queries:
        if request_count >= max_requests_per_minute:
            print("Attesa di un minuto per rispettare il limite di 10 richieste.")
            time.sleep(63) # minuto
            request_count = 0  # Resetta il contatore dopo l'attesa

        try:
            files_founded = github_instance.search_code(query=query, language="Python")
            request_count += 1  # Incrementa il contatore delle richieste

            model = query.replace(" language:Python", "")
            dict_repository_files = dict_model_repositories[model] # dizionario interno del principale

            if files_founded.totalCount <= 0:
                # dizionario vuoto quando non ci sono risultati
                dict_repository_files["0 repositories founded"] = dict()
                continue # si continua con la prossima query
            
            # provo scorro i primi 2000 file (la paginazione è aumomaticamente gestita) ma non supera i 1000 totali
            for file in files_founded[:2000]:
                try:
                    # prendo il nome, è la chiave del dizionario interno
                    repository_name = file.repository.full_name
                    readme = None

                    try:
                        readme = file.repository.get_readme()
                    except GithubException as e:
                        if e.status == 403:
                            print(f"Errore di rate limit per {repository_name}")
                            raise  # Rilancia l'eccezione per gestirla a livello superiore
                        
                        # per lo più errore 404
                        print(f"Errore nel recuperare il readme per {repository_name}: {e}")

                    # controllo per vedere se il repository è già stato processato
                    if repository_name not in dict_repository_files:
                        
                        # popolamento del dizionario
                        dict_repository_files[repository_name] = {
                            "topics": file.repository.get_topics(),
                            "description": file.repository.description,
                            "readme": None,
                            "numbers of commits:": file.repository.get_commits().totalCount,
                            "number of pull requests": file.repository.get_pulls(state='all').totalCount,
                            "number of forks": file.repository.forks_count,
                            "number of stars": file.repository.stargazers_count,
                            "files": []
                        }

                        # solo se il readme non è None
                        if readme:
                            readme_content = readme.decoded_content.decode()
                            tag_to_search = dict_model_tags[model]

                            # cerco se c'è il tag dentro il contenuto del readme
                            if tag_to_search in readme_content:
                                dict_repository_files[repository_name]['readme'] = readme.url
                            else:
                                readme_error = "README does not include the tag string"
                                dict_repository_files[repository_name]['readme'] = readme_error
                        else:
                            dict_repository_files[repository_name]['readme'] = "README not found"

                    # aggiungo i file nella repository
                    dict_repository_files[repository_name]["files"].append({
                        "file_name": file.name,
                        "file_url": file.html_url,
                    })

                    print(f"{token} attesa di mezzo secondo prima della prossima query")
                    time.sleep(0.5)

                    current_time = datetime.now()
                    # vedo se è il momennto di salvare
                    if current_time - last_save_time >= timedelta(minutes=30):
                        print("Salvataggio dei dati dopo 30 minuti.")
                        add_json_data(json_file='search_code_files.json', new_data=dict_model_repositories)
                        last_save_time = current_time  # Aggiorna il timestamp dell'ultimo salvataggio

                except GithubException as e:
                    # se ho incontrato 
                    if e.status == 403:
                        print("LIMITE DI 5000 - SALVO")
                        add_json_data(json_file='search_code_files.json', new_data=dict_model_repositories)
                        print("ATTESA DI 1H")
                        time.sleep(3605)  # ora
                    continue
                

        except Exception as e:
            print(f"Errore: {e}")


# Funzione per avviare ricerche parallele con più token
def search_with_multiple_tokens(tokens: list, queries: list, dict_model_tags: dict, dict_model_repositories: dict, last_save_time: datetime) -> None:
    queries_per_token = len(queries) // len(tokens)

    with ThreadPoolExecutor(max_workers=len(tokens)) as executor:
        futures = []
        for i, token in enumerate(tokens):
            # Distribuisci le query a ogni token
            token_queries = queries[i * queries_per_token: (i + 1) * queries_per_token]
            futures.append(executor.submit(search_code_files, token, token_queries, dict_model_tags, dict_model_repositories, last_save_time))

        # Attende che tutti i thread terminino
        for future in futures:
            future.result()

# salvataggio dei dati su un file json
def add_json_data(json_file: str, new_data: dict) -> None:
    try:
        with open(json_file, 'r') as file:
            current_data = json.load(file)
    except FileNotFoundError:
        # dizionario vuoto all'inizio
        current_data = {}
    
    merged_dict = current_data | new_data

    with open(json_file, 'w') as file:
        json.dump(merged_dict, file, indent=4)


df = load_and_prepare_df(dataset_path='./hf_data/ranked_by_downloads_june.csv', separator=',')

# creazione lista contenente i nomi dei modelli espressi chiaramente
models_name = df['model_name'].str.replace('models/', '', regex=False).tolist()

# togliamo le query già fatte
models_name = preprocessing_queries(models_name) 

# mapping modello-tag specifico (servirà per cercare nel readme)
dict_model_tags = dict(zip(models_name, df['tags']))

# dizionario di output della ricerca
dict_model_repositories = {name: {} for name in models_name}

# token personali
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

#MULTI TOKEN
search_with_multiple_tokens(tokens=TOKENS_LIST, 
                            queries=models_name, 
                            dict_model_tags=dict_model_tags, 
                            dict_model_repositories=dict_model_repositories, 
                            last_save_time=datetime.now())

# SINGLE TOKEN
# search_code_files(token=TOKENS_LIST[0], queries=models_name, dict_model_tags=dict_model_tags, dict_model_repositories=dict_model_repositories)

print("SALVATAGGIO FINALE")
add_json_data(json_file='search_code_files.json', new_data=dict_model_repositories)