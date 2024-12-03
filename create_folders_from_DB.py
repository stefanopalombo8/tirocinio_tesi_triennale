import os
import json
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor
from search_files_url import TAGS
from search_files_content import create_or_load_db

# Da fare solo la prima volta
def create_index(collection):
    # Creazione dell'indice sulla colonna in input
    collection.create_index("model_id")

    # Visualizza gli indici della collezione
    print(collection.index_information())

# Salvare i file per ogni modello
def process_model(model, files_collection):
    model_id = model["_id"]
    model_name = model["model_name"]

    # Perché le cartelle non possono contenere slash
    sanitized_model_name = model_name.replace("/", "_")

    # Cartella principale che contiene tutte le cartelle dei modelli
    main_storage_dir = os.path.join(r"C:\Users\fafup\Desktop", "models_storage")
    os.makedirs(main_storage_dir, exist_ok=True) # Così non la ricrea

    # Creazione di una cartella per ogni modello all'interno della cartella principale
    model_dir = os.path.join(main_storage_dir, sanitized_model_name)
    os.makedirs(model_dir, exist_ok=True)

    # Trovo tutti i file associati a quel modello
    associated_files = list(files_collection.find({"model_id": model_id}))

    print(f"Sto creando i file per il modello {model_name} nella directory {model_dir}")
    print(f"Numero totale di file associati: {len(associated_files)}")

    # index è il numero che metto nel nome di ogni file
    for index, file_doc in enumerate(associated_files, start=1):
        content = file_doc["content"]
        if content:
            file_name = f"file{index}_{model_id}.py"

            file_path = os.path.join(model_dir, file_name)

            # Salvo il content nel file
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

# Funzione per avviare in parallelo il salvataggio
def create_folders_parallel(models_collection):
    all_models = list(models_collection.find())
    with ThreadPoolExecutor(max_workers=4) as executor: # 4 in base ai processori logici del mio pc
        executor.map(process_model, all_models)


# Contare per ogni tag il numero di file (per descrizione online latex)
def count_files_by_tag(models_collection, files_collection):

    tag_file_count = {tag: 0 for tag in TAGS}

    try:
        for tag in TAGS:
            # Trova tutti i modelli con il tag corrente
            models_with_tag = models_collection.find({"tag": tag})

            for model in models_with_tag:
                model_id = model["_id"]

                # Conta i file associati al modello con content diverso da None
                count = files_collection.count_documents({"model_id": model_id, "content": {"$ne": None}})
                tag_file_count[tag] += count

        with open("file_json/tag-num_files_final.json", "w") as file:
            json.dump(tag_file_count, file, indent=4)

    except Exception as e:
        print(f"Errore durante il calcolo dei file per tag: {e}")

if __name__ == "__main__":
    model_collection, files_collection = create_or_load_db()

    #create_index(collection=files_collection)

    #create_folders_parallel()

    count_files_by_tag(models_collection=model_collection, files_collection=files_collection)