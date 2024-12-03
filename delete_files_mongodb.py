import json
from search_files_content import create_or_load_db

def delete_models_and_files(models_to_delete: list, models_collection, files_collection):
    for model_name in models_to_delete:
        try:
            model = models_collection.find_one({"model_name": model_name})
            if model:
                model_id = model["_id"]
                
                # elimino tutti i file associati al modello
                result_files = files_collection.delete_many({"model_id": model_id})
                print(f"Eliminati {result_files.deleted_count} file associati al modello {model_name}")
                
                # elimino il modello
                result_model = models_collection.delete_one({"_id": model_id})
                if result_model.deleted_count > 0:
                    print(f"Modello {model_name} eliminato con successo")
                else:
                    print(f"Errore nell'eliminazione del modello {model_name}")
            else:
                print(f"Modello {model_name} non trovato")
        except Exception as e:
            print(f"Errore nell'eliminazione del modello {model_name}: {e}")

if __name__ == "__main__":
    
    # mi connetto al db
    models_collection, files_collection = create_or_load_db()

    # carico il risultato di un mining parziale per capire quali modelli eliminare per poi riaggiungerli
    with open('file_json/search_code_files_new.json', 'r') as f:
        dict_model_files = json.load(f)

    # lista dei modelli da eliminare 
    models_to_delete = list(dict_model_files.keys())

    delete_models_and_files(models_to_delete=models_to_delete, models_collection=models_collection, files_collection=files_collection)