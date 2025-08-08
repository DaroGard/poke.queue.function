import io
import os
import azure.functions as func
import datetime
import json
import logging
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
import random #Tarea 3

from dotenv import load_dotenv

app = func.FunctionApp()

load_dotenv()

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')
STORAGE_ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.queue_trigger(arg_name="azqueue"
                   , queue_name="requests"
                   , connection="QueueAzureWebJobsStorage"
) 
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request( id, "inprogress")
    request_info = get_request(id)

    #Tarea 3: Obtén el sample_size para el reporte que se está procesando
    sample_size = request_info[0].get("sample_size", None)
    pokemons = get_pokemons(request_info[0]["type"])
    #Tarea 3: Después de obtener la lista completa de resultados de la API externa PokeAPI, si se especificó un sample_size válido y menor al total de resultados, 
    # usa random.sample() para seleccionar aleatoriamente los registros.
    if sample_size and isinstance(sample_size, int) and 0 < sample_size < len(pokemons):
        pokemons = random.sample(pokemons, sample_size)
    #Tarea 3: Genera el CSV usando la lista resultante (completa o muestreada)
    pokemon_bytes = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name, csv_data=pokemon_bytes)
    logger.info(f"File {blob_name} was uploaded successfully")

    complete_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request( id, "completed", complete_url)

def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status
        , "id": id
    }
    if url:
        payload["url"] = url

    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    return response.json()

def get_request(id : int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()

def get_pokemons( type: str) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"

    try:
        response = requests.get(pokeapi_url, timeout=3000)
        data = response.json()
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching type '{type}' from {pokeapi_url}")
        return[]
    
    pokemon_entries = data.get("pokemon", [])
    base_list = [p["pokemon"] for p in pokemon_entries]
    enriched_list = []
    #Tarea 2: Después de obtener la lista inicial de Pokémon para el tipo seleccionado, deberás iterar sobre esa lista
    # Para cada Pokémon en la lista, realiza una llamada adicional a la PokeAPI usando la URL específica de ese Pokémon 
    # (que obtuviste en el paso anterior, ej. https://pokeapi.co/api/v2/pokemon/{nombre_o_id}/ ) para obtener sus detalles completos.
    for p in base_list:
        try:
            details = requests.get(p["url"], timeout=3000)
            details.raise_for_status()
            pdata = details.json()

            #De la respuesta detallada de cada Pokémon, extrae: Las estadísticas base HP, Attack, Defense, Special Attack, Special Defense, Speed). Usualmente están en una lista dentro del JSON de respuesta
            stats_map = {s["stat"]["name"]: s["base_stat"] for s in pdata.get("stats", [])}
            abilities = [a["ability"]["name"] for a in pdata.get("abilities", [])]
            abilities_str = ", ".join(abilities)

            #Añade nuevas columnas al DataFrame y al CSV para cada estadística base (ej. 'HP', 'Attack', 'Defense', etc.)
            enriched_list.append({
                "name":p["name"]
                ,"HP": stats_map.get("hp")
                ,"Attack": stats_map.get("attack")
                ,"Defense": stats_map.get("defense")
                ,"Special Attack": stats_map.get("special-attack")
                ,"Special Defense": stats_map.get("special-defense")
                ,"Speed": stats_map.get("speed")
                ,"Abilities": abilities_str
            })
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching details for {p['name']} ({p['url']})")
    return enriched_list

def generate_csv_to_blob(pokemon_list: list) -> bytes:
    df = pd.DataFrame(pokemon_list)
    output = io.StringIO()
    df.to_csv( output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME,  blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        logger.error("Error uploading file {e}")