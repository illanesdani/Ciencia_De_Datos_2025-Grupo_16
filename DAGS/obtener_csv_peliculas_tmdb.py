import csv
import json
import time
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN_TMDB = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkYWRjYTk2NjIyYTdmZTk5MjUxNGM0NWQxYzBiMjYxYSIsIm5iZiI6MTc1NjMyODA1OC43MjUsInN1YiI6IjY4YWY3MDdhOWE3OTRlNzI4YjM5MDkwNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.Q8upWLzXDwM5HVyMCg1lf7iAHPhDdZqhg0jRXZfpIB4"
TMDB_PAGINAS_TOTALES = 100
FIELDNAMES_PELICULAS=[
                    "id", "title", "adult", "budget", "original_language", "popularity",
                    "release_date", "revenue", "runtime", "status", "vote_average", "vote_count"]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

task_logger = logging.getLogger("airflow.task")

# Trae los resultados de todos los HttpOperator 
# y guarda lo relevante de cada pelicula en un csv
def guardar_peliculas_en_csv(**kwargs):
    ti = kwargs["ti"]
    task_logger.info("Peliculas descargadas. Guardando en csv...")
    results = []
    for i in range(1, TMDB_PAGINAS_TOTALES):
        task_logger.info(f"Guardando peliculas de la pagina {i}")
        response = ti.xcom_pull(task_ids=f"buscar_peliculas_pagina_{i}")
        data = json.loads(response)
        for movie in data.get("results", []):
            results.append([movie["id"]])
    task_logger.info(f"Total de peliculas: {len(results)}")
    with open("/opt/airflow/data/peliculas.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])
        writer.writerows(results)

# Helper para poder paralelizar la busqueda de detalles (si no se demoraba una banda buscar todo 1 por 1)
def buscar_detalles_pelicula(pelicula_id):
    url = f"https://api.themoviedb.org/3/movie/{pelicula_id}?language=en-US"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TOKEN_TMDB}"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status() # Si largo error la peticion, esto larga error aca en el codigo
        data = r.json()
        task_logger.info(f"Descargados detalles de la película {pelicula_id}, {data['title']}")
        # Creamos un diccionario solo con los campos que nos interesan
        return {field: data.get(field) for field in FIELDNAMES_PELICULAS}
    except Exception as e:
        task_logger.error(f"Error al descargar detalles de {pelicula_id}: {e}")
        return None

# Busca los detalles de las peliculas. Son muchas pelis, y TMDB pone un limite de 
# 50 requests por segundo, asi que largamos mas o menos eso por segundo a la vez
def agregar_detalles_peliculas_a_csv():
    task_logger.info("Guardando detalles de peliculas en csv...")
    # Leemos todos los ids a la vez para luego poder paralelizar la busqueda
    with open("/opt/airflow/data/peliculas.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        movie_ids = [row["id"] for row in reader]

    filas_actualizadas = []
    # Ejecutamos las requests concurrentes
    with ThreadPoolExecutor(max_workers=40) as executor:
        # Genera todas las requests a la vez para todas las peliculas, y se van ejecutando
        # en paralelo a medida que quedan disponibles los hilos del ThreadPool. copado
        futuros_resultados_detalles = {executor.submit(buscar_detalles_pelicula, pelicula_id): pelicula_id for pelicula_id in movie_ids}

        # A medida que se van completando esas requests, ejecuta este bucle para cada resultado
        # que solo agrega los detalles de la peli al filas_actualizadas
        for resultado_request_detalles in as_completed(futuros_resultados_detalles):
            result = resultado_request_detalles.result()
            if result:
                filas_actualizadas.append(result)
        
    with open("/opt/airflow/data/peliculas.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = fieldnames = FIELDNAMES_PELICULAS
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filas_actualizadas)

with DAG(
    dag_id="tmdb_peliculas_detalles_dag",
    default_args=default_args,
    description="Descarga películas y luego sus detalles, generando un archivo csv unificado.",
    start_date=datetime(2025, 9, 7),
    catchup=False,
) as dag:

    # Todas van a correr en paralelo
    tasks = []
    for page in range(1, TMDB_PAGINAS_TOTALES):
        task = HttpOperator(
            task_id=f"buscar_peliculas_pagina_{page}",
            # un embole, pero hay que definir esta conn_id en la interfaz de airflow
            # Config -> Connections -> Add Connection -> y poner los datos de TMDB 
            # (solo la base URL en realidad)
            http_conn_id="tmdb_api",
            endpoint=f"/3/discover/movie?page={page}&sort_by=popularity.desc&include_adult=false&include_video=false&language=en-US",
            method="GET",
            headers={
                "accept": "application/json",
                "Authorization": f"Bearer {TOKEN_TMDB}"
            },
        )
        tasks.append(task)

    peliculas_csv = PythonOperator(
        task_id="guardar_peliculas_en_csv",
        python_callable=guardar_peliculas_en_csv,
    )

    detalles_csv = PythonOperator(
        task_id="agregar_detalles_peliculas_a_csv",
        python_callable=agregar_detalles_peliculas_a_csv,
    )

    tasks >> peliculas_csv >> detalles_csv
