import csv
import json
import time
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from datetime import timedelta, datetime

TOKEN_TMDB = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkYWRjYTk2NjIyYTdmZTk5MjUxNGM0NWQxYzBiMjYxYSIsIm5iZiI6MTc1NjMyODA1OC43MjUsInN1YiI6IjY4YWY3MDdhOWE3OTRlNzI4YjM5MDkwNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.Q8upWLzXDwM5HVyMCg1lf7iAHPhDdZqhg0jRXZfpIB4"
TMDB_PAGINAS_TOTALES = 3

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
    with open("/opt/airflow/data/peliculas.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])
        writer.writerows(results)

# Por cada pelicula del csv, busca sus detalles en un GET independiente para cada pelicula
def agregar_detalles_peliculas_a_csv(**kwargs):
    ti = kwargs["ti"]
    task_logger.info("Guardando detalles de peliculas en csv...")
    updated_rows = []
    with open("/opt/airflow/data/peliculas.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            task_logger.info(f"Guardando detalles de pelicula {row['id']}")
            url = f"https://api.themoviedb.org/3/movie/{row['id']}?language=en-US"
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {TOKEN_TMDB}"
            }
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                data = r.json()
                updated_rows.append({
                    "id": data["id"],
                    "title": data.get("title"),
                    "adult": data.get("adult"),
                    "budget": data.get("budget"),
                    "original_language": data.get("original_language"),
                    "popularity": data.get("popularity"),
                    "release_date": data.get("release_date"),
                    "revenue": data.get("revenue"),
                    "runtime": data.get("runtime"),
                    "status": data.get("status"),
                    "vote_average": data.get("vote_average"),
                    "vote_count": data.get("vote_count"),
                })
            time.sleep(0.05)  # delay para respetar rate limit

    with open("/opt/airflow/data/peliculas.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = fieldnames = [
                    "id", "title", "adult", "budget", "original_language", "popularity",
                    "release_date", "revenue", "runtime", "status", "vote_average", "vote_count"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

with DAG(
    dag_id="tmdb_peliculas_detalles_dag",
    default_args=default_args,
    description="Descarga películas y luego sus detalles, generando un archivo csv unificado.",
    start_date=datetime(2025, 9, 7),
    catchup=False,
) as dag:

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
