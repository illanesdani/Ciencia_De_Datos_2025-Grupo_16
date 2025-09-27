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

TMDB_URL_BASE="https://api.themoviedb.org/3"
TOKEN_TMDB = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkYWRjYTk2NjIyYTdmZTk5MjUxNGM0NWQxYzBiMjYxYSIsIm5iZiI6MTc1NjMyODA1OC43MjUsInN1YiI6IjY4YWY3MDdhOWE3OTRlNzI4YjM5MDkwNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.Q8upWLzXDwM5HVyMCg1lf7iAHPhDdZqhg0jRXZfpIB4"
FIELDNAMES_PELICULAS=[
                    "id", "title", "adult", "budget", "original_language", "popularity",
                    "release_date", "revenue", "runtime", "status", "tagline", 
                    "overview", "vote_average", "vote_count", "genres", "production_companies",
                    "spoken_languages", "belongs_to_collection", "keywords", "directors", "main_actors"]
# Cada pagina trae 22 pelis aprox, por ende el calculo para saber el total de registros que traemos
# va a ser 22 * años * paginas_por_año. Ahora son 150 paginas por año
# desde el 1940, por lo que son unas 280.000 peliculas
LIMITE_PAGINAS=150 # la api no permite hacer una request para paginas mayores a la 500
ANIO_DESDE=1940

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

task_logger = logging.getLogger("airflow.task")

def obtener_rangos_fechas(desde="2000-01-01", hasta=datetime.now().strftime("%Y-%m-%d")):
    """Genera rangos de fechas año por año desde 'desde' hasta 'hasta'."""
    rangos = []
    inicio = datetime.strptime(desde, "%Y-%m-%d")
    fin = datetime.strptime(hasta, "%Y-%m-%d")

    while inicio.year <= fin.year:
        comienzo = inicio.replace(month=1, day=1)
        fin_anio = inicio.replace(month=12, day=31)
        if fin_anio > fin:
            fin_anio = fin
        rangos.append((comienzo.strftime("%Y-%m-%d"), fin_anio.strftime("%Y-%m-%d")))
        inicio = inicio.replace(year=inicio.year + 1)

    return rangos

def obtener_ids_peliculas():
    def buscar_pagina_peliculas(page, fecha_inicio, fecha_fin):
        url = (
            f"{TMDB_URL_BASE}/discover/movie?"
            f"page={page}&sort_by=popularity.desc&include_adult=true&language=en-US"
            f"&primary_release_date.gte={fecha_inicio}&primary_release_date.lte={fecha_fin}"
        )
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {TOKEN_TMDB}"
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()["results"]
        except Exception as e:
            task_logger.error(f"Error al descargar pagina {page} ({fecha_inicio} a {fecha_fin}): {e}")
            return None

    with open("/opt/airflow/data/peliculas.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["id"])

        rangos_fechas = obtener_rangos_fechas(desde=f"{ANIO_DESDE}-01-01")  # Desde 1940 buscamos peliculas
        for fecha_inicio, fecha_fin in rangos_fechas:
            # Primero pedimos página 1 para ver cuántas páginas tiene este rango
            url_primera = (
                f"{TMDB_URL_BASE}/discover/movie?page=1&sort_by=popularity.desc&include_adult=true&language=en-US"
                f"&primary_release_date.gte={fecha_inicio}&primary_release_date.lte={fecha_fin}"
            )
            headers = {"accept": "application/json", "Authorization": f"Bearer {TOKEN_TMDB}"}
            resp = requests.get(url_primera, headers=headers, timeout=10)
            if resp.status_code != 200:
                task_logger.error(f"No se pudo obtener página 1 para rango {fecha_inicio} - {fecha_fin}")
                continue

            data = resp.json()
            total_pages = min(data.get("total_pages", 1), LIMITE_PAGINAS)  # respetamos el límite de TMDB
            task_logger.info(f"Procesando {total_pages} páginas para rango {fecha_inicio} - {fecha_fin}")

            with ThreadPoolExecutor(max_workers=40) as executor:
                futuros = {
                    executor.submit(buscar_pagina_peliculas, page, fecha_inicio, fecha_fin): page
                    for page in range(1, total_pages + 1)
                }

                for futuro in as_completed(futuros):
                    result = futuro.result()
                    if result:
                        ids = [[pelicula["id"]] for pelicula in result if "id" in pelicula]
                        writer.writerows(ids)


# Helper para poder paralelizar la busqueda de detalles (si no se demoraba una banda buscar todo 1 por 1)
def buscar_detalles_pelicula(pelicula_id):
    url = f"{TMDB_URL_BASE}/movie/{pelicula_id}?language=en-US&append_to_response=credits,keywords"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TOKEN_TMDB}"
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status() # Si largo error la peticion, esto larga error aca en el codigo
        data = r.json()
        # Hay datos que vienen como arreglos u otras cosas, que debemos extraer a texto
        data["genres"] = ", ".join([gen["name"] for gen in data["genres"]])
        data["production_companies"] = ", ".join([comp["name"] for comp in data["production_companies"]])
        data["spoken_languages"] = ", ".join([lang["english_name"] for lang in data["spoken_languages"]])
        # Puede venir en null si no esta en ninguna coleccion
        data["belongs_to_collection"] = (
            data["belongs_to_collection"]["name"]
            if isinstance(data.get("belongs_to_collection"), dict)
            else None
        )
        # Pueden ser una banda o ninguna, depende la fama de la peli
        data["keywords"] = ", ".join([keyword["name"] for keyword in data["keywords"]["keywords"]])
        # Unimos por si hay mas de 1 director, como en matrix
        # Viene el genero tamb. Asi que lo ponemos al lado del nombre del director y actor mas abajo, separados con
        # un | . En algunos es 0, 1, o 2, habra que ver que es que. Quiza 0 es no hay dato
        data["directors"] = ", ".join([f"{person["name"]}|{person["gender"]}" for person in data["credits"]["crew"] if person["job"] == "Director"])
        # Los actores vienen todos todos y pueden ser muchos. Pero al menos vienen ordenados como aparecen en los creditos
        # que es mas o menos un orden de importancia. Asi que solo los primeros 10 tomamos.
        data["main_actors"] = ", ".join([f"{person["name"]}|{person["gender"]}" for person in data["credits"]["cast"][:10]])
        task_logger.info(f"Descargados detalles de la película {data['id']}, {data['title']}")
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
    # Ejecutamos las requests concurrentes.
    with ThreadPoolExecutor(max_workers=50) as executor:
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
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(filas_actualizadas)

with DAG(
    dag_id="tmdb_peliculas_detalles_dag",
    default_args=default_args,
    description="Descarga películas y luego sus detalles, generando un archivo csv unificado.",
    start_date=datetime(2025, 9, 7),
    catchup=False,
) as dag:

    obtener_ids_peliculas = PythonOperator(
        task_id="obtener_ids_peliculas",
        python_callable=obtener_ids_peliculas,
    )

    buscar_detalles_peliculas = PythonOperator(
        task_id="agregar_detalles_peliculas_a_csv",
        python_callable=agregar_detalles_peliculas_a_csv,
    )

    obtener_ids_peliculas >> buscar_detalles_peliculas
