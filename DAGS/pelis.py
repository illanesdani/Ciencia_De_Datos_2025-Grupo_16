import csv
import json
import time
import requests
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkYWRjYTk2NjIyYTdmZTk5MjUxNGM0NWQxYzBiMjYxYSIsIm5iZiI6MTc1NjMyODA1OC43MjUsInN1YiI6IjY4YWY3MDdhOWE3OTRlNzI4YjM5MDkwNyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.Q8upWLzXDwM5HVyMCg1lf7iAHPhDdZqhg0jRXZfpIB4"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def save_movies_to_csv(**context):
    results = []
    for i in range(1, 101):
        ti = context["ti"]
        response = ti.xcom_pull(task_ids=f"get_movies_page_{i}")
        data = json.loads(response)
        for movie in data.get("results", []):
            results.append([movie["id"], movie["title"]])
    with open("/tmp/movies.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "title"])
        writer.writerows(results)

def add_movie_details_to_csv():
    updated_rows = []
    with open("/tmp/movies.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = f"https://api.themoviedb.org/3/movie/{row['id']}?language=en-US"
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {TOKEN}"
            }
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                data = r.json()
                updated_rows.append({
                    "id": data["id"],
                    "title": data.get("title"),
                    "release_date": data.get("release_date"),
                    "runtime": data.get("runtime")
                })
            time.sleep(1)  # delay para respetar rate limit
    with open("/tmp/movies.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = ["id", "title", "release_date", "runtime"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

with DAG(
    dag_id="tmdb_movies_details_dag",
    default_args=default_args,
    description="Descarga películas y luego sus detalles",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    tasks = []
    for page in range(1, 101):
        task = SimpleHttpOperator(
            task_id=f"get_movies_page_{page}",
            http_conn_id="tmdb_api",
            endpoint=f"/3/discover/movie?page={page}&sort_by=popularity.desc&include_adult=false&include_video=false&language=en-US",
            method="GET",
            headers={
                "accept": "application/json",
                "Authorization": f"Bearer {TOKEN}"
            },
        )
        tasks.append(task)

    save_csv = PythonOperator(
        task_id="save_movies_to_csv",
        python_callable=save_movies_to_csv,
        provide_context=True,
    )

    enrich_csv = PythonOperator(
        task_id="add_movie_details_to_csv",
        python_callable=add_movie_details_to_csv,
    )

    tasks >> save_csv >> enrich_csv
