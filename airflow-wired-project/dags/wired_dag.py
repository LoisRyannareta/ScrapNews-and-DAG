from datetime import datetime, timedelta
import requests
import re
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ========== KONFIGURASI ==========
API_URL = "http://host.docker.internal:8000/articles"
TABLE_NAME = "wired_articles"

# ========== TASK 1: EXTRACT ==========
def extract_from_api(**context):
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict):
            if 'articles' in data:
                articles = data['articles']
            elif 'data' in data:
                articles = data['data']
            else:
                raise ValueError("Format JSON tidak dikenali (dict tanpa key 'articles'/'data')")
        elif isinstance(data, list):
            articles = data
        else:
            raise ValueError(f"Format JSON tidak valid: {type(data)}")

        context['task_instance'].xcom_push(key='articles', value=articles)

        print(f"Berhasil ambil {len(articles)} artikel")
        return articles

    except Exception as e:
        print(f"Error extract: {e}")
        raise


# ========== TASK 2: TRANSFORM ==========
def transform_dates(**context):
    articles = context['task_instance'].xcom_pull(
        key='articles',
        task_ids='extract_from_api'
    )

    print("DEBUG TYPE:", type(articles))

    if not articles:
        raise ValueError("Tidak ada data dari API")

    if not isinstance(articles, list):
        raise ValueError(f"Data bukan list: {type(articles)}")

    transformed = []

    for article in articles:
        if not isinstance(article, dict):
            print("Skip data bukan dict:", article)
            continue

        raw_date = article.get('scraped_at', '')
        if raw_date:
            match = re.search(r'(\d{4}-\d{2}-\d{2})', raw_date)
            clean_date = match.group(1) if match else raw_date[:10]
        else:
            clean_date = None

        raw_author = article.get('author', 'Unknown')
        clean_author = ' '.join(raw_author.strip().title().split())

        transformed.append({
            'title': article.get('title', ''),
            'author': clean_author,
            'date': clean_date,
            'url': article.get('url', ''),
            'content_preview': article.get('description', '')[:200]
        })

    print(f"Transform selesai: {len(transformed)} data")

    context['task_instance'].xcom_push(
        key='transformed_articles',
        value=transformed
    )

    return transformed


# ========== TASK 3: LOAD ==========
def load_to_postgres(ti):
    """Load ke PostgreSQL"""
    articles = ti.xcom_pull(task_ids='transform_dates')

    if isinstance(articles, str):
        articles = json.loads(articles)

    if not articles:
        raise ValueError("Tidak ada data untuk di-load")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # --- Create table ---
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            date DATE,
            url TEXT UNIQUE,
            content_preview TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    inserted = 0
    skipped = 0

    for article in articles:
        try:
            cursor.execute(f"""
                INSERT INTO {TABLE_NAME}
                (title, author, date, url, content_preview)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (
                article['title'],
                article['author'],
                article['date'],
                article['url'],
                article['content_preview']
            ))

            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        except Exception as e:
            print(f" Gagal insert: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Load selesai: {inserted} baru, {skipped} duplikat")


# ========== DEFAULT ARGUMENT ==========
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ========== DAG ==========
dag = DAG(
    'wired_articles_etl',
    default_args=default_args,
    description='ETL artikel dari API ke PostgreSQL',
    schedule='0 8 * * *',
    catchup=False,
    tags=['etl', 'wired'],
)

# ========== TASKS ==========
extract_task = PythonOperator(
    task_id='extract_from_api',
    python_callable=extract_from_api,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_dates',
    python_callable=transform_dates,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

# ========== FLOW ==========
extract_task >> transform_task >> load_task