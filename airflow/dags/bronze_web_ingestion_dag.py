import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from collections import defaultdict
from airflow.decorators import dag, task


BASE_URL = "https://www.football-espana.net"
TARGET_URL = f"{BASE_URL}/category/la-liga/real-madrid"
BRONZE_PATH = "/opt/airflow/datalake_bronze" 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="football_scraping_universal_bronze",
    default_args=default_args,
    description="Extracts ALL web data to Bronze layer as CSV",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["bronze", "universal_extraction"]
)
def football_universal_dag():

    @task()
    def get_news_links():
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(TARGET_URL, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            posts = soup.find_all("div", class_="post-container")
            
            links = []
            for post in posts:
                link_tag = post.find("a", href=True)
                if link_tag:
                    href = link_tag.get("href")
                    links.append(BASE_URL + href if href.startswith("/") else href)
            return links
        except Exception as e:
            print(f"Error obteniendo links: {e}")
            return []

    @task()
    def extract_full_article_data(url): 
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            raw_data = defaultdict(list)
            raw_data["url"] = url

            # 1. Extracción masiva de textos
            tags_to_track = ['h1', 'h2', 'h3', 'h4', 'h5', 'p', 'span', 'div', 'a', 'li', 'strong', 'em', 'time']
            for tag_name in tags_to_track:
                elements = soup.find_all(tag_name)
                texts = [el.get_text(strip=True) for el in elements if el.get_text(strip=True)]
                raw_data[f'{tag_name}_all_texts'] = texts[:10]

            # 2. Hrefs y SRCs
            raw_data['all_hrefs'] = [a.get('href', '') for a in soup.find_all('a', href=True)][:20]
            raw_data['all_src'] = [img.get('src', '') for img in soup.find_all('img')][:10]

            # 3. Clases CSS
            raw_data['all_classes'] = list(set(
                cls for el in soup.find_all() for cls in el.get('class', []) if cls
            ))

            # 4. Meta Tags
            metas = {}
            for meta in soup.find_all('meta'):
                key = meta.get('property') or meta.get('name')
                value = meta.get('content')
                if key and value:
                    metas[key] = value
            raw_data['all_meta'] = str(metas) 

            # 5. Lógica de Autor y Fecha
            author, date = None, None
            for block in soup.find_all(["p", "span", "div"]):
                text = block.get_text("\n", strip=True)
                if "Posted by" in text:
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    if "Posted by" in lines:
                        idx = lines.index("Posted by")
                        if idx + 1 < len(lines): author = lines[idx + 1]
                        if idx + 2 < len(lines): date = lines[idx + 2]
                    break
            
            raw_data["author"] = author
            raw_data["published_time"] = date if date else metas.get("article:published_time")
            raw_data["ingestion_timestamp"] = datetime.now().isoformat()

            return dict(raw_data)

        except Exception as e:
            return {'url': url, 'error': str(e), 'status': 'error'}

    @task()
    def save_to_bronze_json(data_list):
        import json

        if not data_list or len(data_list) == 0:
            print("No hay datos para guardar.")
            return

        # Nomenclatura Workshop
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"web_scraping_realmadrid_{timestamp}.json"
        full_path = os.path.join(BRONZE_PATH, filename)

        if not os.path.exists(BRONZE_PATH):
            os.makedirs(BRONZE_PATH)

        # Guardar como JSON
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(data_list, f, ensure_ascii=False, indent=4)

        print(f"JSON guardado en: {full_path}")
        return full_path
    
    @task()
    def collect_results(results):
        return list(results)
    # Flujo de ejecución
    links_list = get_news_links()

    articles_results = extract_full_article_data.expand(url=links_list)

    collected = collect_results(articles_results)

    save_to_bronze_json(collected)


dag_instance = football_universal_dag()