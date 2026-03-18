import os
import feedparser
import requests
from bs4 import BeautifulSoup
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
import time
from datetime import datetime, timedelta
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    Range,
)
from dotenv import load_dotenv
import numpy as np
import uuid


def get_full_news_content(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            return soup.get_text(separator=" ", strip=True)

        elif response.status_code == 429:
            print("Block (429)! ")
            return ""

    except Exception as e:
        print(f"Error: {e}")
        return ""


def run_rss_pipeline():
    # Business: https://finance.yahoo.com/news/rss
    # Economy (Macro): https://finance.yahoo.com/rss/economy-news
    # Politics: https://news.yahoo.com/rss/politics
    rss_url = [
        "https://finance.yahoo.com/rss/economy-news",
        "https://news.yahoo.com/rss/industry-news",
        "https://news.yahoo.com/rss/politics",
        "https://finance.yahoo.com/rss/world",
    ]
    title = []
    entries = []
    for rss in rss_url:
        print(f"rss {rss}")
        feed = feedparser.parse(rss)
        print(f"Found {len(feed.entries)} news items.")
        for entry in feed.entries[:1]:
            if entry.title not in entries:
                title.append(entry.title)
                entries.append(
                    {
                        "title": entry.title,
                        "link": entry.link,
                        "published": entry.published,
                    }
                )
    all_news_nested = []
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
    for entry in entries[:2]:
        print(f"Processing: {entry['title']}")
        # ดึงเนื้อหาเต็มจาก Link
        full_text = get_full_news_content(entry["link"])
        if full_text:
            # รวม Title และเนื้อหาเข้าด้วยกัน
            chunks = text_splitter.split_text(full_text)
            news_package = [
                f"Title: {entry['title']}",
                f"Published: {entry['published']}",
            ] + [f"Content: {c}" for c in chunks]

            all_news_nested.append(news_package)
        time.sleep(1)
    return all_news_nested, entries


def run_embedding(all_news_nested, entries):
    """
    รับ Nested List และข้อมูลดิบ มาทำ Embedding และเตรียมโครงสร้าง PointStruct
    """
    news_index_map = []
    all_chunks_to_embed = []

    embeddings_model = GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001",
        task_type="retrieval_document",
        output_dimensionality=768,
    )

    # จัดเตรียมข้อมูล Flatten
    for i, package in enumerate(all_news_nested):
        title = package[0]
        chunks = package[2:]
        for c in chunks:
            all_chunks_to_embed.append(f"{title}\n{c}")
            news_index_map.append(i)

    news_index_map = np.array(news_index_map)

    # ทำ Embedding
    print(f"Embedding {len(all_chunks_to_embed)} chunks...")
    vectors_list = embeddings_model.embed_documents(all_chunks_to_embed)
    vectors_np = np.array(vectors_list)

    # สร้าง Points (เตรียมข้อมูลก่อนส่งเข้า DB)
    points = []
    unique_news_ids = np.unique(news_index_map)

    for news_id in unique_news_ids:
        original_entry = entries[news_id]
        mask = news_index_map == news_id
        current_news_vectors = vectors_np[mask]

        # ดึงเนื้อหาเฉพาะของข่าวนี้
        current_texts = [
            all_chunks_to_embed[j]
            for j in range(len(news_index_map))
            if news_index_map[j] == news_id
        ]

        for j, vector in enumerate(current_news_vectors):
            # สร้าง ID เพื่อป้องกันข้อมูลซ้ำ
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vector.tolist(),
                    payload={
                        "title": original_entry["title"],
                        "link": original_entry["link"],
                        "published": original_entry["published"],
                        "content": current_texts[j],
                    },
                )
            )

    return points


def run_insert(points):
    """
    รับ Points มาทำการเชื่อมต่อและ Upsert ลง Qdrant
    """
    # ดึงค่าจาก Environment Variables
    host = os.getenv("QDRANT_HOST", "localhost")
    port = int(os.getenv("QDRANT_PORT", 6333))
    collection_name = os.getenv("COLLECTION_NAME", "news_analysis")

    client = QdrantClient(host=host, port=port)

    # ตรวจสอบและสร้าง Collection
    if not client.collection_exists(collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )

    # Upsert ลง Qdrant
    client.upsert(collection_name=collection_name, points=points)
    print(f"✅ Upserted {len(points)} points to Qdrant successfully.")

    return True


def delete_old_news(days_threshold=10):
    threshold_dt = datetime.now() - timedelta(days=days_threshold)
    threshold_timestamp = threshold_dt.timestamp()

    print(f"Deleting data with timestamp older than: {threshold_timestamp}")
    host = os.getenv("QDRANT_HOST", "localhost")
    port = int(os.getenv("QDRANT_PORT", 6333))

    client = QdrantClient(host=host, port=port)
    client.delete(
        collection_name=os.getenv("COLLECTION_NAME"),
        points_selector=Filter(
            must=[
                FieldCondition(
                    key="published_timestamp", range=Range(lt=threshold_timestamp)
                )
            ]
        ),
    )


if __name__ == "__main__":
    load_dotenv()

    (all_news_nested, entries) = run_rss_pipeline()

    prepared_points = run_embedding(all_news_nested, entries)

    run_insert(prepared_points)

    delete_old_news(10)
