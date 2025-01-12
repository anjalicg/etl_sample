# -----------------------------------------------------------------------------
# Copyright (c) 2025, Anjali Chandrashekharan Geetha
# All rights reserved.
# -----------------------------------------------------------------------------


import pandas as pd
import sqlite3
import logging
import os
from newsapi import NewsApiClient
from datetime import datetime, timedelta, time
from airflow import DAG

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)



"""
Apache Airflow is an orchestrating tool for automating tasks. 
Instead of manually calling functions, Airflow will handle it for you.
Because Airflow automates the ETL process without interrupting the run, 
it provides various ways of sharing data between tasks. 

One of the most common approaches involves using XComs (cross-communication), 
which allows tasks to exchange data within a DAG.

"""


def extract_news_data(query, language="en", sort_by="publishedAt"):
    news_api_key = "<key>"  # Replace with your actual API key
    news_api = NewsApiClient(api_key=news_api_key)
    try:
        result = news_api.get_everything(q=query, language=language, sort_by=sort_by)
        return result["articles"]
    except Exception as e:
        logging.error(f"Connection failed with {e}")


def clean_author_column(text):
    try:
        name = text.split(",")[0].title()
        return name
    except AttributeError:
        return "No Author"

def transform_news_data(articles):
    article_list = []
    for i in articles:
        article_list.append([value.get("name", 0) if key == "source" else value for key, value in i.items() if key in ["author", "title", "publishedAt", "content", "url", "source"]])

    df = pd.DataFrame(article_list, columns=["Source", "Author Name", "News Title", "URL", "Date Published", "Content"])
    df["Date Published"] = pd.to_datetime(df["Date Published"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    df["Author Name"] = df["Author Name"].apply(clean_author_column)
    return df


def load_news_data(data):
    current_folder_path, current_folder_name = os.path.split(os.getcwd())

    # print(f"Current folder: {current_folder_name}")
    # print(f"Current folder path: {current_folder_path}")

    with sqlite3.connect(f"{current_folder_path}/sqlite3/news_data.sqlite") as connection:
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_table (
                "Source" VARCHAR(30),
                "Author Name" TEXT,
                "News Title" TEXT,
                "URL" TEXT,
                "Date Published" TEXT,
                "Content" TEXT
            )
        ''')
    data.to_sql(name="news_table", con=connection, index=False, if_exists="append")


