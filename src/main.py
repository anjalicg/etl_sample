# -----------------------------------------------------------------------------
# Copyright (c) 2025, Anjali Chandrashekharan Geetha
# All rights reserved.
# -----------------------------------------------------------------------------


from src.news_etl import extract_news_data, transform_news_data, load_news_data

articles = extract_news_data("India")
articles_after_transform = transform_news_data(articles)
print(articles_after_transform)
load_news_data(articles_after_transform)
