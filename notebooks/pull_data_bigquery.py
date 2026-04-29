from google.cloud import bigquery
import pandas as pd
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r''

client = bigquery.Client()

query2026 = """
    SELECT
        *
    FROM
        `mydicosilver.sct_data.sct2026`
"""
# Năm 2026 chuẩn rồi
data2026 = client.query(query2026).to_dataframe()

print(data2026)
