import pymysql, pandas as pd, streamlit as st,os


def get_conn():
    return pymysql.connect(host = os.getenv("DB_HOST"), user = os.getenv("DB_USER"), password = os.getenv("DB_PASSWORD"), database = os.getenv("DB_NAME"), cursorclass = pymysql.cursors.DictCursor)


def run_query(query):
    conn = get_conn()

    with conn.cursor() as cursor:
        cursor.execute(query)

        result = cursor.fetchall()

    

    conn.close()

    return pd.DataFrame(result)

