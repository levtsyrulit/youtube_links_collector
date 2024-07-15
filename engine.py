import streamlit as st
import psycopg2
import requests
import json
import pandas as pd
import random

@st.cache_resource(show_spinner=False, ttl=60)
def get_connection():
    return psycopg2.connect(**st.secrets["postgres"])

def get_safe_connection():
    conn = get_connection()
    if not conn or conn.closed or conn.status == ConnectionError:
        st.cache_resource(clear_cache=True)
        conn = get_connection()
    return conn


def run_query(conn, query, **kwargs):
    try:
        with conn.cursor() as cur:
            cur.execute(query, kwargs)
            results = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]

        return [dict(zip(column_names, row)) for row in results]
    except Exception as e:
        print(e)
        conn.cancel()
        return []


def sync_tasks():
    try:
        conn = get_safe_connection()
        tasks = get_tasks_metrics()

        QUERY_UNCOMPLETED_TASKS = """
            SELECT id FROM cyber_tasks WHERE succeeded IS NULL
        """

        uncompleted_tasks_db = run_query(conn, QUERY_UNCOMPLETED_TASKS)

        if len(uncompleted_tasks_db) > 0:
            uncompleted_tasks_db = pd.DataFrame(uncompleted_tasks_db)
            intersection = pd.merge(tasks, uncompleted_tasks_db, how='inner', left_on='taskId', right_on='id')
            with conn.cursor() as cur:
                for index, row in intersection.iterrows():
                    cur.execute(
                        f"UPDATE cyber_tasks SET succeeded = {row['succeeded']}, time_spent = {row['spentSeconds']}, worker_id = {row['worker_id']} WHERE id = '{row['taskId']}'",
                    )
                    conn.commit()
    except Exception as e:
        print(e)

def get_tasks_metrics():
    def query():
        url = "https://pbb-prod.rabbit.tech/apis/fetchAllTaskMetrics"
        headers = {"Content-Type": "application/json"}
        data = {"key": st.secrets["rabbit_key"]}
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data)).json()
            return pd.DataFrame(response['submittedTasks'])
        except Exception as e:
            print(e)
            return []
    
    results = query()

    if st.secrets.get('debug') is not None:
        conn = get_safe_connection()
        task_ids = run_query(conn, "SELECT id FROM cyber_tasks ORDER BY RANDOM()")
        worker_ids = run_query(conn, "SELECT truck_id as id FROM cyber_workers ORDER BY RANDOM()")

        tasks = []

        for i in range(len(task_ids)):
            worker_id = random.choice(worker_ids)['id']
            task_id = task_ids[i]['id']

            tasks.append({
                "taskId": task_id,
                "workerId": worker_id,
                "succeeded": random.random() > 0.5,
                "spentSeconds": random.randint(1, 100),
            })

        return pd.DataFrame(tasks)
    else: return results


def get_completed_tasks_for_worker(worker_id: str):
    conn = get_safe_connection()

    QUERY_COMPLETED_TASKS = """
        SELECT id, succeeded FROM cyber_tasks WHERE worker_id = %(worker_id)s AND succeeded IS NOT NULL AND deleted_at IS NULL
    """

    return run_query(conn, QUERY_COMPLETED_TASKS, worker_id=worker_id)

