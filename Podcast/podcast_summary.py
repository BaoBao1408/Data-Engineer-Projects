from datetime import datetime, timedelta
from airflow.decorators import dag, task
import requests
import xmltodict
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id = 'podcast_summary',
    start_date = datetime(2024,4,4),
    schedule_interval='@daily',
    catchup = False
)

def podcast_summary():
    
    create_database = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres_local',
        sql="""
            create table if not exists episodes(
                link text PRIMARY KEY,
                title text,
                filename text,
                published text,
                description text
            )
        """
    )
    
    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace")
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']
        print(f"Found {len(episodes)} episodes")
        return episodes


        
    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)
    
    
    @task()
    def load_episodes(episodes):
        hook = PostgresHook(postgres_conn_id='postgres_local')

        stored = hook.get_pandas_df("SELECT * from episodes;")
        new_episodes = []
        for episode in episodes:
            if episode['link'] not in stored['link'].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"],filename])
        hook.insert_rows(table="episodes", rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])
    
    load_episodes(podcast_episodes)



summary = podcast_summary()