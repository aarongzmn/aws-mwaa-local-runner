# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import os
import time
import logging
import psycopg2
from datetime import timedelta
from pathlib import Path
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from psycopg2.extras import RealDictCursor
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "Aaron Guzman",
}


def select_from_database(sql_query) -> [dict]:
    """SELECT FROM database and return query results
    """
    conn = PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(sql_query)
        query_response = curs.fetchall()
        response_list = [dict(row) for row in query_response]
    return response_list


def cleanup_working_directory(working_dir):
    file_list = os.listdir(working_dir)
    for file in file_list:
        file_path = os.path.join(working_dir, file)
        os.unlink(file_path)


config = {
    "working_dir": "telegram-scrape/media",
    "s3_bucket": "telegram-scrape",
    "s3_dir": "media/"
}


async def main(client):
    select_sql = """
    SELECT *
    FROM dialogs
    WHERE active=True
    """
    dialogs = select_from_database(select_sql)
    dialog_list = [i for i in dialogs if i["download_photos"] or i["download_videos"] or i["download_documents"] or i["download_webpage_media"]]
    working_dir = config["working_dir"]
    for dialog in dialog_list:
        dialog_id = dialog["id"]
        if dialog["download_photos_override_dt"]:
            download_photos_since = dialog["download_photos_override_dt"]
        else:
            download_photos_since = dialog["db_date_added"] - timedelta(days=30)
        select_sql = f"""
        SELECT
            messages_media.media_id,
            messages_media.media_type,
            messages_media.mime_type,
            messages_media.missing_media,
            messages.date,
            messages.dialog_id,
            messages.message_id,
            messages.public_url
        FROM
            messages_media
            LEFT JOIN messages ON messages_media.media_id = messages.media_id
        WHERE
            messages.dialog_id = {dialog_id}
            AND messages.date >= '{download_photos_since}'
            AND s3_bucket is NULL
            AND s3_key is NULL
            AND missing_media is False
            AND media_id_conflict is NULL
        ORDER BY messages.date DESC
        LIMIT 1000
        """
        messages_media_for_dialog = select_from_database(select_sql)
        messages_to_scrape = []
        if dialog["download_photos"]:
            if dialog["download_webpage_media"]:
                ids_missing_photo_media = [i for i in messages_media_for_dialog if i["media_type"] and "photo" in i["media_type"]]
            else:
                ids_missing_photo_media = [i for i in messages_media_for_dialog if i["media_type"] == "photo"]
            logging.info(f"Scraping {len(ids_missing_photo_media)} photos from Telegram")
            messages_to_scrape.extend(ids_missing_photo_media)
        if dialog["download_videos"]:
            if dialog["download_webpage_media"]:
                ids_missing_video_media = [i for i in messages_media_for_dialog if i["mime_type"] and "video" in i["mime_type"]]
            else:
                ids_missing_video_media = [i for i in messages_media_for_dialog if i["media_type"] == "document" and i["mime_type"] and "video" in i["mime_type"]]
            logging.info(f"Scraping {len(ids_missing_video_media)} videos from Telegram")
            messages_to_scrape.extend(ids_missing_video_media)
        account_dialog_list = [i.entity.__dict__ async for i in client.iter_dialogs()]
        dialog_api_info = [i for i in account_dialog_list if i["id"] == dialog_id]
        if not dialog_api_info:
            logging.info(f"Unable to locate API data for dialog_id: {dialog_id}. Maybe it was archived or deleted?")
            continue
        dialog_title = dialog_api_info[0]["title"]
        messages_media_s3_keys = []
        try:
            for message in messages_to_scrape:
                db_message_id = message["message_id"]
                db_message_media_id = message["media_id"]
                async for message in client.iter_messages(dialog_title, ids=db_message_id):
                    if not message:
                        messages_media_s3_keys.append(
                            {
                                "media_id": db_message_media_id,
                                "s3_bucket": None,
                                "s3_key": None,
                                "missing_media": True,
                                "media_id_conflict": "NULL"
                            }
                        )
                        logging.info(f"No media found for channel: {dialog_id}, message: {db_message_id}")
                        continue
                    if hasattr(message.peer_id, "channel_id"):
                        channel_id = message.peer_id.channel_id
                    else:
                        channel_id = "D" + dialog_id
                    message_url = f"https://t.me/c/{channel_id}/{db_message_id}"
                    if not message.media:
                        logging.info(f"{message_url}, NO MEDIA")
                    else:
                        if hasattr(message.media, "photo"):
                            media_id = message.media.photo.id
                            media_type = "photo"
                        elif hasattr(message.media, "webpage"):
                            media_id = message.media.webpage.id
                            media_type = "webpage"
                        elif hasattr(message.media, "document"):
                            media_id = message.media.document.id
                            media_type = "document"
                        else:
                            logging.info(message.media)
                            break
                        if db_message_media_id != media_id:
                            logging.info(f"Database media_id: '{db_message_media_id}' does not match API media_id: '{media_id}'. Continuing to next message.")
                            messages_media_s3_keys.append(
                                {
                                    "media_id": db_message_media_id,
                                    "s3_bucket": None,
                                    "s3_key": None,
                                    "missing_media": False,
                                    "media_id_conflict": media_id
                                }
                            )
                        else:
                            filename = f"{media_type}-{channel_id}-{db_message_id}-{media_id}"
                            media_path = await message.download_media(file=working_dir + "/" + filename)
                            try:
                                hook = S3Hook('s3_aaron')
                                hook.load_file(
                                    filename=media_path,
                                    key=media_path.replace("telegram-scrape/", ""),
                                    bucket_name=config["s3_bucket"],
                                    replace=False
                                )
                                os.unlink(media_path)
                                logging.info(f"Processed media for: {message_url}. File uploaded to S3 bucket '{config['working_dir']}' as '{filename}'. Removing file from '{media_path}'")
                                messages_media_s3_keys.append(
                                    {
                                        "media_id": db_message_media_id,
                                        "s3_bucket": config["s3_bucket"],
                                        "s3_key": media_path.replace("telegram-scrape/", ""),
                                        "missing_media": False,
                                        "media_id_conflict": "NULL"
                                    }
                                )
                            except ValueError as e:
                                if "already exists" in str(e):
                                    logging.info(str(e))
                                    messages_media_s3_keys.append(
                                        {
                                            "media_id": db_message_media_id,
                                            "s3_bucket": config["s3_bucket"],
                                            "s3_key": media_path.replace("telegram-scrape/", ""),
                                            "missing_media": False,
                                            "media_id_conflict": "NULL"
                                        }
                                    )
                                else:
                                    raise
        except:
            conn = PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn()
            for record in messages_media_s3_keys:
                update_sql = f"""
                UPDATE messages_media
                SET s3_bucket='{record["s3_bucket"]}', s3_key='{record["s3_key"]}', media_id_conflict={record["media_id_conflict"]}
                WHERE media_id={record["media_id"]}
                """
                with conn.cursor(cursor_factory=RealDictCursor) as curs:
                    curs.execute(update_sql)
                time.sleep(60)
            continue
    return


@dag(
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=days_ago(2),
    tags=['telegram']
)
def download_telegram_media():
    """
    ### Telegram Pipeline
    This DAG using the TaskFlow API to create a Telegram pipeline.
    (https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    @task()
    def download_telegram_messages():
        """
        #### Download Telegram Media
        """
        # Create working directory if it doesn't already exist
        working_dir = config["working_dir"]
        Path(working_dir).mkdir(parents=True, exist_ok=True)
        cleanup_working_directory(working_dir)

        query_string = """
        SELECT *
        FROM sessions
        WHERE active is True
            AND session_string IS NOT NULL
        """
        session_info = select_from_database(query_string)[0]
        session_string = session_info["session_string"]
        app_id = session_info["app_id"]
        api_hash = session_info["api_hash"]
        client = TelegramClient(StringSession(session_string), app_id, api_hash)
        with client:
            client.loop.run_until_complete(main(client))
        cleanup_working_directory(working_dir)
        return

    download_telegram_messages()


download_telegram_media = download_telegram_media()
