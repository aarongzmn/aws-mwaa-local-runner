# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case

import json
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from telethon.sync import TelegramClient
from telethon.utils import get_display_name
from telethon.sessions import StringSession

import logging
# logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


def insert_into_database(table_name: str, record_list: [dict], ignore_dup_key: str = None) -> None:
    """Bulk INSERT INTO database (1000 rows at a time). Input list shoud be
    list of dictionaries with the keys matching the table column names.
    """
    col_names = ", ".join(record_list[0].keys())
    insert_values = [tuple(e.values()) for e in record_list]
    with PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn() as conn:
        with conn.cursor() as curs:
            if ignore_dup_key:
                logging.info("Ignoring duplicate values.")
                sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s ON CONFLICT ({ignore_dup_key}) DO NOTHING"
            else:
                sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
            psycopg2.extras.execute_values(curs, sql, insert_values, page_size=1000)
    return


async def get_newest_message_id_from_telegram_api(dialog_name, client):
    """Get the last message id of a telegram group from telethon api

    :param dialog_name:
    :return last_message_id:
    """
    try:
        async for message in client.iter_messages(dialog_name, limit=1):
            last_message_id = message.id
            return last_message_id
    except Exception as e:
        logging.error(e)


def get_message_attributes_from_dialog(message):
    message_dict = message.__dict__
    if "file" in message_dict:
        message_dict["file"] = message_dict["file"].__dict__

    if "fwd_from" in message_dict and message_dict["fwd_from"]:
        message_dict["fwd_from"] = message_dict["fwd_from"].__dict__

    if "media" in message_dict and message_dict["media"]:
        message_dict["media"] = todict(message_dict["media"])

    if message_dict["replies"] and message_dict["replies"]:
        message_dict["replies"] = message_dict["replies"].__dict__
    return message_dict


def todict(obj, classkey=None) -> dict:
    """Recursively convert object to dictionary.
    """
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = todict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return todict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [todict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, todict(value, classkey))
            for key, value in obj.__dict__.items()
            if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    elif isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S%z")
    else:
        return obj


async def get_telegram_message_updates(client):
    st = time.time()
    account_dialog_list = [todict(i) async for i in client.iter_dialogs()]

    scrape_options = config["scrape_options"]
    if scrape_options["groups"] is False and scrape_options["channels"]:
        # Only scrape 'channel' dialogs
        api_dialog_data_list = [i for i in account_dialog_list if not i["is_group"] and i["is_channel"]]
    elif scrape_options["groups"] and scrape_options["channels"] is False:
        # Only scrape 'group' dialogs
        api_dialog_data_list = [i for i in account_dialog_list if i["is_group"]]
    else:
        # Scrape both channels and groups dialogs
        api_dialog_data_list = [i for i in account_dialog_list if not i["is_user"]]

    if config['max_scrape_dialogs']:
        logging.warning(f"Scrape settings set to scrape max {config['max_scrape_dialogs']} dialogs.")
        api_dialog_data_list = api_dialog_data_list[0:config["max_scrape_dialogs"]]
    if config["max_scrape_messages"]:
        logging.warning(f"Scrape settings set to scrape max {config['max_scrape_messages']} messages.")

    if config["save_dialogs_to_db"]:
        try:
            updates_for_dialog_tables = get_updates_for_dialog_tables(api_dialog_data_list)
            if len(updates_for_dialog_tables["dialogs"]) > 0:
                insert_into_database("dialogs", updates_for_dialog_tables["dialogs"], ignore_dup_key="id")
            if len(updates_for_dialog_tables["dialog_updates"]) > 0:
                insert_into_database("dialog_updates", updates_for_dialog_tables["dialog_updates"])
            if len(updates_for_dialog_tables["dialog_media"]) > 0:
                insert_into_database("dialog_media", updates_for_dialog_tables["dialog_media"])
        except:
            logging.info(api_dialog_data_list.keys())
            logging.info(len(updates_for_dialog_tables["dialogs"]))
            logging.info(updates_for_dialog_tables["dialogs"].keys())

    if config["save_dialogs_to_s3"]:
        ts = str(datetime.utcnow().timestamp()).split(".")[0]
        key_name = f"dialogs/{ts}.json"
        json_bytes = json.dumps(api_dialog_data_list).encode('utf-8')
        hook = S3Hook('s3_aaron')
        hook.load_file_obj(
            json_bytes,
            key_name,
            bucket_name=config["root_dir"],
            replace=False
        )
    else:
        logging.warning(f"Scrape settings set to 'save_dialogs_to_s3'={config['save_dialogs_to_s3']}, this will disable saving dialog files.")

    if config["save_messages"] is False:
        logging.warning(f"Scrape settings set to 'save_messages'={config['save_messages']}, this will disable saving dialog files.")

    sql_query = """
    SELECT *
    FROM dialogs
    """
    db_dialogs = select_from_database(sql_query)
    for api_dialog_data in api_dialog_data_list:
        dialog_name = api_dialog_data["name"]
        dialog_username = api_dialog_data["entity"].get("username")
        dialog_id = api_dialog_data["entity"]["id"]
        db_dialog_data_list = [i for i in db_dialogs if i["id"] == dialog_id]
        db_dialog_data = db_dialog_data_list[0]
        if db_dialog_data["active"] is False:
            continue

        logging.info(f"Starting message scrape for: '{dialog_name}', '{dialog_username}' | '{dialog_id}'")
        sql_query = f"""
        SELECT MAX(message_id)
        FROM messages
        WHERE dialog_id = {dialog_id}
        """
        last_db_message_id = select_from_database(sql_query)[0].get("max")
        if last_db_message_id is None:
            last_db_message_id = 1
        scrape_to = await get_newest_message_id_from_telegram_api(dialog_name, client)
        if scrape_to is None:
            logging.info(f"No messages found for diolog: '{dialog_username}' | '{dialog_id}'")
            continue
        if last_db_message_id >= scrape_to:
            logging.info("Database is up to date. No scrape needed.")
            continue
        scrape_from = 1
        limit = 1000
        if config["max_scrape_messages"]:
            limit = config["max_scrape_messages"]
            scrape_from = max(scrape_to - limit, 0)
        else:
            if last_db_message_id:
                scrape_from = last_db_message_id
            else:
                logging.info("No messages found in database. Starting scrape at message_id = 1")

        logging.info(f"Scrape range for dialog_id '{dialog_id}' from {scrape_from} to {scrape_to}")
        message_list = []
        while scrape_from < scrape_to:
            async for message in client.iter_messages(dialog_name, min_id=scrape_from, reverse=True, limit=limit):
                # https://docs.telethon.dev/en/stable/modules/custom.html#telethon.tl.custom.message.Message
                scrape_from = message.id
                if message.media:
                    media_dir = ""
                else:
                    media_dir = None
                message_attribs = get_message_attributes_from_dialog(message)
                # Enrich data (Add additional attributes)
                message_dict = todict(message_attribs)
                message_dict["media_dir"] = media_dir
                message_dict["dialog_id"] = dialog_id
                message_dict["sender"] = get_display_name(message.sender)
                private_url = f"https://t.me/c/{dialog_id}/{message.id}"
                message_dict["private_url"] = private_url
                public_url = f"https://t.me/{dialog_username}/{message.id}"
                message_dict["public_url"] = public_url
                message_list.append(message_dict)

            if config["save_messages"] and len(message_list) > config["save_messages_chunk_size"]:
                from_id = min([i["id"] for i in message_list])
                to_id = max([i["id"] for i in message_list])
                key_name = f"messages/staging/{dialog_id}-from-{from_id}-to-{to_id}-({len(message_list)}).json"
                logging.info(f"Saving file as: {key_name}")
                string_data = json.dumps(message_list)
                hook = S3Hook('s3_aaron')
                hook.load_string(
                    string_data,
                    key=key_name,
                    bucket_name=config["root_dir"],
                    replace=False
                )
                message_list = []

        if config["save_messages"]:
            from_id = min([i["id"] for i in message_list])
            to_id = max([i["id"] for i in message_list])
            key_name = f"messages/staging/{dialog_id}-from-{from_id}-to-{to_id}-({len(message_list)}).json"
            logging.info(f"Saving file as: {key_name}")
            string_data = json.dumps(message_list)
            hook = S3Hook('s3_aaron')
            hook.load_string(
                string_data,
                key=key_name,
                bucket_name=config["root_dir"],
                replace=False
            )
    et = time.time()
    elapsed = et - st
    logging.info(f"Process completed in {elapsed} seconds.")
    return


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['telegram']
)
def telegram_ingest_media():
    """
    ### Telegram Pipeline
    This DAG using the TaskFlow API to create a Telegram pipeline.
    (https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    @task()
    def upload_staging_files_to_database():
        """
        #### Read S3 files and upload to database
        Check the S3 bucket 'staging' directory for new files and for each file:
        1. Read file from S3
        2. Upload file contents to database
        3. Move file from 'staging' dir to 'archive' dir
        4. Delete file from 'staging' dir
        """
        bucket_name = config["root_dir"]
        staging_dir = config["staging_dir"]
        archive_dir = config["archive_dir"]
        logging.info(f"Looking for keys in bucket: '{bucket_name}' with prefix '{staging_dir}'")
        s3_hook = S3Hook('s3_aaron')
        staging_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=staging_dir)
        staging_keys = [i for i in staging_files if i != staging_dir]
        logging.info(f"Found {len(staging_keys)} keys in {staging_dir}")

        for key_name in staging_keys:
            # Read file from S3
            response_str = s3_hook.read_key(key_name, bucket_name)
            response_dict = json.loads(response_str)
            logging.info(f"Found file {bucket_name}/{key_name} containing {len(response_dict)} records")

            # Upload file contents to database
            messages_table, media_attributes = get_updates_for_message_tables(response_dict)
            media_table = standardize_media_attributes(media_attributes)
            if len(media_table) > 0:
                insert_into_database("messages", messages_table)
                logging.info("Inserted new records into 'messages' table.")
            if len(media_table) > 0:
                insert_into_database("messages_media", media_table, ignore_dup_key="media_id")
                logging.info("Inserted new records into 'media_table' table.")

            # Move file from 'staging' dir to 'archive' dir
            try:
                response_str = s3_hook.copy_object(
                    source_bucket_key=key_name,
                    dest_bucket_key=key_name.replace(staging_dir, archive_dir),
                    source_bucket_name=bucket_name,
                    dest_bucket_name=bucket_name
                )
                logging.info(f"File {key_name} was moved to the archive directory")
            except ValueError as e:
                if "already exists" in str(e):
                    print(str(e))
                    # This catches an error that occurs when the file has previously already been copied over
                    pass

            # Delete file from 'staging' dir
            response_str = s3_hook.delete_objects(
                bucket=bucket_name,
                keys=key_name
            )
            logging.info(f"File {key_name} was deleted from the staging directory")
        return

    @task()
    def download_telegram_messages():
        """
        #### Check Telegram for new messages and save them to S3.
        """
        sql_query = """
        SELECT *
        FROM sessions
        WHERE active is True
            AND session_string IS NOT NULL
        """
        session_list = select_from_database(sql_query)
        for s in session_list:
            logging.info(f"Using account {s['phone_number']} to scrape Telegram data.")
            client = TelegramClient(StringSession(s["session_string"]), s["app_id"], s["api_hash"])
            with client:
                client.loop.run_until_complete(get_telegram_message_updates(client))
        return

    @task()
    def download_telegram_media_files():
        """
        #### Download any missing Telegram message media files
        1. Check database for messages with missing media files
        2. Download media files to S3
        3. Update database with S3 file save location
        Describe process here...
        """
        logging.info("Step 3!!! This means all steps ran sucessfully!")
        return

    first = upload_staging_files_to_database()
    second = download_telegram_messages()
    third = download_telegram_media_files()
    first >> second >> third


telegram_ingest_media = telegram_ingest_media()
