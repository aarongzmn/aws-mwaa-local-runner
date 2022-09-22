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
# logging.basicConfig(level=logging.CRITICAL)
# logging.basicConfig(level=logging.ERROR)
# logging.basicConfig(level=logging.WARNING)
logging.basicConfig(level=logging.INFO)
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

config = {
    "scrape_options": {
        "groups": False,
        "channels": True,
        "media": False
    },
    "test_account": False,
    "root_dir": "telegram-scrape",
    "max_scrape_subs": 25,
    "max_scrape_messages": None,
    "save_subscriptions": True,
    "save_messages": True,
    "save_messages_chunk_size": 50000
}


def get_session_list_from_database():
    """Get a list of sessions that will be used to scrape Telegram

    Returns:
        list: List of account sessions that will be used to scrape Telegram.
    """
    conn = PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(
            """
            SELECT *
            FROM sessions
            WHERE active is True
                AND session_string IS NOT NULL
            """
        )
        query_result = curs.fetchall()
        result_list = [dict(row) for row in query_result]
    return result_list


def get_latest_id_for_each_sub(channel_id):
    """Query database for list of latest message_id for each subscription
    """
    max_id_dict = {}
    latest_id = max_id_dict.get(channel_id, 0)
    return latest_id


async def get_last_message_id(sub_name, client):
    """Get the last message id of a telegram group from telethon api

    :param sub_name:
    :return last_message_id:
    """
    try:
        async for message in client.iter_messages(sub_name, limit=1):
            last_message_id = message.id
            return last_message_id
    except Exception as e:
        print(e)


def get_message_attributes_from_group(message):
    pass


def get_message_attributes_from_channel(message):
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
    all_subs = [todict(i) async for i in client.iter_dialogs()]
    scrape_options = config["scrape_options"]
    if scrape_options["groups"] is False and scrape_options["channels"]:
        # Only scrape channels
        subscriptions = [i for i in all_subs if not i["is_group"] and i["is_channel"]]
        print(f"Found {len(subscriptions)} total channels.")

    elif scrape_options["groups"] and scrape_options["channels"] is False:
        # Only scrape groups/subs
        subscriptions = [i for i in all_subs if i["is_group"]]
        print(f"Found {len(subscriptions)} total groups/subs.")
    else:
        # Scrape both channels and groups/subs
        subscriptions = [i for i in all_subs if not i["is_user"]]
        print(f"Found {len(subscriptions)} total channels and groups/subs.")

    if config["max_scrape_subs"]:
        # logging.warning(f"Scrape settings set to max {config['max_scrape_subs']} subscriptions.")
        subscriptions = subscriptions[0:config["max_scrape_subs"]]

    # if config["save_subscriptions"]:
    #     ts = str(datetime.utcnow().timestamp()).split(".")[0]
    #     key_name = f"subscriptions/{ts}.json"
    #     json_bytes = json.dumps(subscriptions).encode('utf-8')
    #     hook = S3Hook('s3_aaron')
    #     hook.load_file_obj(
    #         json_bytes,
    #         key_name,
    #         bucket_name=config["root_dir"],
    #         replace=False
    #     )
    for sub in subscriptions:
        try:
            channel_id = sub["dialog"]["peer"]["channel_id"]
            sub_name = sub["name"]
            sub_username = sub["entity"]["username"]
            if sub_username is None or sub_username == "":
                logging.error(f"No channel username found for {channel_id}")
                continue

            max_id = await get_last_message_id(sub_name, client)
            if config["max_scrape_messages"]:
                # logging.warning(f"Scrape settings set to max {config['max_scrape_messages']} messages.")
                limit = config["max_scrape_messages"]
                min_id = max(max_id - limit, 0)
            else:
                min_id = get_latest_id_for_each_sub(channel_id)
                limit = 1000

            print(f"Found sub metadata for: {sub_username}, {channel_id}, contains {max_id} messages.")
        except:
            logging.error(f"Failed to get sub metadata for: {sub_username}, {channel_id}")
            continue
        message_list = []
        while min_id < max_id:
            async for message in client.iter_messages(sub_username, min_id=min_id, reverse=True, limit=limit):
                # https://docs.telethon.dev/en/stable/modules/custom.html#telethon.tl.custom.message.Message
                min_id = message.id
                if message.media:
                    media_dir = ""
                else:
                    media_dir = None

                if sub["is_group"]:
                    message_attribs = get_message_attributes_from_group(message)
                else:
                    message_attribs = get_message_attributes_from_channel(message)

                # Enrich data (Add additional attributes)
                message_dict = todict(message_attribs)
                message_dict["media_dir"] = media_dir
                message_dict["subscription_id"] = channel_id
                message_dict["sender"] = get_display_name(message.sender)
                private_url = f"https://t.me/c/{channel_id}/{message.id}"
                message_dict["private_url"] = private_url
                public_url = f"https://t.me/{sub_username}/{message.id}"
                message_dict["public_url"] = public_url
                message_list.append(message_dict)
            print(min_id, max_id)

            if config["save_messages"] and len(message_list) > config["save_messages_chunk_size"]:
                from_id = min([i["id"] for i in message_list])
                to_id = max([i["id"] for i in message_list])
                key_name = f"messages/staging/{channel_id}-from-{from_id}-to-{to_id}.json"
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
            key_name = f"messages/staging/{channel_id}-from-{from_id}-to-{to_id}.json"
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
    print(f"Process completed in {elapsed} seconds.")


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['telegram'])
def telegram_ingest():
    """
    ### Telegram Pipeline
    This DAG using the TaskFlow API to create a Telegram pipeline.
    (https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    @task()
    def upload_staging_files_to_database() -> list:
        """
        #### Read S3 files and upload to database
        1. Check the S3 bucket 'staging' directory for new files.
        2. Parse files and upload to database
        """
        bucket_name = config["root_dir"]
        prefix = "messages/staging"
        hook = S3Hook('s3_aaron')
        staging_files = hook.list_keys(
            bucket_name=bucket_name,
            prefix=prefix
        )
        print(staging_files)
        return

    @task()
    def download_telegram_messages():
        """
        #### Check Telegram for new messages and save them to S3.
        Describe process here...
        """
        session_list = get_session_list_from_database()
        for s in session_list:
            print(f"Using account {s['phone_number']} to scrape Telegram data.")
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
        print("Step 3!!! This means all steps ran sucessfully!")

        return

    upload_staging_files_to_database()
    download_telegram_messages()
    download_telegram_media_files()


telegram_ingest = telegram_ingest()
