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
    "max_scrape_subs": None,
    "max_scrape_messages": None,
    "save_subscriptions": True,
    "save_messages": True,
    "save_messages_chunk_size": 50000
}
if config['max_scrape_subs']:
    logging.warning(f"Scrape settings set to max {config['max_scrape_subs']} subscriptions.")
if config["max_scrape_messages"]:
    logging.warning(f"Scrape settings set to max {config['max_scrape_messages']=} messages.")


def select_from_database(sql_query):
    """SELECT FROM database and return query results
    """
    conn = PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(sql_query)
        query_response = curs.fetchall()
        response_list = [dict(row) for row in query_response]
    return response_list


def insert_into_database(table_name: str, record_list: [dict], ignore_duplicates: bool = False) -> list:
    """Bulk INSERT INTO database (1000 rows at a time). Input list shoud be items in dictionary format
    """
    col_names = ", ".join(record_list[0].keys())
    insert_values = [tuple(e.values()) for e in record_list]
    with PostgresHook(postgres_conn_id="coe-postgres-aaron").get_conn() as conn:
        with conn.cursor() as curs:
            if ignore_duplicates:
                print("Ignoring duplicate values.")
                sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s ON CONFLICT (id) DO NOTHING"
            else:
                sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
            psycopg2.extras.execute_values(curs, sql, insert_values, page_size=1000)
    return


def get_updates_for_subscription_tables(sub):
    subscription_tables = {}

    subscriptions = {}
    subscriptions["id"] = sub["entity"]["id"]
    subscriptions["access_hash"] = sub["entity"]["access_hash"]
    subscriptions["is_user"] = sub["is_user"]
    subscriptions["is_group"] = sub["is_group"]
    subscriptions["is_channel"] = sub["is_channel"]
    subscriptions["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")
    subscription_tables["subscriptions"] = subscriptions

    subscription_updates = {}
    subscription_updates["subscription_id"] = sub["entity"]["id"]
    subscription_updates["title"] = sub["entity"]["title"]
    subscription_updates["broadcast"] = sub["entity"]["broadcast"]
    subscription_updates["verified"] = sub["entity"]["verified"]
    subscription_updates["megagroup"] = sub["entity"]["megagroup"]
    subscription_updates["restricted"] = sub["entity"]["restricted"]
    subscription_updates["scam"] = sub["entity"]["scam"]
    subscription_updates["has_link"] = sub["entity"]["has_link"]
    subscription_updates["slowmode_enabled"] = sub["entity"]["slowmode_enabled"]
    subscription_updates["fake"] = sub["entity"]["fake"]
    subscription_updates["gigagroup"] = sub["entity"]["gigagroup"]
    subscription_updates["noforwards"] = sub["entity"]["noforwards"]
    subscription_updates["username"] = sub["entity"]["username"]
    # subscription_updates["restriction_reason"] = sub["entity"]["restriction_reason"] Error: 'is of type json[] but expression is of type text[]'
    subscription_updates["participants_count"] = sub["entity"]["participants_count"]
    subscription_updates["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")

    subscription_media = sub["entity"]["photo"]
    if "photo_id" in subscription_media:
        subscription_media["subscription_id"] = sub["entity"]["id"]
        subscription_tables["subscription_media"] = subscription_media
        subscription_updates["photo_id"] = subscription_media["photo_id"]
    else:
        subscription_media["subscription_id"] = None
        subscription_updates["photo_id"] = None
    subscription_tables["subscription_updates"] = subscription_updates
    return subscription_tables


async def get_newest_message_id_from_telegram_api(subscription_name, client):
    """Get the last message id of a telegram group from telethon api

    :param subscription_name:
    :return last_message_id:
    """
    try:
        async for message in client.iter_messages(subscription_name, limit=1):
            last_message_id = message.id
            return last_message_id
    except Exception as e:
        print(e)


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
        subscriptions = subscriptions[0:config["max_scrape_subs"]]

    subscription_table, subscription_updates_table, subscription_media_table = get_updates_for_subscription_tables(subscriptions)

    print(f"Found {len(subscription_table)} updates for the subscriptions table, inserting now...")
    insert_into_database("subscriptions", subscription_table, True)
    print("Inserted records into 'subscriptions' table.")

    print(f"Found {len(subscription_updates_table)} updates for the subscription_updates table, inserting now...")
    insert_into_database("subscription_updates", subscription_updates_table)
    print("Inserted records into 'subscription_updates' table.")

    print(f"Found {len(subscription_media_table)} updates for the subscription_media table, inserting now...")
    insert_into_database("subscription_media", subscription_media_table)
    print("Inserted records into 'subscription_media' table.")

    if config["save_subscriptions"]:
        ts = str(datetime.utcnow().timestamp()).split(".")[0]
        key_name = f"subscriptions/{ts}.json"
        json_bytes = json.dumps(subscriptions).encode('utf-8')
        hook = S3Hook('s3_aaron')
        hook.load_file_obj(
            json_bytes,
            key_name,
            bucket_name=config["root_dir"],
            replace=False
        )


    sql_query = """
    SELECT *
    FROM subscriptions
    """
    db_subscriptions = select_from_database(sql_query)
    for sub in subscriptions:
        try:
            subscription_table, subscription_updates_table, subscription_media_table = get_updates_for_subscription_tables(sub)
            subscription_name = sub["name"]
            subscription_username = sub["entity"].get("username")
            subscription_id = sub["entity"]["id"]
            subs_database_info = [i for i in db_subscriptions if i["id"] == subscription_id]
            if not subs_database_info:
                logging.info(f"No database data found for subscription: {subscription_name}, {subscription_username}, {subscription_id}")
                
                continue
            else:
                sub_db_info = subs_database_info[0]
                if sub_db_info["active"] is False:
                    # active = False database parameter means the subscription should not be scraped
                    continue
                else:
                    sql_query = f"""
                    SELECT MAX(id)
                    FROM messages
                    WHERE subscription_id = {subscription_id}
                    """
                    last_db_message_id = select_from_database(sql_query).get("max")
                    scrape_to = await get_newest_message_id_from_telegram_api(subscription_name, client)
                    if config["max_scrape_messages"]:
                        limit = config["max_scrape_messages"]
                        scrape_from = max(scrape_to - limit, 0)
                    else:
                        limit = 1000
                        if last_db_message_id:
                            scrape_from = last_db_message_id
                        else:
                            scrape_from = 1

            print(f"Found sub metadata for: {subscription_name}, {subscription_username}, {subscription_id} where most recent message ID is: {scrape_to}.")
        except:
            logging.error(f"Failed to get sub metadata for: {subscription_name}, {subscription_username}, {subscription_id}")
            continue
        message_list = []
        while scrape_from < scrape_to:
            async for message in client.iter_messages(subscription_username, min_id=scrape_from, reverse=True, limit=limit):
                # https://docs.telethon.dev/en/stable/modules/custom.html#telethon.tl.custom.message.Message
                scrape_from = message.id
                # if message.media:
                #     media_dir = ""
                # else:
                #     media_dir = None

                # if sub["is_group"]:
                #     message_attribs = get_message_attributes_from_group(message)
                # else:
                #     message_attribs = get_message_attributes_from_channel(message)

                # # Enrich data (Add additional attributes)
                # message_dict = todict(message_attribs)
                # message_dict["media_dir"] = media_dir
                # message_dict["subscription_id"] = subscription_id
                # message_dict["sender"] = get_display_name(message.sender)
                # private_url = f"https://t.me/c/{subscription_id}/{message.id}"
                # message_dict["private_url"] = private_url
                # public_url = f"https://t.me/{subscription_username}/{message.id}"
                # message_dict["public_url"] = public_url
                # message_list.append(message_dict)
            print(scrape_from, scrape_to)

        #     if config["save_messages"] and len(message_list) > config["save_messages_chunk_size"]:
        #         from_id = min([i["id"] for i in message_list])
        #         to_id = max([i["id"] for i in message_list])
        #         key_name = f"messages/staging/{subscription_id}-from-{from_id}-to-{to_id}.json"
        #         string_data = json.dumps(message_list)
        #         hook = S3Hook('s3_aaron')
        #         hook.load_string(
        #             string_data,
        #             key=key_name,
        #             bucket_name=config["root_dir"],
        #             replace=False
        #         )
        #         message_list = []

        # if config["save_messages"]:
        #     from_id = min([i["id"] for i in message_list])
        #     to_id = max([i["id"] for i in message_list])
        #     key_name = f"messages/staging/{subscription_id}-from-{from_id}-to-{to_id}.json"
        #     string_data = json.dumps(message_list)
        #     hook = S3Hook('s3_aaron')
        #     hook.load_string(
        #         string_data,
        #         key=key_name,
        #         bucket_name=config["root_dir"],
        #         replace=False
        #     )
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
        sql_query = """
        SELECT *
        FROM sessions
        WHERE active is True
            AND session_string IS NOT NULL
        """
        session_list = select_from_database(sql_query)
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
