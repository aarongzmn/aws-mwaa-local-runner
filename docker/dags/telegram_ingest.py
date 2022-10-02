# DAG exhibiting task flow paradigm in airflow 2.0
# https://airflow.apache.org/docs/apache-airflow/2.0.2/tutorial_taskflow_api.html
# Modified for our use case
import os
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
from telethon.sync import TelegramClient
from telethon.utils import get_display_name
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerChannel

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

config = {
    "scrape_options": {
        "groups": True,
        "channels": True,
        "media": False
    },
    "s3_bucket": "telegram-scrape",
    "staging_dir": "messages/staging/",
    "archive_dir": "messages/archive/",
    "media_working_dir": "telegram-scrape/media",
    "max_scrape_dialogs": False,
    "max_scrape_messages": False,
    "save_dialogs_to_db": True,
    "save_messages_to_db": True,
    "save_messages_to_s3": True,
    "save_messages": True,
    "save_messages_chunk_size": 25000
}

default_args = {
    "owner": "Aaron Guzman",
    "email": "aguzman@adl.org",
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
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


def get_updates_for_dialog_tables(dialog_data_list) -> dict:
    """Parse and transform Telethon scraped data to be updated in database.
    Returns:
        dict: Dictionary containing keys for each table that is to be updated.
        The value of each key is another dictionary containing keys names that
        match the column names in each table.
    """
    dialog_tables = {
        "dialogs": [],
        "dialog_media": [],
        "dialog_updates": [],
    }
    for dialog_data in dialog_data_list:
        dialogs = {}
        dialogs["id"] = dialog_data["entity"]["id"]
        dialogs["access_hash"] = dialog_data["entity"]["access_hash"]
        dialogs["is_user"] = dialog_data["is_user"]
        dialogs["is_group"] = dialog_data["is_group"]
        dialogs["is_channel"] = dialog_data["is_channel"]
        dialogs["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")
        dialog_tables["dialogs"].append(dialogs)

        dialog_updates = {}
        dialog_updates["dialog_id"] = dialog_data["entity"]["id"]
        dialog_updates["title"] = dialog_data["entity"]["title"]
        dialog_updates["broadcast"] = dialog_data["entity"]["broadcast"]
        dialog_updates["verified"] = dialog_data["entity"]["verified"]
        dialog_updates["megagroup"] = dialog_data["entity"]["megagroup"]
        dialog_updates["restricted"] = dialog_data["entity"]["restricted"]
        dialog_updates["scam"] = dialog_data["entity"]["scam"]
        dialog_updates["has_link"] = dialog_data["entity"]["has_link"]
        dialog_updates["slowmode_enabled"] = dialog_data["entity"]["slowmode_enabled"]
        dialog_updates["fake"] = dialog_data["entity"]["fake"]
        dialog_updates["gigagroup"] = dialog_data["entity"]["gigagroup"]
        dialog_updates["noforwards"] = dialog_data["entity"]["noforwards"]
        dialog_updates["username"] = dialog_data["entity"]["username"]
        # dialog_updates["restriction_reason"] = dialog_data["entity"]["restriction_reason"] Error: 'is of type json[] but expression is of type text[]'
        dialog_updates["participants_count"] = dialog_data["entity"]["participants_count"]
        dialog_updates["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+0000")

        dialog_media = dialog_data["entity"]["photo"]
        if "photo_id" in dialog_media:
            dialog_media["dialog_id"] = dialog_data["entity"]["id"]
            dialog_tables["dialog_media"].append(dialog_media)
            dialog_updates["photo_id"] = dialog_media["photo_id"]
        else:
            dialog_media["dialog_id"] = None
            dialog_updates["photo_id"] = None
        dialog_tables["dialog_updates"].append(dialog_updates)
    return dialog_tables


def get_updates_for_message_tables(telegram_messages) -> (list, list):
    """Transform Telegram messages data and create lists that
    will be used to update the 'messages' and 'media' tables
    in the telegram database.

    The input for this function are the S3 files stored in the
    telegram-scrape/messages/staging directory. This data is
    parsed and split into two lists:
        1. messages_table: Used to update the 'messages' table
        2. media_table: Used to update the 'media' table
    """
    messages_table = [] # Updates for 'messages' table
    media_attributes = [] # List of media attributes that need to be formatted before inserting into database
    for i in range(len(telegram_messages)):
        message = telegram_messages[i]
        message_data = {}
        try:
            
            message_data["dialog_id"] = message["dialog_id"]
        except:
            # renamed database column from subscription_id to dialog_id, older S3 files may still have previous name
            message_data["dialog_id"] = message["subscription_id"]
        message_data["message_id"] = message["id"]
        message_data["date"] = message["date"]
        message_data["from_id"] = message["from_id"]
        message_data["message"] = message["message"]
        message_data["pinned"] = message["pinned"]
        message_data["post_author"] = message["post_author"]
        message_data["private_url"] = message["private_url"]
        message_data["public_url"] = message["public_url"]
        message_data["sender_username"] = message["sender"]
        reply_to = message["reply_to"]
        if reply_to and "reply_to_msg_id" in reply_to:
            message_data["reply_to_msg_id"] = reply_to["reply_to_msg_id"]
        else:
            message_data["reply_to_msg_id"] = None
        if reply_to and "reply_to_peer_id" in reply_to:
            message_data["reply_to_peer_id"] = reply_to["reply_to_peer_id"]
        else:
            message_data["reply_to_peer_id"] = None
        fwd_from = message["fwd_from"]
        if fwd_from:
            if fwd_from["from_id"] and "channel_id" in fwd_from["from_id"]:
                message_data["fwd_from_channel_id"] = fwd_from["from_id"]["channel_id"]
            else:
                message_data["fwd_from_channel_id"] = None
            if fwd_from["from_id"] and "user_id" in fwd_from["from_id"]:
                message_data["fwd_from_user_id"] = fwd_from["from_id"]["user_id"]
            else:
                message_data["fwd_from_user_id"] = None
            message_data["fwd_from_date"] = message["fwd_from"]["date"]
            message_data["fwd_from_from_name"] = message["fwd_from"]["from_name"]
            message_data["fwd_from_message_id"] = message["fwd_from"]["channel_post"]
            message_data["fwd_from_post_author"] = message["fwd_from"]["post_author"]
        else:
            message_data["fwd_from_channel_id"] = None
            message_data["fwd_from_user_id"] = None
            message_data["fwd_from_date"] = None
            message_data["fwd_from_from_name"] = None
            message_data["fwd_from_message_id"] = None
            message_data["fwd_from_post_author"] = None

        # Add webpage media to 'message' and extract any media to be placed in the 'message_media' table
        if message["media"] and "webpage" in message["media"] and "url" in message["media"]["webpage"]:
            webpage_data = message["media"]["webpage"]
            message_data["webpage_url"] = webpage_data["url"]
            message_data["webpage_type"] = webpage_data["type"]
            message_data["webpage_site_name"] = webpage_data["site_name"]
            message_data["webpage_title"] = webpage_data["title"]
            message_data["webpage_description"] = webpage_data["description"]
            message_data["author"] = webpage_data["author"]
            if "photo" in webpage_data and webpage_data["photo"] and "id" in webpage_data["photo"]:
                message_data["webpage_photo_media_id"] = webpage_data["photo"]["id"]
                webpage_data["photo"]["media_type"] = "webpage_photo"
                webpage_data["photo"]["message_id"] = message["id"]
                try:
                    webpage_data["photo"]["dialog_id"] = message["dialog_id"]
                except:
                # renamed database column from subscription_id to dialog_id, older files may still have previous name
                    webpage_data["photo"]["dialog_id"] = message["subscription_id"]
                media_attributes.append(message["media"]["webpage"]["photo"])
            else:
                message_data["webpage_photo_media_id"] = None

            if "document" in webpage_data and webpage_data["document"] and "id" in webpage_data["document"]:
                message_data["webpage_document_media_id"] = webpage_data["document"]["id"]
                webpage_data["document"]["media_type"] = "webpage_document"
                webpage_data["document"]["message_id"] = message["id"]
                try:
                    webpage_data["document"]["dialog_id"] = message["dialog_id"]
                except:
                # renamed database column from subscription_id to dialog_id, older files may still have previous name
                    webpage_data["document"]["dialog_id"] = message["subscription_id"]
                media_attributes.append(message["media"]["webpage"]["document"])
            else:
                message_data["webpage_document_media_id"] = None
        else:
            message_data["webpage_url"] = None
            message_data["webpage_type"] = None
            message_data["webpage_site_name"] = None
            message_data["webpage_title"] = None
            message_data["webpage_description"] = None
            message_data["author"] = None
            message_data["webpage_photo_media_id"] = None
            message_data["webpage_document_media_id"] = None

        if message["media"] and "photo" in message["media"] and message["media"]["photo"]:
            message_data["media_id"] = message["media"]["photo"]["id"]
            message["media"]["photo"]["media_type"] = "photo"
            message["media"]["photo"]["message_id"] = message["id"]
            try:
                message["media"]["photo"]["dialog_id"] = message["dialog_id"]
            except:
            # renamed database column from subscription_id to dialog_id, older files may still have previous name
                message["media"]["photo"]["dialog_id"] = message["subscription_id"]
            media_attributes.append(message["media"]["photo"])

        elif message["media"] and "document" in message["media"]:
            message_data["media_id"] = message["media"]["document"]["id"]
            message["media"]["document"]["media_type"] = "document"
            message["media"]["document"]["message_id"] = message["id"]
            try:
                message["media"]["document"]["dialog_id"] = message["dialog_id"]
            except:
            # renamed database column from subscription_id to dialog_id, older files may still have previous name
                message["media"]["document"]["dialog_id"] = message["subscription_id"]
            media_attributes.append(message["media"]["document"])
        else:
            message_data["media_id"] = None
        messages_table.append(message_data)

    return messages_table, media_attributes


def standardize_media_attributes(media_attributes) -> list:
    media_table = []
    for i in range(len(media_attributes)):
        m = {}
        attributes = media_attributes[i].get("attributes")
        if attributes:
            duration_key = [a for a in attributes if "duration" in a]
            if duration_key:
                m["media_duration"] = duration_key[0]["duration"]
            else:
                m["media_duration"] = None
            video_width_key = [a for a in attributes if "w" in a]
            if video_width_key:
                m["video_width"] = video_width_key[0]["w"]
            else:
                m["video_width"] = None
            video_height_key = [a for a in attributes if "h" in a]
            if video_height_key:
                m["video_height"] = video_height_key[0]["h"]
            else:
                m["video_height"] = None
            file_name_key = [a for a in attributes if "file_name" in a]
            if file_name_key:
                m["media_filename"] = file_name_key[0]["file_name"]
            else:
                m["media_filename"] = None
        else:
            m["media_duration"] = None
            m["video_width"] = None
            m["video_height"] = None
            m["media_filename"] = None
        m["media_id"] = media_attributes[i].get("id")
        m["access_hash"] = media_attributes[i].get("access_hash")
        m["file_reference"] = media_attributes[i].get("file_reference")
        m["date"] = media_attributes[i].get("date")
        m["dc_id"] = media_attributes[i].get("dc_id")
        m["has_stickers"] = media_attributes[i].get("has_stickers")
        m["mime_type"] = media_attributes[i].get("mime_type")
        m["size"] = media_attributes[i].get("size")
        m["media_type"] = media_attributes[i].get("media_type")
        m["message_id"] = media_attributes[i].get("message_id")
        try:
            m["dialog_id"] = media_attributes[i]["dialog_id"]
            m["dialog_message_id"] = str(media_attributes[i]["dialog_id"]) + str(media_attributes[i].get("message_id"))
        except:
            m["dialog_id"] = media_attributes[i]["subscription_id"]
            m["dialog_message_id"] = str(media_attributes[i]["subscription_id"]) + str(media_attributes[i].get("message_id"))
        media_table.append(m)
    return media_table


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


def cleanup_working_directory(working_dir):
    file_list = os.listdir(working_dir)
    for file in file_list:
        file_path = os.path.join(working_dir, file)
        os.unlink(file_path)


async def get_telegram_message_updates(client):
    st = time.time()
    account_dialog_list = [todict(i) async for i in client.iter_dialogs()]

    # scrape_options = config["scrape_options"]
    # if scrape_options["groups"] is False and scrape_options["channels"]:
    #     # Only scrape 'channel' dialogs
    #     api_dialog_data_list = [i for i in account_dialog_list if not i["is_group"] and i["is_channel"]]
    # elif scrape_options["groups"] and scrape_options["channels"] is False:
    #     # Only scrape 'group' dialogs
    #     api_dialog_data_list = [i for i in account_dialog_list if i["is_group"]]
    # else:
    #     # Scrape both channels and groups dialogs
    #     api_dialog_data_list = [i for i in account_dialog_list if not i["is_user"]]
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
            if len(updates_for_dialog_tables["dialog_media"]) > 0:
                insert_into_database("dialog_media", updates_for_dialog_tables["dialog_media"], ignore_dup_key="photo_id")
            if len(updates_for_dialog_tables["dialog_updates"]) > 0:
                insert_into_database("dialog_updates", updates_for_dialog_tables["dialog_updates"])
        except:
            logging.info(len(updates_for_dialog_tables["dialogs"]))
            logging.info(updates_for_dialog_tables["dialogs"].keys())

    if config["save_messages_to_s3"] is False:
        logging.warning(f"Scrape settings set to 'save_messages_to_s3'={config['save_messages_to_s3']}, this will disable saving message files to S3.")

    scrape_options = config["scrape_options"]
    if scrape_options["groups"] is False and scrape_options["channels"]:
        # Scrape Channels, NOT Groups and NOT Users
        sql_query = """
        SELECT *
        FROM dialogs
        WHERE is_channel is True
            AND is_group is False
        """
    elif scrape_options["groups"] and scrape_options["channels"] is False:
        # Scrape Groups, NOT Channels and NOT Users
        sql_query = """
        SELECT *
        FROM dialogs
        WHERE is_group is True
        """
    else:
        # Scrape Channels and Groups, NOT Users
        sql_query = """
        SELECT *
        FROM dialogs
        WHERE is_user is False
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
        db_dialog_id = db_dialog_data["id"]
        db_dialog_access_hash = int(db_dialog_data["access_hash"])
        dialog_entity = await client.get_entity(InputPeerChannel(db_dialog_id, db_dialog_access_hash))

        logging.info(f"Starting message scrape for: '{dialog_name}', '{dialog_username}' | '{dialog_id}'")
        sql_query = f"""
        SELECT MAX(message_id)
        FROM messages
        WHERE dialog_id = {dialog_id}
        """
        last_db_message_id = select_from_database(sql_query)[0].get("max")
        if last_db_message_id is None:
            last_db_message_id = 1
        scrape_to = await get_newest_message_id_from_telegram_api(dialog_entity, client)
        if scrape_to is None:
            logging.info(f"No messages found for diolog: '{dialog_username}' | '{dialog_id}'")
            continue
        if last_db_message_id >= scrape_to:
            logging.info("Database is up to date. No scrape needed.")
            continue
        scrape_from = 0
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
            async for message in client.iter_messages(dialog_entity, min_id=scrape_from, reverse=True, limit=limit):
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

            if config["save_messages_to_s3"] and len(message_list) > config["save_messages_chunk_size"]:
                from_id = min([i["id"] for i in message_list])
                to_id = max([i["id"] for i in message_list])
                key_name = f"messages/staging/{dialog_id}-from-{from_id}-to-{to_id}-({len(message_list)}).json"
                logging.info(f"Saving file as: {key_name}")
                string_data = json.dumps(message_list)
                hook = S3Hook('s3_aaron')
                hook.load_string(
                    string_data,
                    key=key_name,
                    bucket_name=config["s3_bucket"],
                    replace=False
                )
                message_list = []

        if config["save_messages_to_s3"]:
            from_id = min([i["id"] for i in message_list])
            to_id = max([i["id"] for i in message_list])
            key_name = f"messages/staging/{dialog_id}-from-{from_id}-to-{to_id}-({len(message_list)}).json"
            logging.info(f"Saving file as: {key_name}")
            string_data = json.dumps(message_list)
            hook = S3Hook('s3_aaron')
            hook.load_string(
                string_data,
                key=key_name,
                bucket_name=config["s3_bucket"],
                replace=False
            )
    et = time.time()
    elapsed = et - st
    logging.info(f"Process completed in {elapsed} seconds.")
    return


async def download_telegram_media(client):
    select_sql = """
    SELECT *
    FROM dialogs
    WHERE active=True
    """
    dialogs = select_from_database(select_sql)
    dialog_list = [i for i in dialogs if i["download_photos"] or i["download_videos"] or i["download_documents"] or i["download_webpage_media"]]
    media_working_dir = config["media_working_dir"]
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
                            media_path = await message.download_media(file=media_working_dir + "/" + filename)
                            try:
                                hook = S3Hook('s3_aaron')
                                hook.load_file(
                                    filename=media_path,
                                    key=media_path.replace("telegram-scrape/", ""),
                                    bucket_name=config["s3_bucket"],
                                    replace=False
                                )
                                os.unlink(media_path)
                                logging.info(f"Processed media for: {message_url}. File uploaded to S3 bucket '{config['media_working_dir']}' as '{filename}'. Removing file from '{media_path}'")
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
    start_date=days_ago(2),
    description="Telegram Pipeline",
    schedule_interval="@daily",
    catchup=False,
    tags=['telegram']
)
def telegram_ingest():
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
        bucket_name = config["s3_bucket"]
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
                insert_into_database("messages_media", media_table, ignore_dup_key="media_id")
                logging.info("Inserted new records into 'media_table' table.")

            if len(messages_table) > 0:
                insert_into_database("messages", messages_table)
                logging.info("Inserted new records into 'messages' table.")

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
        """
        # Create working directory if it doesn't already exist
        if config["media"]:
            media_working_dir = config["media_working_dir"]
            Path(media_working_dir).mkdir(parents=True, exist_ok=True)
            cleanup_media_working_directory(media_working_dir)

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
                client.loop.run_until_complete(download_telegram_media(client))
            cleanup_media_working_directory(media_working_dir)
        else:
            logging.info("Media scrape config setting set to False. No media will be scraped.")
            return




    first = upload_staging_files_to_database()
    second = download_telegram_messages()
    third = download_telegram_media_files()
    fourth = upload_staging_files_to_database()
    first >> second >> third >> fourth


telegram_ingest = telegram_ingest()
