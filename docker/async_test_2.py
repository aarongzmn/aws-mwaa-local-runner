import os
import json
import time
from datetime import datetime
import asyncio
import psycopg2
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.utils import get_display_name
from telethon.tl.types import InputPeerChannel, Message, MessageActionChannelCreate, MessageActionChatEditPhoto, MessageActionPinMessage, MessageActionChatEditTitle, MessageActionPaymentSent, MessageActionGroupCall

from psycopg2.extras import RealDictCursor
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
from pprint import pprint
import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)



def select_from_database(select_sql: str):
    pg_connection = {
        'dbname': "telegram",
        'user': os.getenv("ADL_CEO_POSTGRES_USERNAME"),
        'password': os.getenv("ADL_CEO_POSTGRES_PASSWORD"),
        'port': os.getenv("ADL_CEO_POSTGRES_PORT"),
        'host': os.getenv("ADL_CEO_POSTGRES_ENDPOINT")
    }
    conn = psycopg2.connect(**pg_connection)
    with conn.cursor(cursor_factory=RealDictCursor) as curs:
        curs.execute(select_sql)
        query_result = curs.fetchall()
    result_dict = [dict(row) for row in query_result]
    return result_dict


def cleanup_working_directory(working_dir):
    file_list = os.listdir(working_dir)
    for file in file_list:
        file_path = os.path.join(working_dir, file)
        os.unlink(file_path)


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


async def get_newest_message_id_from_telegram_api(dialog_name, client):
    """Get the last message id of a telegram group from telethon api

    :param dialog_name:
    :return last_message_id:
    """
    async for message in client.iter_messages(dialog_name, limit=1):
        last_message_id = message.id
        return last_message_id


async def main():
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
    
    async with client:
        st = time.time()
        jsonl_messages = "telegram-scrape/messages/archive.jsonl"
        jsonl_actions = "telegram-scrape/messages/actions.jsonl"
        
        query_string = """
        SELECT *
        FROM dialogs
        WHERE is_user is False
        """
        dialogs_list = select_from_database(query_string)
        for d in dialogs_list:
            # dialog_id = 1414255764
            # dialog_access_hash = 7716041229790944475
            dialog_id = d["id"]
            dialog_access_hash = int(d["access_hash"])
            if d["is_group"]:
                dialog_type = d["group"]
            else:
                dialog_type = d["channel"]
            dialog_entity = await client.get_entity(InputPeerChannel(dialog_id, dialog_access_hash))
            scrape_from = 0
            scrape_to = await get_newest_message_id_from_telegram_api(dialog_entity, client)
            if not scrape_to:
                logging("No messages in this dialog")
                scrape_to = 0
            logging.info(f"{dialog_id}, {scrape_to}")
            messages = await client.get_messages(dialog_entity, min_id=scrape_from, reverse=True)
            print(messages.total)
            break


            # async for message in client.iter_messages(dialog_entity, min_id=scrape_from, reverse=True):
            #     scrape_from = message.id
            #     message_url = f"https://t.me/c/{dialog_id}/{message.id}"
            #     logging.info(message_url)
            #     if isinstance(message, Message):
            #         # Append to JSON Lines file
            #         save_path = jsonl_messages
            #         message_dict = message.to_dict()
            #         message_dict["message_url"] = message_url
            #         pprint(message_dict)
            #     else:
            #         if message.action:
            #             if isinstance(message.action, MessageActionChannelCreate):
            #                 pass
            #             elif isinstance(message.action, MessageActionChatEditTitle):
            #                 pass
            #             elif isinstance(message.action, MessageActionChatEditPhoto):
            #                 pass
            #             elif isinstance(message.action, MessageActionPinMessage):
            #                 pass
            #             elif isinstance(message.action, MessageActionPaymentSent):
            #                 pass
            #             elif isinstance(message.action, MessageActionGroupCall):
            #                 pass
            #             else:
            #                 logging.info(f"Unable to identify the type of MessageAction for {message.id}: {message.action}")
            #         else:
            #             logging.info(f"Unable to identify the type of Message. No Action or Message found for {message.id}: {message.__dict__}")
            #         save_path = jsonl_actions
            #         message_dict = message

            #     with open(save_path, "a") as w:
            #         w.write(json.dumps(message_dict, default=str) + "\n")

            et = time.time()
            elapsed = et - st
            logging.info(f"Process completed in {elapsed} seconds.")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
