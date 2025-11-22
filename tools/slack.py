import os

from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()

slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
slack_channel_name = os.getenv("SLACK_CHANNEL_NAME")

client = WebClient(token=slack_bot_token)

def get_channel_id(channel_name):
    """Get the Slack channel ID for the specified channel name."""
    channels = (
            client.conversations_list().get("channels", []) +
            client.conversations_list(types="private_channel").get("channels", [])
    )
    for channel in channels:
        if channel["name"] == channel_name:
            return channel["id"]
    return None

slack_channel_id = get_channel_id(slack_channel_name)
if not slack_channel_id:
    exit("No slack channel found.")


def delete_all_bot_messages():
    """Delete all messages sent by the bot in the specified Slack channel."""
    history = client.conversations_history(channel=slack_channel_id)
    for message in history["messages"]:
        if message.get("bot_id"):
            client.chat_delete(channel=slack_channel_id, ts=message["ts"])


def send_file_to_slack(filename, file):
    """Send a file to Slack."""
    client.files_upload_v2(
        channel=slack_channel_id,
        filename=filename,
        file=file,
    )