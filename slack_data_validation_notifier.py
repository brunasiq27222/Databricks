# Databricks Notebook - Example of Sending Alerts to Slack

# This notebook demonstrates how to check data consistency in a data lake environment and send alerts to Slack if inconsistencies are identified. 
# It includes:
# - Generating CSV files with inconsistent data.
# - Sending messages to Slack with alerts.
# - Attaching generated files as part of the Slack message.

# Importing necessary libraries
from slack_sdk.errors import SlackApiError
from slack_sdk import WebClient
import pandas as pd

# Paths for generated files
FILE_PATH = "FileStore\\tables\\data-alerts.csv" 
FILE_PATH2 = "FileStore\\tables\\data-alerts2.csv"
CHANNEL = '#example_channel'  # Slack channel name
channel_id = 'example_channel_id'  # Slack channel ID

# Function to save inconsistent data to a CSV file
def save_file():
    # Simulating data extracted from the data environment
    data = {'column1': [1, 2], 'column2': ['value1', 'value2']}
    df_file = pd.DataFrame(data)
    df_file.to_csv(FILE_PATH, sep=';', index=False)

# Function to save another set of inconsistent data to a CSV file
def save_file2():
    # Simulating another data extraction
    data = {'column1': [3, 4], 'column2': ['value3', 'value4']}
    df_file2 = pd.DataFrame(data)
    df_file2.to_csv(FILE_PATH2, sep=';', index=False)

# Function to check for inconsistencies in the data
def is_missing_data():
    # Simulating data validation
    errors = {'ok': True}
    inconsistencies = [{'id': '123'}, {'id': '456'}]  # Example of inconsistencies

    if inconsistencies:
        errors = {
            'ok': False,
            'message': "There are inconsistencies in the processed data.",
            'details': inconsistencies
        }
        save_file()
        save_file2()
    return errors

# Building the Slack message
def build_block(validation): 
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": validation['message']}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "Please review the inconsistencies in the generated file."}
            ]
        },
    ]
    return blocks

# Function to send a message to Slack
def send_slack_message(slack_token, message=None, blocks=None):
    slack_client = WebClient(token=slack_token)
    try:
        result = slack_client.chat_postMessage(channel=CHANNEL, blocks=blocks)
        print(result['ok'])

        if result['ok']:
            with open(FILE_PATH, "rb") as file:
                result = slack_client.files_upload(
                    channels=CHANNEL,
                    file=FILE_PATH,
                    title='data-alerts.csv',
                    filetype='csv'
                )
                print(result['ok'])

            with open(FILE_PATH2, "rb") as file:
                result = slack_client.files_upload(
                    channels=CHANNEL,
                    file=FILE_PATH2,
                    title='data-alerts2.csv',
                    filetype='csv'
                )
                print(result['ok'])
    except SlackApiError as e:
        print(f"Error: {e}")

# Execute the validation and send an alert if necessary
slack_token = 'your_token_here'  # Use a secure token
validation = is_missing_data()
if not validation['ok']:
    send_slack_message(slack_token=slack_token, blocks=build_block(validation))
