# Databricks notebook source
# MAGIC %md
# MAGIC ### Objective
# MAGIC
# MAGIC This notebook demonstrates how to integrate Slack notifications into a data loading process in a data lake environment. The main steps include:
# MAGIC 1. Simulating the reading of data from a source table.
# MAGIC 2. Incrementally saving the data into a target table.
# MAGIC 3. Sending a Slack notification to confirm the success of the data loading process.

# COMMAND ----------

# Importing necessary libraries
import requests
import json

# Slack webhook URL (replace with your actual URL)
slack_webhook_url = 'https://hooks.slack.com/services/your-webhook-url'

# Function to send a message to Slack
def send_slack_message(message):
    """
    Sends a message to a specified Slack channel using the webhook URL.
    
    Args:
        message (str): The message text to send to Slack.
    """
    payload = {'text': message}
    response = requests.post(slack_webhook_url, data=json.dumps(payload),
                             headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise Exception(f'Request to Slack returned an error {response.status_code}, the response is:\n{response.text}')

# Example: Simulating data reading (replace with your actual data loading logic)
df = spark.sql("SELECT * FROM your_database.your_table")

# Logic to save the data incrementally in a target schema
df.write.mode("append").saveAsTable("your_schema.your_target_table")

# Sending an alert to Slack
send_slack_message("Data successfully loaded into the target table.")
