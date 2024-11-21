# Databricks notebook source
# MAGIC %md
# MAGIC ### Explanation:
# MAGIC
# MAGIC 1. *Import libraries*: The requests and json libraries are imported to send the message to Slack.
# MAGIC 2. *Configure webhook*: Replace slack_webhook_url with the URL of the webhook configured in Slack.
# MAGIC
# MAGIC To create a Databricks notebook that sends an alert to Slack when data is loaded, you need to follow these steps:
# MAGIC
# MAGIC 1. *Configure a webhook in Slack*.
# MAGIC 2. *Create the notebook in Databricks*.
# MAGIC 3. *Write code to load the data*.
# MAGIC 4. *Send the alert to Slack using the webhook*.
# MAGIC
# MAGIC Step 1: Configure a webhook in Slack
# MAGIC
# MAGIC 1. Go to Slack and navigate to *Apps*.
# MAGIC 2. Search for *Incoming Webhooks* and add it to your workspace.
# MAGIC 3. Configure a new webhook and copy the generated URL. This URL will be used to send messages to Slack.
# MAGIC
# MAGIC
# MAGIC 3. *Function to send message*: The send_slack_message function is used to send the message to Slack.
# MAGIC 4. *Read data*: Data is read from the table.
# MAGIC 5. *Calculate fare_amount sum*: The sum of fare_amount is calculated using groupBy().sum("fare_amount").
# MAGIC 6. *Check sum*: If the fare_amount sum exceeds 115.00, an alert message with text and emoji will be sent to Slack.
# MAGIC 7. *Save data*: The data is saved in the marketing schema incrementally.
# MAGIC
# MAGIC Run the notebook in Databricks. If the fare_amount sum exceeds 115.00, you should receive an alert in Slack with the specified message and emoji.

# COMMAND ----------

# Importar bibliotecas necessárias
import requests
import json

# URL do webhook do Slack (substitua pela sua URL)
slack_webhook_url = 'your_url'
# Função para enviar mensagem para o Slack
def send_slack_message(message):
    payload = {'text': message}
    response = requests.post(slack_webhook_url, data=json.dumps(payload),
                             headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise Exception(f'Request to Slack returned an error {response.status_code}, the response is:\n{response.text}')

# Leitura dos dados da tabela
df = spark.sql("select * from samples.nyctaxi.trips")

# Calcular a soma de fare_amount
total_fare_amount = df.groupBy().sum("fare_amount").collect()[0][0]

# Verificar se a soma de fare_amount é maior que 115,00
if total_fare_amount > 115.00:
    # Mensagem de alerta com texto e emoji
    message = "🚨Finance\nCurrently, your expenses are above $1,000.00."
    send_slack_message(message)
