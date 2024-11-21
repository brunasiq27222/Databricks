# Databricks notebook source
# MAGIC %md
# MAGIC ### Explicação:
# MAGIC
# MAGIC 1. *Importar bibliotecas*: As bibliotecas requests e json são importadas para enviar a mensagem para o Slack.
# MAGIC 2. *Configurar webhook*: Substitua slack_webhook_url pela URL do webhook configurado no Slack.
# MAGIC
# MAGIC Para criar um notebook no Databricks que envie um alerta para o Slack quando os dados são carregados, você precisará seguir os seguintes passos:
# MAGIC
# MAGIC 1. *Configurar um webhook no Slack*.
# MAGIC 2. *Criar o notebook no Databricks*.
# MAGIC 3. *Escrever código para carregar os dados*.
# MAGIC 4. *Enviar o alerta para o Slack usando o webhook*.
# MAGIC
# MAGIC  Passo 1: Configurar um webhook no Slack
# MAGIC
# MAGIC 1. Vá para o Slack e navegue até *Apps*.
# MAGIC 2. Procure por *Incoming Webhooks* e adicione ao seu workspace.
# MAGIC 3. Configure um novo webhook e copie a URL gerada. Esta URL será usada para enviar mensagens para o Slack.
# MAGIC
# MAGIC
# MAGIC 3. *Função para enviar mensagem*: A função send_slack_message é usada para enviar a mensagem para o Slack.
# MAGIC 4. *Leitura dos dados*: Os dados são lidos da tabela tabela.
# MAGIC 5. *Calcular soma de fare_amount*: A soma de fare_amount é calculada usando groupBy().sum("fare_amount").
# MAGIC 6. *Verificar soma*: Se a soma de fare_amount for maior que 115,00, uma mensagem de alerta com texto e emoji é enviada para o Slack.
# MAGIC 7. *Salvar dados*: Os dados são salvos no schema marketing de forma incremental.
# MAGIC
# MAGIC Execute o notebook no Databricks. Se a soma de fare_amount exceder 115,00, você deverá receber um alerta no Slack com a mensagem e o emoji especificados.

# COMMAND ----------

# Importar bibliotecas necessárias
import requests
import json

# URL do webhook do Slack (substitua pela sua URL)
slack_webhook_url = 'https://hooks.slack.com/services/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'
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
    message = "🚨Financeiro \nAtualmente seus gastos estão > 1.000,00"
    send_slack_message(message)
