# Data Pipelines Alerting
This repository contains templates for building data pipelines with Slack-integrated alerts. It demonstrates how to monitor data consistency, save CSV files with identified inconsistencies, and send automated notifications to specific Slack channels.

These templates are generic and can be adapted for various use cases, such as ETL pipeline monitoring, data load notifications, and data integration in a Medallion Architecture environment, ensuring that bronze, silver, and gold layers maintain high-quality data for downstream applications.
  
## Features
- Automated data quality checks.
- Generation of CSV files containing inconsistent records.
- Slack integration for sending notifications, including file attachments.
- Structured, reusable, and easily customizable code.

## Technologies Used
- Databricks: For running pipelines and performing data quality checks.
- Slack API: For sending notifications and files to Slack channels.
- PySpark: For data manipulation and transformation.
- Python (requests, json): For external API communication.

# Contact Information
For more information or questions about the project, please contact Bruna Siqueira at https://www.linkedin.com/in/bruna-siqueira-05814a182/
