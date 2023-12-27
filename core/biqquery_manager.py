"""
BigQueryManager Class
---------------------
Purpose:
    To streamline interactions with Google BigQuery, providing a high-level interface for managing datasets and tables.
Functionality:
    - Handles authentication with Google Cloud services.
    - Offers methods to create, manage, and delete BigQuery datasets and tables.
    - Simplifies API interaction, abstracting away direct API calls and handling responses.
Use Cases:
    Ideal for applications requiring dynamic BigQuery resource management, such as ETL pipelines, data warehousing tasks, and automated dataset/table creation and cleanup processes. 
    It can be integrated into cloud-based data processing solutions, data analytics platforms, or as part of a larger data engineering workflow.
"""

import google.auth
import google.auth.transport.requests
import requests
import json
from google.cloud import bigquery

class BigQueryManager:
    """
    BigQueryManager class to manage Google BigQuery resources.
    This class provides methods to create BigQuery datasets and tables.
    """

    def __init__(self):
        # Initialize Google credentials
        self.credentials, self.project = google.auth.default()

    def create_bigquery_dataset(self, project_id, dataset_id, description=""):
        """
        Create a BigQuery dataset.

        :param project_id: Google Cloud project ID.
        :param dataset_id: BigQuery dataset ID.
        :param description: Optional description for the dataset.
        :return: Boolean indicating success or failure.
        """
        # Ensure credentials are valid for the session
        self.credentials.refresh(google.auth.transport.requests.Request())

        # Define the URL for dataset creation
        url = f'https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets'

        # Dataset configuration
        dataset_config = {
            "datasetReference": {
                "projectId": project_id,
                "datasetId": dataset_id
            }
        }

        # Add description if provided
        if description:
            dataset_config["description"] = description

        # Make an authorized POST request
        headers = {
            'Authorization': 'Bearer ' + self.credentials.token,
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, data=json.dumps(dataset_config))

        # Check the response
        if response.status_code == 200:
            print(f"Dataset {dataset_id} created successfully")
            return True
        else:
            print(f"Failed to create dataset: {response.text}")
            return False

    def create_bigquery_table(self, project_id, table_name, dataset, schema, description=""):
        """
        Create a BigQuery table.

        :param project_id: Google Cloud project ID.
        :param table_name: BigQuery table name.
        :param dataset: BigQuery dataset name.
        :param schema: Schema of the table.
        :param description: Optional description for the table.
        :return: Boolean indicating success or failure.
        """
        client = bigquery.Client(credentials=self.credentials)

        bq_path = f"{project_id}.{dataset}.{table_name}"

        # Create a Table instance with the schema
        table = bigquery.Table(bq_path, schema=schema)

        # Add description to the table if provided
        if description:
            table.description = description

        # Create the table
        try:
            created_table = client.create_table(table)
            print(f"Created table {created_table.full_table_id}")
            return True
        except Exception as e:
            print(f"Failed to create table: {e}")
            return False
