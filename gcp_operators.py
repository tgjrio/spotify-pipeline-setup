import google.auth
import requests
import json
import google.auth
import os

from google.cloud import dataform_v1beta1
from google.cloud import bigquery

from google.cloud.dataform_v1beta1.types import dataform
from google.api_core.exceptions import GoogleAPICallError



def create_bigquery_dataset(project_id, dataset_id, description=""):
    # Set up credentials
    credentials, project = google.auth.default()

    # Ensure credentials are valid for the session
    credentials.refresh(google.auth.transport.requests.Request())

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
        'Authorization': 'Bearer ' + credentials.token,
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
    

def create_bigquery_table(project_id, table_name, dataset, schema, description=""):
    credentials, project = google.auth.default()
    client = bigquery.Client(credentials=credentials)

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
    

def create_dataform_repository(project_id, location, repository_id):
    # Set up credentials
    credentials, project = google.auth.default()

    # Initialize the Dataform client
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    # Repository path
    parent = f"projects/{project_id}/locations/{location}"

    # Repository configuration
    repository = dataform.Repository(
        display_name=repository_id
    )

    # Create the repository
    try:
        response = client.create_repository(
            request={"parent": parent, "repository_id": repository_id, "repository": repository}
        )
        print(f"Repository created successfully: {response.name}")
        return True
    except GoogleAPICallError as e:
        print(f"Failed to create repository: {e}")
        return False
    

def create_dataform_workspace(project_id, location, repository_id, workspace_name):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    repository_path = client.repository_path(project_id, location, repository_id)
    print(f"Target repository path: {repository_path}")

    request = dataform_v1beta1.CreateWorkspaceRequest(
        parent=repository_path,
        workspace_id=workspace_name,
    )

    # Make the request
    try:
        response = client.create_workspace(request=request)
        print(f"Workspace '{workspace_name}' created successfully in repository: {repository_path}")
        return response
    except Exception as e:
        print(f"Failed to create workspace: {e}")
        return None
    

def insert_logic_to_workspace(project_id, location, repository_id, workspace_name, dataform_files_path):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    workspace_path = client.workspace_path(project_id, location, repository_id, workspace_name)
    print(f"Workspace path for insertion: {workspace_path}")

    for root, dirs, files in os.walk(dataform_files_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            path_in_workspace = os.path.relpath(file_path, dataform_files_path)
            with open(file_path, "rb") as file:
                contents = file.read()

                request = dataform.WriteFileRequest(
                    workspace=workspace_path,
                    path=path_in_workspace,
                    contents=contents,
                )

                try:
                    response = client.write_file(request=request)
                    print(f"File '{file_name}' inserted successfully into workspace '{workspace_name}' at '{path_in_workspace}'")
                except Exception as e:
                    print(f"Failed to insert file '{file_name}' at '{path_in_workspace}': {e}")

    print(f"All files from '{dataform_files_path}' have been processed.")


def install_npm_packages_in_workspace(project_id, location, repository_id, workspace_name):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    workspace_path = client.workspace_path(project_id, location, repository_id, workspace_name)
    print(f"Installing NPM packages in workspace: {workspace_path}")

    request = dataform.InstallNpmPackagesRequest(
        workspace=workspace_path,
    )

    try:
        response = client.install_npm_packages(request=request)
        print("NPM packages installed successfully.")
        return response
    except Exception as e:
        print(f"Failed to install NPM packages: {e}")
        return None
    

def commit_changes_to_workspace(project_id, location, repository_id, workspace_name, user_name, user_email):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    workspace_path = client.workspace_path(project_id, location, repository_id, workspace_name)
    print(f"Committing changes to workspace: {workspace_path}")

    author = dataform.CommitAuthor()
    author.name = user_name
    author.email_address = user_email

    request = dataform.CommitWorkspaceChangesRequest(
        name=workspace_path,
        author=author,
        commit_message="Committing changes to workspace"
    )

    try:
        client.commit_workspace_changes(request=request)
        print("Changes committed successfully.")
    except Exception as e:
        print(f"Failed to commit changes: {e}")


def push_git_commits_to_default_branch(project_id, location, repository_id, workspace_name):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    repository_path = client.workspace_path(project_id, location, repository_id, workspace_name)
    print(f"Pushing git commits for repository: {repository_path}")

    request = dataform.PushGitCommitsRequest(
        name=repository_path
    )

    try:
        client.push_git_commits(request=request)
        print("Git commits pushed successfully to the default branch.")
    except Exception as e:
        print(f"Failed to push git commits: {e}")


def create_compilation_result(project_id, location, repository_id):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    repository_path = client.repository_path(project_id, location, repository_id)
    print(f"Creating compilation result")

    compilation_result = dataform.CompilationResult()
    compilation_result.git_commitish = "main"

    request = dataform.CreateCompilationResultRequest(
        parent=repository_path,  # Adjust as needed
        compilation_result=compilation_result,
    )

    try:
        response = client.create_compilation_result(request=request)
        print("Compilation result created successfully.")
        return response.name
    except Exception as e:
        print(f"Failed to create compilation result: {e}")
        return None
    

def create_workflow_invocation(project_id,location, repository_id, compilation_result_name):
    credentials, project = google.auth.default()
    client = dataform_v1beta1.DataformClient(credentials=credentials)

    repository_path = client.repository_path(project_id, location, repository_id)
    print(f"Creating workflow invocation for repository: {repository_path}")

    workflow_invocation = dataform.WorkflowInvocation()
    workflow_invocation.compilation_result = compilation_result_name

    request = dataform.CreateWorkflowInvocationRequest(
        parent=repository_path,
        workflow_invocation=workflow_invocation,
    )

    try:
        response = client.create_workflow_invocation(request=request)
        print("Workflow invocation created successfully.")
        return response
    except Exception as e:
        print(f"Failed to create workflow invocation: {e}")
        return None