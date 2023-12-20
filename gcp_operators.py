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
    """
    Creates a dataset in Google BigQuery.
    - Sets up and validates Google credentials.
    - Constructs and sends a POST request to the BigQuery API to create a dataset.
    - Includes optional dataset description.
    - Returns True if the dataset is created successfully, False otherwise.
    Parameters:
        project_id (str): The ID of the Google Cloud project.
        dataset_id (str): The ID of the dataset to create.
        description (str, optional): A description for the dataset.
    """

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
    """
    Creates a table in a specified dataset in Google BigQuery.
    - Authenticates and initializes a BigQuery client.
    - Constructs a full BigQuery path for the new table.
    - Creates a Table instance with the provided schema and optional description.
    - Attempts to create the table in BigQuery and handles any exceptions.
    - Prints the success or failure message.
    Returns True if the table is created successfully, False otherwise.
    Parameters:
        project_id (str): The Google Cloud project ID.
        table_name (str): The name of the table to create.
        dataset (str): The dataset in which the table will be created.
        schema (List[bigquery.SchemaField]): The schema definition for the table.
        description (str, optional): A description for the table.
    """

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
    """
    Creates a repository in Dataform within Google Cloud.
    - Sets up and validates Google Cloud credentials.
    - Initializes a Dataform client.
    - Configures the repository with the provided display name.
    - Makes a request to create the repository in the specified project and location.
    - Handles exceptions and prints success or failure messages.
    Returns True if the repository is created successfully, False otherwise.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region for the repository.
        repository_id (str): The ID for the new repository.
    """

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
    """
    Creates a workspace in a Dataform repository.
    - Authenticates and initializes a Dataform client.
    - Constructs the path to the target repository.
    - Configures a request to create a workspace with the specified name.
    - Attempts to create the workspace and handles any exceptions.
    - Prints success message with workspace details or failure message.
    Returns the response object if successful, or None in case of failure.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The ID of the repository.
        workspace_name (str): The name for the new workspace.
    """

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
    """
    Inserts files from a local directory into a Dataform workspace.
    - Authenticates and initializes a Dataform client.
    - Constructs the path to the specified workspace.
    - Iterates over files in the provided directory path.
    - For each file, constructs a path within the workspace and reads its contents.
    - Makes a request to write each file into the workspace.
    - Handles exceptions and prints status messages for each file.
    Concludes by indicating all files have been processed.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The repository ID.
        workspace_name (str): The workspace name.
        dataform_files_path (str): Local file system path to Dataform files.
    """

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
    """
    Installs NPM packages in a specified Dataform workspace.
    - Sets up Google Cloud credentials and initializes a Dataform client.
    - Identifies the workspace path based on provided project, location, repository, and workspace names.
    - Sends a request to install NPM packages in the identified workspace.
    - Handles exceptions and prints success or failure messages.
    Returns the response object if successful, or None in case of failure.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The repository ID.
        workspace_name (str): The name of the workspace where NPM packages will be installed.
    """

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
    """
    Commits changes to a specific Dataform workspace.
    - Authenticates and initializes a Dataform client.
    - Constructs the workspace path based on the provided project, location, repository, and workspace names.
    - Sets up the author information for the commit using the provided user name and email.
    - Creates a request to commit changes to the workspace with a commit message.
    - Executes the commit request and handles any exceptions.
    - Prints a success message or an error message based on the outcome.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The repository ID.
        workspace_name (str): The name of the workspace to commit changes.
        user_name (str): Name of the user committing the changes.
        user_email (str): Email address of the user committing the changes.
    """

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
    """
    Pushes Git commits to the default branch of a specific workspace in Dataform.
    - Authenticates and initializes a Dataform client.
    - Determines the path to the targeted repository within the workspace.
    - Constructs and sends a request to push Git commits to the repository's default branch.
    - Handles any exceptions and prints out success or failure messages.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The ID of the repository.
        workspace_name (str): The name of the workspace containing the Git repository.
    """

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
    """
    Creates a compilation result for a given repository in Dataform.
    - Initializes Google Cloud credentials and a Dataform client.
    - Defines the path to the targeted repository.
    - Configures a compilation result targeting the 'main' Git branch.
    - Sends a request to create the compilation result in the repository.
    - Handles exceptions and prints success or error messages.
    Returns the name of the created compilation result if successful, or None in case of failure.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The ID of the repository.
    """

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
    """
    Initiates a workflow invocation in a Dataform repository.
    - Sets up Google Cloud credentials and initializes a Dataform client.
    - Determines the path to the targeted repository.
    - Configures a workflow invocation with the specified compilation result name.
    - Sends a request to create the workflow invocation in the repository.
    - Handles any exceptions and prints success or failure messages.
    Returns the response object if successful, or None in case of failure.
    Parameters:
        project_id (str): The Google Cloud project ID.
        location (str): The location/region of the repository.
        repository_id (str): The ID of the repository.
        compilation_result_name (str): The name of the compilation result to be used in the workflow invocation.
    """

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