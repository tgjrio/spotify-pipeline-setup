"""
DataformManager Class
---------------------
Purpose:
    To facilitate interactions with Dataform by Google, allowing users to programmatically manage data transformations and quality in BigQuery.
Functionality:
    - Manages authentication and session handling with Dataform services.
    - Provides methods for creating repositories, workspaces, and handling data logic.
    - Automates tasks like file insertion into workspaces, workspace changes, and deployment of data models.
Use Cases:
    Suitable for scenarios involving data modeling, data transformation, and quality checks in big data environments. 
    This class can be integrated into data analytics workflows, data governance tools, and in scenarios where Dataform is used for orchestrating complex data transformation pipelines.
"""

import google.auth
import os

from google.cloud import dataform_v1beta1
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import dataform

class DataformManager:
    """
    DataformManager class to manage Dataform resources.
    Provides methods for managing repositories, workspaces, files, and workflow invocations.
    """

    def __init__(self):
        # Initialize Google credentials
        self.credentials, self.project = google.auth.default()
        self.client = dataform_v1beta1.DataformClient(credentials=self.credentials)

    def create_dataform_repository(self, project_id, location, repository_id):
        """
        Create a Dataform repository.
        """
        parent = f"projects/{project_id}/locations/{location}"
        repository = dataform.Repository(display_name=repository_id)
        try:
            response = self.client.create_repository(
                request={"parent": parent, "repository_id": repository_id, "repository": repository}
            )
            print(f"Repository created successfully: {response.name}")
            return True
        except GoogleAPICallError as e:
            print(f"Failed to create repository: {e}")
            return False

    def create_dataform_workspace(self, project_id, location, repository_id, workspace_name):
        """
        Create a Dataform workspace.
        """
        repository_path = self.client.repository_path(project_id, location, repository_id)
        print(f"Target repository path: {repository_path}")

        request = dataform_v1beta1.CreateWorkspaceRequest(
            parent=repository_path,
            workspace_id=workspace_name,
        )

        try:
            response = self.client.create_workspace(request=request)
            print(f"Workspace '{workspace_name}' created successfully in repository: {repository_path}")
            return response
        except Exception as e:
            print(f"Failed to create workspace: {e}")
            return None
        
    def insert_logic_to_workspace(self, project_id, location, repository_id, workspace_name, dataform_files_path):
        """
        Insert logic into a Dataform workspace.
        """
        workspace_path = self.client.workspace_path(project_id, location, repository_id, workspace_name)
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
                        response = self.client.write_file(request=request)
                        print(f"File '{file_name}' inserted successfully into workspace '{workspace_name}' at '{path_in_workspace}'")
                    except Exception as e:
                        print(f"Failed to insert file '{file_name}' at '{path_in_workspace}': {e}")

        print(f"All files from '{dataform_files_path}' have been processed.")

    def install_npm_packages_in_workspace(self, project_id, location, repository_id, workspace_name):
            """
            Install NPM packages in a Dataform workspace.
            """
            workspace_path = self.client.workspace_path(project_id, location, repository_id, workspace_name)
            print(f"Installing NPM packages in workspace: {workspace_path}")

            request = dataform.InstallNpmPackagesRequest(
                workspace=workspace_path,
            )

            try:
                response = self.client.install_npm_packages(request=request)
                print("NPM packages installed successfully.")
                return response
            except Exception as e:
                print(f"Failed to install NPM packages: {e}")
                return None

    def commit_changes_to_workspace(self, project_id, location, repository_id, workspace_name, user_name, user_email):
        """
        Commit changes to a Dataform workspace.
        """
        workspace_path = self.client.workspace_path(project_id, location, repository_id, workspace_name)
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
            self.client.commit_workspace_changes(request=request)
            print("Changes committed successfully.")
        except Exception as e:
            print(f"Failed to commit changes: {e}")

    def push_git_commits_to_default_branch(self, project_id, location, repository_id, workspace_name):
        """
        Push git commits to the default branch of a Dataform repository.
        """
        repository_path = self.client.workspace_path(project_id, location, repository_id, workspace_name)
        print(f"Pushing git commits for repository: {repository_path}")

        request = dataform.PushGitCommitsRequest(
            name=repository_path
        )

        try:
            self.client.push_git_commits(request=request)
            print("Git commits pushed successfully to the default branch.")
        except Exception as e:
            print(f"Failed to push git commits: {e}")

    def create_compilation_result(self, project_id, location, repository_id):
        """
        Create a compilation result in a Dataform repository.
        """
        repository_path = self.client.repository_path(project_id, location, repository_id)
        print(f"Creating compilation result")

        compilation_result = dataform.CompilationResult()
        compilation_result.git_commitish = "main"

        request = dataform.CreateCompilationResultRequest(
            parent=repository_path,
            compilation_result=compilation_result,
        )

        try:
            response = self.client.create_compilation_result(request=request)
            print("Compilation result created successfully.")
            return response.name
        except Exception as e:
            print(f"Failed to create compilation result: {e}")
            return None

    def create_workflow_invocation(self, project_id, location, repository_id, compilation_result_name):
        """
        Create a workflow invocation in a Dataform repository.
        """
        repository_path = self.client.repository_path(project_id, location, repository_id)
        print(f"Creating workflow invocation for repository: {repository_path}")

        workflow_invocation = dataform.WorkflowInvocation()
        workflow_invocation.compilation_result = compilation_result_name

        request = dataform.CreateWorkflowInvocationRequest(
            parent=repository_path,
            workflow_invocation=workflow_invocation,
        )

        try:
            response = self.client.create_workflow_invocation(request=request)
            print("Workflow invocation created successfully.")
            return response
        except Exception as e:
            print(f"Failed to create workflow invocation: {e}")
            return None