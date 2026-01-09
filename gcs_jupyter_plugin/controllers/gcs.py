# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import aiohttp
import tornado
from jupyter_server.base.handlers import APIHandler

from gcs_jupyter_plugin import credentials
from gcs_jupyter_plugin.services import gcs

from gcs_jupyter_plugin.commons.constants import (
    MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE,
)

class Credentials:
    project_id = None
    def __init__(self, project_id):
        self.project_id = project_id

class ProjectSelectorHandler(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            project_id = self.get_argument("project_id")
            
            if not project_id:
                self.set_status(400)
                self.finish(json.dumps({"error": "No project_id provided"}))
                return
            
            from gcs_jupyter_plugin.controllers.gcs import Credentials
            Credentials.project_id = project_id
            
            self.finish(json.dumps({"status": "PROJECT_UPDATED", "project_id": project_id}))
        except Exception as e:
            self.log.exception("Failed to set project_id")
            self.set_status(500)
            self.finish(json.dumps({"error": str(e)})) 


class ProjectsListController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        """Returns projects list"""
        try:
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session, Credentials.project_id
                )
            projects = await client.list_gcp_projects()
            self.finish(json.dumps(projects))
        except RuntimeError as e:
            error_data = e.args[0]
            status_code = error_data.get("status", 500)

            self.log.exception(f"Error fetching projects: {str(e)}")
            self.set_status(status_code)
            self.finish(json.dumps(error_data))
        except Exception as e:
            self.log.exception(f"Error fetching projects: {str(e)}")
            self.finish({"error": str(e)})
class ListBucketsController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session, Credentials.project_id
                )
                buckets = await client.list_buckets()
            # Check if the service returned an error object (e.g., 403 Forbidden)
            if isinstance(buckets, dict) and "error" in buckets:
                # Extract the message. GCS errors often contain "403" or "access denied"
                error_msg = buckets.get("error", "")
                if "403" in error_msg or "denied" in error_msg.lower():
                    self.set_status(403)
                else:
                    self.set_status(500)
                self.finish(json.dumps(buckets))
                return

            self.finish(json.dumps(buckets))
        except Exception as e:
            self.log.exception("Error fetching datasets.")
            self.set_status(500)
            self.finish({"error": str(e)})


class ListFilesController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            prefix = self.get_argument("prefix")
            bucket = self.get_argument("bucket")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )
                files = await client.list_files(bucket, prefix)

            result = json.dumps(files)
            self.finish(result)
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})


class CreateFolderController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            path = data.get("path", "")
            folder_name = data.get("folderName")

            if not bucket or not folder_name:
                self.set_status(400)
                self.finish({"error": MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE})
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                folder = await client.create_folder(bucket, path, folder_name)
            self.finish(json.dumps(folder))
        except Exception as e:
            self.log.exception("Error creating folder.")
            self.set_status(500)
            self.finish({"error": str(e)})


class SaveFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            # Get parameters from the request
            bucket = self.get_body_argument("bucket", None)
            destination_path = self.get_body_argument("path", None)
            content = self.get_body_argument("contents", "")
            upload_flag = True if self.get_body_argument("upload") == "true" else False

            if not bucket or not destination_path:
                self.set_status(400)
                self.finish(
                    json.dumps({"error": MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE})
                )
                return

            # Use the client to upload the content
            storage_client = gcs.Client(await credentials.get_cached(), self.log, None)
            result = await storage_client.save_content(
                bucket, destination_path, content, upload_flag
            )

            if isinstance(result, dict) and "error" in result:
                self.set_status(result.get("status", 500))
                self.finish(json.dumps(result))
            else:
                self.set_status(200)
                self.finish(json.dumps(result))

        except Exception as e:
            self.log.exception("Error saving content")
            self.set_status(500)
            self.finish(json.dumps({"error": str(e)}))


class LoadFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            file_path = data.get("path")
            file_format = data.get("format")

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                file = await client.get_file(bucket, file_path, file_format)

            if file_path.endswith(".json"):
                self.set_header("Content-Type", "application/json")
                self.write(json.dumps(file))
            elif file_format == "json":
                self.set_header("Content-Type", "application/json")
                self.finish(json.dumps(file))
            elif file_format == "base64":
                self.set_header("Content-Type", "application/octet-stream")
                self.write(file)
            else:
                self.set_header("Content-Type", "text/plain")
                self.finish(file)
        except Exception as e:
            self.log.exception("Error fetching file")
            self.set_status(404)
            self.finish({"error": str(e)})


class DeleteFileController(APIHandler):
    @tornado.web.authenticated
    async def delete(self):
        try:
            bucket = self.get_argument("bucket")
            path = self.get_argument("path")

            if not bucket:
                self.finish(
                    {
                        "error": "Missing required parameters.",
                        "status": 400,
                    }
                )
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                result = await client.delete_file(bucket, path)

                # Check for specific error conditions in the result
                if "error" in result:
                    self.finish(
                        {
                            "error": result.get("error"),
                            "status": result.get("status"),
                        }
                    )
                    return

                # Set correct success status for delete operation
                self.finish(
                    {
                        "message": "File / Folder Successfully deleted",
                        "status": 200,
                    }
                )
        except Exception as e:
            self.log.exception("Error deleting file")
            self.set_status(500)
            self.finish({"error": str(e)})


class RenameFileController(APIHandler):
    @tornado.web.authenticated
    async def patch(self):
        try:
            data = json.loads(self.request.body)
            old_bucket = data.get("oldBucket")
            old_path = data.get("oldPath")
            new_bucket = data.get("newBucket")
            new_path = data.get("newPath")

            if not old_bucket or not old_path or not new_bucket or not new_path:
                self.set_status(400)
                self.finish(json.dumps({"error": "Missing required parameters."}))
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                if old_bucket != new_bucket:
                    # In Jupyter lab, The UI does not allow cross-bucket renaming, and user can modify the name of the file alone.
                    # Jupyter lab appends the prefix ( bucket and parent folder if any ) to the new path when renaming,
                    # so we can safely assume that old_bucket and new_bucket are the same in the case of renaming.

                    # Cut-paste functionality is using rename functionality internally, which allows cross-bucket renaming.
                    # For cut-paste scenario, there is a chance that the old_bucket and new_bucket are different.
                    # Google storage sdk (python) does not support cross-bucket renaming, so we need to handle this case.
                    # So, when user performs cut-paste operation within the same bucket, we can allow the renaming operation.
                    # If the user tries to rename a file across buckets, we will copy the file to the new bucket and delete the old file.
                    # This is a workaround to support cross-bucket renaming, as the Google storage sdk (python) does not support cross-bucket renaming directly.
                    result = await client.move_blob(
                        old_bucket, old_path, new_bucket, new_path
                    )
                else:
                    # If the old and new buckets are the same, we can use the rename operation
                    result = await client.rename_file(old_bucket, old_path, new_path)

                # Check for specific error conditions in the result
                if "error" in result:
                    self.finish(
                        {
                            "error": result.get("error"),
                            "status": result.get("status"),
                        }
                    )
                    return
                self.set_status(200)
                self.finish(json.dumps(result))

        except Exception as e:
            self.log.exception("Error renaming file")
            self.set_status(500)
            self.finish(json.dumps({"error": str(e)}))


class CopyFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            source_bucket = data.get("sourceBucket")
            source_path = data.get("sourcePath")
            destination_bucket = data.get("destinationBucket")
            destination_path = data.get("destinationPath")

            if (
                not source_bucket
                or not source_path
                or not destination_bucket
                or not destination_path
            ):
                self.set_status(400)
                self.finish(
                    json.dumps({"error": MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE})
                )
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                result = await client.copy_file(
                    source_bucket, source_path, destination_bucket, destination_path
                )

                if "error" in result:
                    self.set_status(result.get("status", 500))
                    self.finish(json.dumps(result))
                else:
                    self.set_status(200)
                    self.finish(json.dumps(result))

        except Exception as e:
            self.log.exception("Error copying file or folder")
            self.set_status(500)
            self.finish(json.dumps({"error": str(e)}))


class DownloadFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            file_path = data.get("path")
            name = data.get("name")
            file_format = data.get("format")

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )
                file_content = await client.download_file(
                    bucket, file_path, name, file_format
                )

                self.finish(file_content)

        except Exception as e:
            self.log.exception("Error fetching file")
            self.finish({"error": str(e)})
