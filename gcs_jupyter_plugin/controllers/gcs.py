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
import os
import tempfile
import aiohttp
import tornado
from jupyter_server.base.handlers import APIHandler

from gcs_jupyter_plugin import credentials
from gcs_jupyter_plugin.services import gcs


class ListBucketsController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            prefix = self.get_argument("prefix")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                buckets = await client.list_buckets(prefix)
            self.finish(json.dumps(buckets))
        except Exception as e:
            self.log.exception("Error fetching datasets.")
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

                files = await client.list_files(bucket,prefix)
            self.finish(json.dumps(files))
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
                self.finish({"error": "Missing required parameters."})
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
            uploadFlag = True if self.get_body_argument("upload") == "true" else False

            if not bucket or not destination_path:
                self.set_status(400)
                self.finish(json.dumps({"error": "Missing required parameters."}))
                return

            # Use the client to upload the content
            storage_client = gcs.Client(await credentials.get_cached(), self.log, None)
            result = await storage_client.save_content(
                bucket, destination_path, content, uploadFlag 
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
    async def get(self):
        try:
            bucket = self.get_argument("bucket")
            file_path = self.get_argument("path")
            format = self.get_argument("format")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

                file = await client.get_file(bucket,file_path, format)
            self.finish(json.dumps(file))
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})


class DeleteFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
        try:
            data = json.loads(self.request.body)
            bucket = data.get("bucket")
            path = data.get("path")

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
                self.finish({
                    "message" : "File / Folder Successfully deleted",
                    "status" : 200,
                })
        except Exception as e:
            self.log.exception("Error deleting file")
            self.set_status(500)
            self.finish({"error": str(e)})


class RenameFileController(APIHandler):
    @tornado.web.authenticated
    async def post(self):
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

            # Currently rename_blob only works within the same bucket
            if old_bucket != new_bucket:
                self.set_status(400)
                self.finish(
                    json.dumps({"error": "Cross-bucket renaming is not supported."})
                )
                return

            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )

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


class DownloadFileController(APIHandler):
    @tornado.web.authenticated
    async def get(self):
        try:
            bucket = self.get_argument("bucket")
            file_path = self.get_argument("path")
            name = self.get_argument("name")
            format = self.get_argument("format")
            async with aiohttp.ClientSession() as client_session:
                client = gcs.Client(
                    await credentials.get_cached(), self.log, client_session
                )
                file_content = await client.download_file(bucket,file_path, name, format)

                self.finish(file_content)
                
                # if format == 'text':
                #     self.set_header('Content-Type', 'text/plain')
                #     self.set_header('Content-Disposition', f'attachment; filename="{name}"')
                #     self.finish(file_content)
                # elif format == 'json':
                #     self.set_header('Content-Type', 'application/json')
                #     self.set_header('Content-Disposition', f'attachment; filename="{name}"')
                #     self.finish(file_content)
                # elif format == 'base64':
                #     self.set_header('Content-Type', 'application/octet-stream')
                #     self.set_header('Content-Disposition', f'attachment; filename="{name}"')
                #     self.finish(base64.b64decode(file_content))
                # else:
                #     # returning the content as it is when format is not available
                #     self.set_header('Content-Type', 'application/octet-stream')
                #     self.set_header('Content-Disposition', f'attachment; filename="{name}"')
                #     self.finish(file_content)

            
        except Exception as e:
            self.log.exception("Error fetching datasets")
            self.finish({"error": str(e)})