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

import asyncio
import json
import base64
from datetime import timedelta
import pathlib
import nbformat
import re
import io
import functools

import tornado.web

from google.oauth2 import credentials
from google.cloud import storage
from google.api_core.client_options import ClientOptions

from gcs_jupyter_plugin.commons.constants import CONTENT_TYPE, STORAGE_SERVICE_NAME, BINARY_ENCODING_EXTENSIONS, MIMETYPE_MAP
from gcs_jupyter_plugin import urls

def ensure_client_setup(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if self.storage_client is None:
            await self.setup()
        return await func(self, *args, **kwargs)
    return wrapper

class Client(tornado.web.RequestHandler):
    def __init__(self, credentials, log, client_session):
        self.log = log
        if not (
            ("access_token" in credentials)
            and ("project_id" in credentials)
            and ("region_id" in credentials)
        ):
            self.log.exception("Missing required credentials")
            raise ValueError("Missing required credentials")
        self._access_token = credentials["access_token"]
        self.project_id = credentials["project_id"]
        self.region_id = credentials["region_id"]
        self.client_session = client_session
        self.storage_client = None
    
    async def setup(self):
        """Asynchronously initializes the GCS client."""
        token = self._access_token
        project = self.project_id
        creds = credentials.Credentials(token)
        
        storage_url = await urls.gcp_service_url(STORAGE_SERVICE_NAME) 
        self.storage_client = storage.Client(
            project=project,
            credentials=creds,
            client_options=ClientOptions(api_endpoint=storage_url)
        )

    @ensure_client_setup
    async def list_buckets(self, prefix=None):
        try:
            bucket_list = []
            loop = asyncio.get_running_loop()
            
            def _get_buckets_from_gcs():
                # this log just to knnow what api_endpoint is picked
                # Keeping this since positive testing is not done yet. 
                self.log.info(f"client initiated with endpoint: {self.storage_client._connection.API_BASE_URL}")
                return self.storage_client.list_buckets(prefix=prefix)

            buckets = await loop.run_in_executor(None, _get_buckets_from_gcs)

            for bucket in buckets:
                bucket_list.append(
                    {
                        "items": {
                            "name": bucket.name,
                            "updated": (
                                bucket.updated.isoformat() if bucket.updated else ""
                            ),
                        }
                    }
                )
            return bucket_list
        except Exception as e:
            self.log.exception("Error fetching datasets list.")
            return {"error": str(e)}

    # gcs -- list files implementation
    @ensure_client_setup
    async def list_files(self, bucket, prefix):
        try:
            result = {}
            file_list = []
            subdir_list = []
            loop = asyncio.get_running_loop()

            def _get_blobs_from_gcs():
                return self.storage_client.list_blobs(
                    bucket,
                    prefix=prefix,
                    delimiter="/",
                    fields="items(name,size,timeCreated,updated,contentType),prefixes",
                )

            blobs = await loop.run_in_executor(None, _get_blobs_from_gcs)

            files = list(blobs)

            # Adding Sub-directories
            if blobs.prefixes:
                for pref in blobs.prefixes:
                    subdir_list.append({"prefixes": {"name": pref}})

            # Adding Files
            for file in files:
                if not (file.name == prefix and file.size == 0):
                    file_list.append(
                        {
                            "items": {
                                "name": file.name,
                                "timeCreated": (
                                    file.time_created.isoformat()
                                    if file.time_created
                                    else ""
                                ),
                                "updated": (
                                    file.updated.isoformat() if file.updated else ""
                                ),
                                "size": file.size,
                                "content_type": file.content_type,
                            }
                        }
                    )

            result["prefixes"] = subdir_list
            result["files"] = file_list
            return result

        except Exception as e:
            self.log.exception(f"Error listing files: {e}")
            return []  # Return empty list on error.

    @ensure_client_setup
    async def get_file(self, bucket_name, file_path, format):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_path)

            if format == "base64":
                file_content = blob.download_as_bytes()
                try:
                    base64_encoded = base64.b64encode(file_content).decode("utf-8")
                    return base64_encoded
                except Exception:
                    return []

            if file_path.endswith(".ipynb"):
                file_content = blob.download_as_text()
                return nbformat.reads(
                    file_content, as_version=4, capture_validation_error=True
                )
            elif format == "json":
                file_content = blob.download_as_text()
                return file_content
            else:
                return blob.download_as_text()

        except Exception as e:
            raise e

    @ensure_client_setup
    async def create_folder(self, bucket, path, folder_name):
        try:
            # Format the folder path
            new_folder_path = str(pathlib.PosixPath(path).joinpath(folder_name)) + "/"

            self.log.info(f"Creating folder at: {new_folder_path}")

            # Get the bucket
            bucket_obj = self.storage_client.bucket(bucket)
            # Create an empty blob with a trailing slash to indicate a folder
            blob = bucket_obj.blob(new_folder_path)
            # Upload empty content to create the folder
            blob.upload_from_string("")

            # Return the folder information
            return {
                "name": new_folder_path,
                "bucket": bucket,
                "id": f"{bucket}/{new_folder_path}",
                "kind": "storage#object",
                "mediaLink": blob.media_link,
                "selfLink": blob.self_link,
                "generation": blob.generation,
                "metageneration": blob.metageneration,
                "contentType": "application/x-www-form-urlencoded;charset=UTF-8",
                "timeCreated": (
                    blob.time_created.isoformat() if blob.time_created else ""
                ),
                "updated": blob.updated.isoformat() if blob.updated else "",
                "storageClass": blob.storage_class,
                "size": "0",
                "md5Hash": blob.md5_hash,
                "etag": blob.etag,
            }
        except Exception as e:
            self.log.exception("Error creating folder.")
            return {"error": str(e)}

    @ensure_client_setup
    async def save_content(
        self, bucket_name, destination_blob_name, content, upload_flag
    ):
        """Upload content directly to Google Cloud Storage.

        Args:
            bucket_name: The name of the GCS bucket
            destination_blob_name: The path in the bucket where the content should be stored
            content: The content to upload (string or JSON)
            uploadFlag: true if uploading a file, false when saving a file

        Returns:
            Dictionary with metadata or error information
        """
        try:
            bytes_content = None

            if isinstance(content, bytes):
                bytes_content = content
            elif isinstance(content, str) and content.startswith("data:"):
                data_url_match = re.match(r"data:([^;]+);base64,(.*)", content)
                if data_url_match:
                    base64_data = data_url_match.group(2)
                    bytes_content = base64.b64decode(base64_data)
                else:
                    raise ValueError("Invalid base64 data URL format")
            elif isinstance(content, dict):
                bytes_content = json.dumps(content).encode("utf-8")
            elif isinstance(content, str):
                if destination_blob_name.lower().endswith(BINARY_ENCODING_EXTENSIONS):
                    bytes_content = content.encode("latin1")
                else:
                    bytes_content = content.encode("utf-8")
            else:
                raise ValueError(f"Unsupported content type: {type(content)}")

            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Conflict check
            if blob.exists() and upload_flag:
                return {
                    "name": destination_blob_name,
                    "bucket": bucket_name,
                    "exists": True,
                    "success": False,
                    "error": f"A file with name {destination_blob_name} already exists in the destination.",
                    "status": 409,  # Conflict
                }

            # Determine content type
            file_ext = pathlib.Path(destination_blob_name.lower()).suffix
            content_type = MIMETYPE_MAP.get(file_ext, "application/octet-stream")

            # Upload to GCS
            file_obj = io.BytesIO(bytes_content)
            blob.upload_from_file(file_obj, content_type=content_type)

            return {
                "name": destination_blob_name,
                "bucket": bucket_name,
                "size": blob.size,
                "contentType": blob.content_type,
                "timeCreated": blob.time_created.isoformat() if blob.time_created else "",
                "updated": blob.updated.isoformat() if blob.updated else "",
                "success": True,
            }

        except Exception as e:
            self.log.exception(f"Error uploading content to {destination_blob_name}.")
            return {"error": str(e), "status": 500}

    @ensure_client_setup
    async def delete_file(self, bucket, path):
        try:
            # Get the bucket
            bucket_obj = self.storage_client.bucket(bucket)

            # Check if it's a folder/bucket deletion attempt
            if path == "" or path == "/":
                return {
                    "error": "Deleting Bucket is not allowed.",
                    "status": 409,
                }

            is_file = True
            target_blob = bucket_obj.blob(path)
            if target_blob.exists():
                # It's a file or a 0-byte folder marker
                if target_blob.size == 0 and target_blob.name.endswith("/"):
                    # It's explicitly an empty folder marker
                    is_file = False
                else:
                    # It's a regular file
                    is_file = True
            else:
                # If the exact blob doesn't exist, check if it's a folder (prefix)
                # We append '/' to the path to check for objects *within* that folder.
                blobs_under_prefix = list(bucket_obj.list_blobs(prefix=path + "/"))
                if len(blobs_under_prefix) > 0:
                    is_file = False
                elif bucket_obj.blob(path + "/").exists():
                    # It might be a 0-byte object created for an empty folder
                    is_file = False
                else:
                    return {"error": "File/Folder not found.", "status": 404}

            if is_file:
                # Delete a single file
                try:
                    target_blob.delete()
                    self.log.info(f"Successfully deleted file: {path}")
                    return {"success": True, "status": 200}
                except Exception as e:
                    self.log.exception(f"Error deleting file {path}.")
                    return {"error": str(e), "status": 500}
            else:
                # Delete a folder (recursively delete all blobs with the prefix)
                folder_prefix = path if path.endswith("/") else path + "/"
                self.log.info(f"Attempting to delete non-empty folder: {folder_prefix}")
                try:
                    blobs_to_delete = list(bucket_obj.list_blobs(prefix=folder_prefix))
                    if not blobs_to_delete:
                        # This case handles if list_blobs returns empty for some reason,
                        # but we already determined it's a folder, possibly an empty marker.
                        # Try deleting the 0-byte marker if it exists.
                        empty_folder_blob = bucket_obj.blob(folder_prefix)
                        if empty_folder_blob.exists() and empty_folder_blob.size == 0:
                            empty_folder_blob.delete()
                            self.log.info(
                                f"Successfully deleted empty folder marker: {folder_prefix}"
                            )
                            return {"success": True, "status": 200}
                        else:
                            return {
                                "error": f"Folder '{path}' found no contents to delete.",
                                "status": 404,
                            }

                    for blob_to_delete in blobs_to_delete:
                        self.log.info(f"Deleting blob: {blob_to_delete.name}")
                        blob_to_delete.delete()

                    self.log.info(
                        f"Successfully deleted non-empty folder and its contents: {path}"
                    )
                    return {"success": True, "status": 200}

                except Exception as e:
                    self.log.exception(f"Error deleting non-empty folder {path}.")
                    return {"error": str(e), "status": 500}

        except Exception as e:
            self.log.exception(f"Error deleting file {path}.")
            return {"error": str(e), "status": 500}

    @ensure_client_setup
    async def rename_file(self, bucket_name, blob_name, new_name):
        """
        Renames a blob using the rename_blob method.
        Note: This only works within the same bucket.
        """
        try:
            # Get the bucket and blob
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            # Check if source blob exists
            is_file = True
            if not blob.exists():
                # It might be a folder, so adding trail slash and checking for a blob (0 byte object will be returned)
                # using blobs , we can exclude the 0 byte blob and count the children
                blobs = bucket.list_blobs(
                    prefix=(blob_name if blob_name.endswith("/") else blob_name + "/")
                )

                blob_count = 0
                for iblob in blobs:
                    # For empty folders, gcs creates a zero-byte object with a trailing slash to simulate a folder.
                    # here we exclude that 0 byte object.
                    blob = iblob
                    if (
                        iblob.name[:-1] if iblob.name.endswith("/") else iblob.name
                    ) != blob_name:
                        blob_count += 1
                        # breaking the loop here, since we just want to know whether at-least 1 file present or not.
                        # Folder cannot be renamed even if 1 file/folder present
                        if blob_count > 1:
                            break
                if (
                    blob.exists()
                    and blob_count == 0
                    and (blob.name[:-1] if blob.name.endswith("/") else blob.name)
                    == blob_name
                ):
                    # Only 0 byte Object present
                    is_file = False
                elif blob_count > 0:
                    self.log.info("Renaming a non-empty folder.")
                    return await self.rename_non_empty_folder(
                        bucket, blob_name, new_name
                    )
                else:
                    return {"error": f"{blob_name} not found", "status": 404}

            # Check for availability of new name ( if already present, return error)
            if is_file:
                blob_new = bucket.blob(new_name)

                if blob_new.exists():
                    return {
                        "error": f"A file with name {blob_new.name} already exists in the destination.",
                        "status": 409,
                    }
            else:
                # Adding Trailing slash to avoid partial match of other folders
                blob_new = bucket.blob(new_name)
                blobs = bucket.list_blobs(prefix=new_name + "/")
                if any(blobs):
                    return {
                        "error": f"A folder with name {blob_new.name} already exists in the destination.",
                        "status": 409,
                    }

            # Rename the blob
            if is_file:
                new_blob = bucket.rename_blob(blob, new_name)
            else:
                new_blob = bucket.rename_blob(blob, new_name + "/")

            # Return success response
            return {
                "name": new_blob.name,
                "bucket": bucket_name,
                "success": True,
                "status": 200,
            }

        except Exception as e:
            self.log.exception(f"Error renaming from {blob_name} to {new_name}.")
            return {"error": str(e), "status": 500}

    async def rename_non_empty_folder(
        self, bucket: storage.Bucket, source_prefix: str, new_prefix: str
    ):
        source_prefix_normalized = (
            source_prefix if source_prefix.endswith("/") else source_prefix + "/"
        )
        new_prefix_normalized = (
            new_prefix if new_prefix.endswith("/") else new_prefix + "/"
        )

        blobs_to_rename = list(bucket.list_blobs(prefix=source_prefix_normalized))

        if not blobs_to_rename:
            empty_folder_blob = bucket.blob(source_prefix_normalized)
            if empty_folder_blob.exists() and empty_folder_blob.size == 0:
                self.log.info(
                    f"Renaming empty folder marker '{source_prefix_normalized}' to '{new_prefix_normalized}'."
                )
                new_blob = bucket.rename_blob(empty_folder_blob, new_prefix_normalized)
                return {
                    "name": new_blob.name,
                    "bucket": bucket.name,
                    "success": True,
                    "status": 200,
                    "message": "Empty folder marker renamed.",
                }
            else:
                return {
                    "error": f"Folder '{source_prefix}' not found or has no objects to rename.",
                    "status": 404,
                }

        if (
            new_prefix_normalized.startswith(source_prefix_normalized)
            and new_prefix_normalized != source_prefix_normalized
        ):
            return {
                "error": f"Cannot rename folder '{source_prefix}' to a sub-path of itself '{new_prefix}'.",
                "status": 400,
            }

        renamed_blobs = []
        try:
            for blob in blobs_to_rename:
                relative_path = blob.name[len(source_prefix_normalized) :]
                new_blob_name = new_prefix_normalized + relative_path

                if bucket.blob(new_blob_name).exists():
                    self.log.error(
                        f"Conflict detected during manual folder rename: '{new_blob_name}' already exists."
                    )
                    raise ValueError(
                        f"Destination object '{new_blob_name}' already exists. Aborting rename."
                    )

                new_blob = bucket.copy_blob(blob, bucket, new_name=new_blob_name)
                renamed_blobs.append(new_blob)
                blob.delete()

            return {
                "message": f"Folder '{source_prefix}' and its contents renamed to '{new_prefix}'.",
                "bucket": bucket.name,
                "success": True,
                "status": 200,
            }
        except Exception as e:
            self.log.error(
                f"Error during manual folder rename. Attempting to restore original files: {e}"
            )
            for blob in renamed_blobs:
                original_blob_name = (
                    source_prefix_normalized + blob.name[len(new_prefix_normalized) :]
                )
                try:
                    self.log.warning(
                        f"Attempting to revert '{blob.name}' to '{original_blob_name}'"
                    )
                    bucket.copy_blob(blob, bucket, new_name=original_blob_name)
                    blob.delete()
                except Exception as revert_e:
                    self.log.error(
                        f"Failed to revert blob '{blob.name}' to '{original_blob_name}': {revert_e}"
                    )
            return {
                "success": False,
                "error": f"Failed to rename folder: {str(e)}",
                "status": 500,
            }

    @ensure_client_setup
    async def copy_file(
        self, source_bucket_name, source_path, destination_bucket_name, destination_path
    ):
        """Copies a blob from one bucket to another, or within the same bucket.
        Can also copy a folder (all blobs with a given prefix).
        """
        try:
            source_bucket = self.storage_client.bucket(source_bucket_name)
            destination_bucket = self.storage_client.bucket(destination_bucket_name)

            if destination_path.startswith("/"):
                destination_path = destination_path[1:]

            source_blob = source_bucket.blob(source_path)

            is_folder = False
            # Check if source_path is a folder by checking for objects with its prefix
            blobs_under_source_prefix = list(
                source_bucket.list_blobs(prefix=source_path + "/")
            )
            if blobs_under_source_prefix or (
                source_blob.exists()
                and source_blob.size == 0
                and source_blob.name.endswith("/")
            ):
                is_folder = True

            if is_folder:
                # folder copy
                source_prefix_normalized = (
                    source_path if source_path.endswith("/") else source_path + "/"
                )
                destination_prefix_normalized = (
                    destination_path
                    if destination_path.endswith("/")
                    else destination_path + "/"
                )

                if (
                    source_bucket_name == destination_bucket_name
                    and destination_prefix_normalized.startswith(
                        source_prefix_normalized
                    )
                ):
                    return {
                        "error": f"Cannot paste folder '{source_path}' to a sub-path of itself '{destination_path}'.",
                        "status": 400,
                        "isFolder": True,
                    }

                # all blobs within the source folder
                blobs_to_copy = blobs_under_source_prefix

                if not blobs_to_copy:
                    # If it's an empty folder (0-byte object marker), copy it
                    empty_folder_blob = source_bucket.blob(source_prefix_normalized)
                    if empty_folder_blob.exists() and empty_folder_blob.size == 0:
                        new_blob = source_bucket.copy_blob(
                            empty_folder_blob,
                            destination_bucket,
                            new_name=destination_prefix_normalized,
                        )
                        return {
                            "name": new_blob.name,
                            "bucket": destination_bucket_name,
                            "success": True,
                            "status": 200,
                            "isFolder": True,
                            "message": "Empty folder copied successfully.",
                        }
                    else:
                        return {
                            "error": f"Folder '{source_path}' not found or empty.",
                            "status": 404,
                            "isFolder": True,
                        }

                for blob in blobs_to_copy:
                    relative_path = blob.name[len(source_prefix_normalized) :]
                    new_blob_name = destination_prefix_normalized + relative_path

                    # Check for existence at destination before copying
                    if destination_bucket.blob(new_blob_name).exists():
                        return {
                            "error": f"A file/folder with name '{new_blob_name}' already exists in the destination.",
                            "status": 409,  # Conflict
                            "isFolder": True,
                        }
                    source_bucket.copy_blob(
                        blob, destination_bucket, new_name=new_blob_name
                    )

                return {
                    "message": f"Folder '{source_path}' and its contents copied to '{destination_path}'.",
                    "bucket": destination_bucket_name,
                    "success": True,
                    "isFolder": True,
                    "status": 200,
                }
            else:
                # Handle single file copy
                destination_blob = destination_bucket.blob(destination_path)

                if not source_blob.exists():
                    return {
                        "error": f"Source file '{source_path}' not found.",
                        "status": 404,
                        "isFolder": False,
                    }

                if destination_blob.exists():
                    return {
                        "error": f"A file with name '{destination_path}' already exists in the destination.",
                        "isFolder": False,
                        "status": 409,  # Conflict
                    }
                new_blob = source_bucket.copy_blob(
                    source_blob, destination_bucket, new_name=destination_path
                )
                return {
                    "name": new_blob.name,
                    "bucket": new_blob.bucket.name,
                    "size": new_blob.size,
                    "contentType": new_blob.content_type,
                    "timeCreated": (
                        new_blob.time_created.isoformat()
                        if new_blob.time_created
                        else ""
                    ),
                    "updated": (
                        new_blob.updated.isoformat() if new_blob.updated else ""
                    ),
                    "success": True,
                    "isFolder": False,
                    "status": 200,
                }

        except Exception as e:
            self.log.exception(
                f"Error copying from {source_path} to {destination_path}."
            )
            return {"error": str(e), "status": 500}

    async def move_blob(
        self,
        source_bucket_name: str,
        source_path: str,
        destination_bucket_name: str,
        destination_path: str,
    ):
        """
        Moves a file or folder from a source location to a destination location.
        This handles both intra-bucket and inter-bucket moves.
        It uses a copy-then-delete strategy for all moves, as GCS does not support atomic cross-bucket moves.
        """
        try:
            source_path = (
                source_path.rstrip("/") if source_path.endswith("/") else source_path
            )
            destination_path = (
                destination_path.rstrip("/")
                if destination_path.endswith("/")
                else destination_path
            )
            # First, attempt to copy the file/folder
            copy_result = await self.copy_file(
                source_bucket_name,
                source_path,
                destination_bucket_name,
                destination_path,
            )

            if not copy_result.get("success"):
                return copy_result  # Return error if copy failed

            # If copy was successful, proceed to delete the source
            delete_result = await self.delete_file(source_bucket_name, source_path)

            if not delete_result.get("success"):
                # Log a warning if deletion fails after successful copy, but still report success for the move
                # as the file is now in the new location.
                self.log.warning(
                    f"Failed to delete source '{source_path}' from bucket '{source_bucket_name}' "
                    f"after successful copy to '{destination_path}' in '{destination_bucket_name}'. "
                    f"Error: {delete_result.get('error')}"
                )

            # Return the success message from the copy operation
            return copy_result

        except Exception as e:
            self.log.exception(
                f"Error moving '{source_path}' from '{source_bucket_name}' "
                f"to '{destination_path}' in '{destination_bucket_name}'."
            )
            return {"error": str(e), "status": 500}

    @ensure_client_setup
    async def download_file(self, bucket_name, file_path, name, format):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(file_path)

            return blob.download_as_bytes()

        except Exception as e:
            self.log.exception(f"Error getting file: {e}")
            return []  # Return Empty File
