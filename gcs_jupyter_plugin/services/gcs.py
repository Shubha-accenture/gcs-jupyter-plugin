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
import io
import aiohttp
import mimetypes
import base64
from datetime import timedelta

import tornado.ioloop
import tornado.web

from tornado import gen

from google.oauth2 import credentials
from google.cloud import storage
import proto

from gcs_jupyter_plugin import urls
from gcs_jupyter_plugin.commons.constants import CONTENT_TYPE, STORAGE_SERVICE_NAME


class Client (tornado.web.RequestHandler):
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

    async def list_buckets(self, prefix=None):
        try:
            bucket_list = []
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            buckets = client.list_buckets()
            buckets = client.list_buckets(prefix=prefix)
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
    async def list_files(self, bucket , prefix):
        try:
            result = {}
            file_list = []
            subdir_list = []
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            blobs = client.list_blobs(bucket , prefix=prefix, delimiter="/")
            bucketObj = client.bucket(bucket)
            files = list(blobs)

            # Prefixes dont have crreated / updated at data with Object. So we have to run through loop
            # and hit client.list_blobs() with each prefix to load blobs to get updated date info ( we can set max_result=1 ).
            # This is taking time when loop runs. So to avoid this, Grouping prefix with updated/created date
            prefix_latest_updated = {}
            if blobs.prefixes:
                all_blobs_under_prefix = client.list_blobs(bucket, prefix=prefix)
                for blob in all_blobs_under_prefix:
                    relative_name = blob.name[len(prefix or ''):]
                    parts = relative_name.split('/', 1)
                    if len(parts) > 1:
                        subdirectory = prefix + parts[0] + '/'
                        if subdirectory in blobs.prefixes:
                            if subdirectory not in prefix_latest_updated or (blob.updated and prefix_latest_updated[subdirectory] < blob.updated):
                                prefix_latest_updated[subdirectory] = blob.updated

            # Adding Sub-directories
            if blobs.prefixes:
                for pref in blobs.prefixes:
                    
                    subdir_name = pref[:-1]
                    subdir_list.append(
                        {
                            "prefixes": {
                                "name": pref,
                                "updatedAt": prefix_latest_updated.get(pref).isoformat() if prefix_latest_updated.get(pref) else ""
                            }
                        }
                    )
            
            # Adding Files
            for file in files:
                if not (file.name == prefix and file.size == 0):
                    file_list.append(
                        {
                            "items": {
                                "name": file.name,
                                "timeCreated": file.time_created.isoformat() if file.time_created else "",
                                "updated": file.updated.isoformat() if file.updated else "",
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
            return [] #Return empty list on error.

    async def get_file(self, bucket_name, file_path , format):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            if format == 'base64':
                file_content = blob.download_as_bytes()
                try:
                    base64_encoded = base64.b64encode(file_content).decode('utf-8')
                    return base64_encoded
                except Exception as encode_error:
                    return []
            elif format == 'json':
                file_content = blob.download_as_text()
                return file_content
            else:
                return blob.download_as_text()

        except Exception as e:
            self.log.exception(f"Error getting file: {e}")
            return [] #Return empty list on error.

    async def create_folder(self, bucket, path, folder_name):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)

            # Format the folder path
            new_folder_path = (
                folder_name + "/" if path == "" else path + "/" + folder_name + "/"
            )

            # Get the bucket
            bucket_obj = client.bucket(bucket)
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

    async def save_content(self, bucket_name, destination_blob_name, content, uploadFlag):
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
            # Ensure content is in string format if it's not already
            if isinstance(content, dict):
                content = json.dumps(content)

            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            storage_client = storage.Client(project=project, credentials=creds)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            if blob.exists() and uploadFlag: # when uploadFlag false, user is peroforming save. So file should present.
                return {
                    "name": destination_blob_name,
                    "bucket": bucket_name,
                    "exists": True,
                    "success": False,
                    "error": f"A file with name {destination_blob_name} already exists in the destination.",
                    "status": 409, # Conflict
                }
    
            blob.upload_from_string(
                content,
                content_type="media",
            )

            return {
                "name": destination_blob_name,
                "bucket": bucket_name,
                "size": blob.size,
                "contentType": blob.content_type,
                "timeCreated": (
                    blob.time_created.isoformat() if blob.time_created else ""
                ),
                "updated": blob.updated.isoformat() if blob.updated else "",
                "success": True,
            }

        except Exception as e:
            if uploadFlag:
                self.log.exception(f"Error uploading content to {destination_blob_name}.")
            else:
                self.log.exception(f"Error saving content to {destination_blob_name}.")
            return {"error": str(e), "status": 500}

    
    async def delete_file(self, bucket, path):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)

            # Get the bucket
            bucket_obj = client.bucket(bucket)

            # Check if it's a folder/bucket deletion attempt
            if path == "" or path == "/":
                return {
                    "error": "Deleting Bucket is not allowed.",
                    "status": 409,
                }

            # retriving blob
            blob = bucket_obj.blob(path)

            isFile = True
            
            if not blob.exists():
                # using blobs , we can exclude the 0 byte blob and count the children
                blobs = bucket_obj.list_blobs(prefix=path+"/")

                blob_count = 0
                for iblob in blobs:
                    # For empty folders, gcs creates a zero-byte object with a trailing slash to simulate a folder.
                    # here we exclude that 0 byte object.
                    blob = iblob
                    isFile = False
                    if (iblob.name[:-1] if iblob.name.endswith('/') else iblob.name) != path:
                        blob_count += 1
                        # breaking the loop here, since we just want to know whether at-least 1 file present or not.
                        # Folder cannot be deleted even if 1 file/folder present
                        break

                if blob_count > 0:
                    return {
                        "error": "Non-Empty folder cannot be deleted.",
                        "status": 409,
                    }

            # Checking whether blob exists
            if not blob.exists():
                # Without trailing slash, 0 byte object wont be pointed out. 
                # So, In the case of Empty folder, blob.exists() returns false and causes 404.
                blob = bucket_obj.blob(path+"/")
                if not blob.exists():
                    return {"error": "File/Folder not found.", "status": 404}

            # Attempt to delete the blob
            try:
                blob.delete()
                return {"success": True}
            except Exception as e:
                self.log.exception(f"Error deleting file/folder {path}.")
                return {"error": str(e), "status": 500}

        except Exception as e:
            self.log.exception(f"Error deleting file {path}.")
            return {"error": str(e), "status": 500}

    async def rename_file(self, bucket_name, blob_name, new_name):
        """
        Renames a blob using the rename_blob method.
        Note: This only works within the same bucket.
        """
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            storage_client = storage.Client(project=project, credentials=creds)

            # Get the bucket and blob
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            # Check if source blob exists
            isFile = True
            if not blob.exists():
                # It might be a folder, so adding trail slash and checking for a blob (0 byte object will be returned)
                # using blobs , we can exclude the 0 byte blob and count the children
                blobs = bucket.list_blobs(prefix=(blob_name if blob_name.endswith('/') else blob_name+"/"))

                blob_count = 0
                for iblob in blobs:
                    # For empty folders, gcs creates a zero-byte object with a trailing slash to simulate a folder.
                    # here we exclude that 0 byte object.
                    blob = iblob
                    if (iblob.name[:-1] if iblob.name.endswith('/') else iblob.name) != blob_name:
                        blob_count += 1
                        # breaking the loop here, since we just want to know whether at-least 1 file present or not.
                        # Folder cannot be renamed even if 1 file/folder present
                        if blob_count > 1:
                            break
                if blob.exists() and blob_count == 0 and (blob.name[:-1] if blob.name.endswith('/') else blob.name) == blob_name:
                    # Only 0 byte Object present
                    isFile = False
                elif blob_count > 0:
                    return {
                        "error": "Non-Empty folder cannot be renamed.",
                        "status": 409,
                    }
                else:
                    return {"error": f"{blob_name} not found",
                        "status": 404}

            # Check for availability of new name ( if already present, return error)
            if isFile:
                blobNew = bucket.blob(new_name)

                if blobNew.exists():
                    return {"error": f"A file with name {blobNew.name} already exists in the destination.",
                            "status": 409}
            else:
                # Adding Trailing slash to avoid partial match of other folders
                blobNew = bucket.blob(new_name)
                blobs = bucket.list_blobs(prefix=new_name+"/")
                if any(blobs):
                    return {"error": f"A folder with name {blobNew.name} already exists in the destination.",
                            "status": 409}

            # Rename the blob
            if isFile:
                new_blob = bucket.rename_blob(blob, new_name)
            else:
                new_blob = bucket.rename_blob(blob, new_name+"/")

            # Return success response
            return {"name": new_blob.name, "bucket": bucket_name,
                         "success": True , "status" : 200 }

        except Exception as e:
            self.log.exception(f"Error renaming from {blob_name} to {new_name}.")
            return {"error": str(e), "status": 500}

    
    async def download_file(self, bucket_name, file_path , name , format):
        try:
            token = self._access_token
            project = self.project_id
            creds = credentials.Credentials(token)
            client = storage.Client(project=project, credentials=creds)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_path)

            return blob.download_as_bytes()

        except Exception as e:
            self.log.exception(f"Error getting file: {e}")
            return [] #Return Empty File