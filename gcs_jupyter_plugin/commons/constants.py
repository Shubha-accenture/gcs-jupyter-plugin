# Copyright 2023 Google LLC
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

GCS = "gs://"

STORAGE_SERVICE_NAME = "storage"
COMPUTE_SERVICE_DEFAULT_URL = "https://compute.googleapis.com/compute/v1"
STORAGE_SERVICE_DEFAULT_URL = "https://storage.googleapis.com"
CONTENT_TYPE = "application/json"

MISSING_REQUIRED_PARAMETERS_ERROR_MESSAGE = "Missing required parameters."

BINARY_ENCODING_EXTENSIONS = (".parquet", "avro", "orc", ".png", ".jpg", ".jpeg", ".gif", ".pdf")

MIMETYPE_MAP = {
                ".json": "application/json",
                ".txt": "text/plain",
                ".csv": "text/plain",
                ".ipynb": "application/x-ipynb+json",
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".gif": "image/gif",
                ".pdf": "application/pdf",
                ".html": "text/html",
            }