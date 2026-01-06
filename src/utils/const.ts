/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { version } = require('../../package.json');
export const VERSION_DETAIL = version;

/** gcs extension */
export const GCS_PLUGIN_TITLE = 'Google Cloud Storage';
export const NAMESPACE = 'gcs-jupyter-plugin:gcsBrowser';
export const PLUGIN_ID = 'gcs-jupyter-plugin:plugin';

/** auth */
export const API_HEADER_CONTENT_TYPE = 'application/json';
export const STATUS_SUCCESS = 'SUCCEEDED';

/** API endpoints */
export const CREDENTIAL_ENDPOINT = 'credentials';
export const LOG_ENDPOINT = 'log';
export const LOGIN_ENDPOINT = 'login';
export const HEALTH_ENDPOINT = 'health';
export const PROJECT_LIST_ENDPOINT = 'api/storage/projectList';
export const LIST_BUCKETS_ENDPOINT = 'api/storage/listBuckets';
export const LIST_FILES_ENDPOINT = 'api/storage/listFiles';
export const LOAD_FILE_ENDPOINT = 'api/storage/loadFile';
export const CREATE_FOLDER_ENDPOINT = 'api/storage/createFolder';
export const SAVE_ENDPOINT = 'api/storage/saveFile';
export const DELETE_ENDPOINT = 'api/storage/deleteFile';
export const RENAME_ENDPOINT = 'api/storage/renameFile';
export const COPY_ENDPOINT = 'api/storage/copyFile';
export const DOWNLOAD_ENDPOINT = 'api/storage/downloadFile';

/** toolbar items ( Used in Name and tooltip ) */
export const NEW_FOLDER = 'New Folder';
export const FILE_UPLOAD = 'File Upload';
export const REFRESH = 'Refresh';
export const TOGGLE_FILE_FILTER = 'Toggle File Filter';

/** folder creation */
export const DIRECTORY = 'directory';
export const UNTITLED_DIRECTORY_NAME = 'UntitledFolder';

/** file creation */
export const FILE = 'file';
export const UNTITLED_FILE_NAME = 'untitled';
export const UNTITLED_FILE_EXT = '.txt';

/** notebook creation */
export const NOTEBOOK = 'notebook';
export const UNTITLED_NOTEBOOK_NAME = 'Untitled';
export const UNTITLED_NOTEBOOK_EXT = '.ipynb';

/** Jupyter signals */
export const DELETE_SIGNAL = 'delete';
export const RENAME_SIGNAL = 'rename';

export const ERROR_TEXT_INACCESSBILE_PROJECT =
  'You do not have permission to access project';
