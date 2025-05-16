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

import { requestAPI } from '../handler';
const { version } = require('../../package.json');
export const VERSION_DETAIL = version;

interface IGcpUrlResponseData {

  storage_url: string;
}
export const gcpServiceUrls = (async () => {
  const data = (await requestAPI('getGcpServiceUrls')) as IGcpUrlResponseData;
  const storage_url = new URL(data.storage_url);
  const storage_upload_url = new URL(data.storage_url);

  if (
    !storage_url.pathname ||
    storage_url.pathname === '' ||
    storage_url.pathname === '/'
  ) {
    // If the overwritten  storage_url doesn't contain a path, add it.
    storage_url.pathname = 'storage/v1/';
  }
  storage_upload_url.pathname = 'upload/storage/v1/';

  return {
    STORAGE: storage_url.toString(),
    STORAGE_UPLOAD: storage_upload_url.toString()
  };
})();

export const API_HEADER_CONTENT_TYPE = 'application/json';
export const API_HEADER_BEARER = 'Bearer ';
export const STATUS_SUCCESS = 'SUCCEEDED';
