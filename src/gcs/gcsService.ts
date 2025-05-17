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
import { authApi } from '../utils/utils';
import { showDialog, Dialog } from '@jupyterlab/apputils';

export class GcsService {
  /**
   * Translate a Jupyter Lab file path into tokens.  IE.
   *   gs:bucket-name/directory/file.ipynb
   * (Note that this isn't exactly a gsutil compatible URI)
   * Would translate to:
   * {
   *   bucket: 'bucket-name',
   *   path: 'directory/file.ipynb',
   *   name: 'file.ipynb'
   * }
   * @param localPath The absolute Jupyter file path
   * @returns Object containing the GCS bucket and object ID
   */
  static pathParser(localPath: string) {
    const matches = /^(?<bucket>[\w\-\_\.]+)\/?(?<path>.*)/.exec(
      localPath
    )?.groups;
    if (!matches) {
      throw 'Invalid Path';
    }
    const path = matches['path'];
    return {
      path: path,
      bucket: matches['bucket'],
      name: path.split('/').at(-1)
    };
  }

  /**
   * Thin wrapper around storage.bucket.list
   * @see https://cloud.google.com/storage/docs/listing-buckets#rest-list-buckets
   */
  static async listBuckets({ prefix }: { prefix: string }) {
    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }
    const data = (await requestAPI(
      `api/storage/listBuckets?prefix=${prefix}`
    )) as any;
    return data;
  }


  /**
   * Thin wrapper around storage.object.list
   * @see https://cloud.google.com/storage/docs/listing-objects
   */
  static async listFiles({ prefix, bucket }: { prefix: string; bucket: string }) {
    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }
    const data = (await requestAPI(
      `api/storage/listFiles?prefix=${prefix}&bucket=${bucket}`
    )) as any;
    return data;
  }

  /**
   * Thin wrapper around storage.object.download-into-memory
   * @see https://cloud.google.com/storage/docs/downloading-objects-into-memory
   */
  static async loadFile({bucket, path, format }: {
    bucket: string; path: string; format: 'text' | 'json' | 'base64';
    }
  ): Promise<string> {

    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }
    const data = (await requestAPI(
      `api/storage/loadFile?bucket=${bucket}&path=${path}&format=${format}`
    )) as any;

    return data;
  }

  /**
   * Thin wrapper around storage.folder.create
   * @see https://cloud.google.com/storage/docs/create-folders
   */
  static async createFolder({
    bucket,
    path,
    folderName
    }: {
      bucket: string;
      path: string;
      folderName: string;
    }) {
      const credentials = await authApi();
      if (!credentials) {
        throw 'not logged in';
      }
      const data = await requestAPI('api/storage/createFolder', {
        method: 'POST',
        body: JSON.stringify({
          bucket,
          path,
          folderName
        })
      });
    return data;
  }

  /**
   * Thin wrapper around storage.object.upload
   * @see https://cloud.google.com/storage/docs/uploading-objects-from-memory
   */
  static async saveFile({
    bucket,
    path,
    contents,
    upload = false
  }: {
    bucket: string;
    path: string;
    contents: Blob | string;
    upload?: boolean 
  }) {
    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }

    try {
      // Create form data to send the file
      const formData = new FormData();
      formData.append('bucket', bucket);
      formData.append('path', path);
      formData.append('contents', contents);
      formData.append('upload', String(upload));
      
      const response = await requestAPI('api/storage/saveFile', {
        method: 'POST',
        body: formData,
      });
      
      return response;
    } catch (error: any) {
      throw error?.message || 'Error saving file';
    }
  }

  /**
   * Thin wrapper around storage.object.delete
   * @see https://cloud.google.com/storage/docs/deleting-objects
   */
  static async deleteFile({ bucket, path }: { bucket: string; path: string }) {
    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }
    try {
      const response: { status?: number, error?: string } = await requestAPI('api/storage/deleteFile', {
        method: 'POST',
        body: JSON.stringify({
          bucket,
          path
        })
      });
      
      return response;
    } catch (error: unknown) {
      if (typeof error === 'string') {
        throw error; 
      } else {
        throw 'Error deleting file';
      }
    }
  }

  /**
   * Thin wrapper around storage.object.rename
   * @see https://cloud.google.com/storage/docs/copying-renaming-moving-objects
   */
  static async renameFile({
    oldBucket,
    oldPath,
    newBucket,
    newPath
  }: {
    oldBucket: string;
    oldPath: string;
    newBucket: string;
    newPath: string;
  }) {
    const credentials = await authApi();
    if (!credentials) {
      throw 'not logged in';
    }
    try {
      const response: { status?: number, error?: string } = await requestAPI('api/storage/renameFile', {
        method: 'POST',
        body: JSON.stringify({
          oldBucket,
          oldPath,
          newBucket,
          newPath
        })
      });

      return response;
    } catch (error: any) {
      await showDialog({
        title: 'Rename Error',
        body: 'Error renaming file',
        buttons: [Dialog.okButton()]
      });
      console.error('Error during rename operation:', error);
      throw error?.message || 'Error renaming file';
    }
  }
}