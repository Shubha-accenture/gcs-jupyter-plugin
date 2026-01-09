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
import { showDialog, Dialog } from '@jupyterlab/apputils';
import {
  COPY_ENDPOINT,
  CREATE_FOLDER_ENDPOINT,
  DELETE_ENDPOINT,
  LIST_BUCKETS_ENDPOINT,
  LIST_FILES_ENDPOINT,
  LOAD_FILE_ENDPOINT,
  PROJECT_LIST_ENDPOINT,
  RENAME_ENDPOINT,
  SAVE_ENDPOINT,
  SET_PROJECT_ENDPOINT
} from '../utils/const';

export class GcsService {
  private static _currentProjectId: string | null = null;

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
    console.log('local path', localPath);
    const matches = /^(?<bucket>[\w\-\_\.]+)\/?(?<path>.*)/.exec(
      localPath
    )?.groups;
    if (!matches) {
      throw new Error('Invalid Path');
    }
    const path = matches['path'];
    console.log(' path', path);
    return {
      path: path,
      bucket: matches['bucket'],
      name: path.split('/').at(-1)
    };
  }

  /**
   * Sets project ID
   */
  static async setProject(projectID: string) {
    try {
      const data = (await requestAPI(
        `${SET_PROJECT_ENDPOINT}?project_id=${encodeURIComponent(projectID)}`
      )) as any;
      this._currentProjectId = projectID;
      return data;
    } catch (error: any) {
      console.error(error?.message ?? 'Error fetching Projects List');
    }
  }

  // 3. Add this getter so GCSDrive can check if it's safe to list buckets
  static getCurrentProject(): string | null {
    return this._currentProjectId;
  }

  /**
   * List projects available to the user
   */
  static async getProjectsList() {
    try {
      const data = (await requestAPI(PROJECT_LIST_ENDPOINT)) as any;
      return data;
    } catch (error: any) {
      console.error(error?.message ?? 'Error fetching Projects List');
    }
  }

  /**
   * Thin wrapper around storage.bucket.list
   * @see https://cloud.google.com/storage/docs/listing-buckets#rest-list-buckets
   */
  static async listBuckets() {
    try {
      const listBucketUrl = LIST_BUCKETS_ENDPOINT;
      const data = (await requestAPI(listBucketUrl)) as any;
      return data;
    } catch (error: any) {
      console.error(error?.message ?? 'Error fetching Buckets');
    }
  }

  /**
   * Thin wrapper around storage.object.list
   * @see https://cloud.google.com/storage/docs/listing-objects
   */
  static async listFiles({
    prefix,
    bucket
  }: {
    prefix: string;
    bucket: string;
  }) {
    const url = `${LIST_FILES_ENDPOINT}?prefix=${encodeURIComponent(prefix)}&bucket=${encodeURIComponent(bucket)}`;

    const data = (await requestAPI(url)) as any;

    return data;
  }

  /**
   * Thin wrapper around storage.object.download-into-memory
   * @see https://cloud.google.com/storage/docs/downloading-objects-into-memory
   */
  static async loadFile({
    bucket,
    path,
    format
  }: {
    bucket: string;
    path: string;
    format: 'text' | 'json' | 'base64';
  }): Promise<string> {
    const data = (await requestAPI(LOAD_FILE_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({
        bucket,
        path,
        format
      })
    })) as any;

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
    const data = await requestAPI(CREATE_FOLDER_ENDPOINT, {
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
    upload?: boolean;
  }) {
    try {
      // Create form data to send the file
      const formData = new FormData();
      formData.append('bucket', bucket);
      formData.append('path', path);
      formData.append('contents', contents);
      formData.append('upload', String(upload));

      const response = await requestAPI(SAVE_ENDPOINT, {
        method: 'POST',
        body: formData
      });

      return response;
    } catch (error: any) {
      console.error(error?.message ?? 'Error saving file');
    }
  }

  /**
   * Thin wrapper around storage.object.delete
   * @see https://cloud.google.com/storage/docs/deleting-objects
   */
  static async deleteFile({ bucket, path }: { bucket: string; path: string }) {
    try {
      const response: { status?: number; error?: string } = await requestAPI(
        DELETE_ENDPOINT +
          '?bucket=' +
          encodeURIComponent(bucket) +
          '&path=' +
          encodeURIComponent(path),
        {
          method: 'DELETE'
        }
      );

      return response;
    } catch (error: unknown) {
      if (typeof error === 'string') {
        throw error;
      } else {
        throw new Error('Error deleting file');
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
    try {
      const response: { status?: number; error?: string } = await requestAPI(
        RENAME_ENDPOINT,
        {
          method: 'PATCH',
          body: JSON.stringify({
            oldBucket,
            oldPath,
            newBucket,
            newPath
          })
        }
      );

      return response;
    } catch (error: any) {
      await showDialog({
        title: 'Rename Error',
        body: 'Error renaming file',
        buttons: [Dialog.okButton()]
      });
      console.error('Error during rename operation:', error);
    }
  }

  /**
   * Thin wrapper around storage.object.copy
   * @see https://cloud.google.com/storage/docs/copying-objects
   */
  static async copyFile({
    sourceBucket,
    sourcePath,
    destinationBucket,
    destinationPath
  }: {
    sourceBucket: string;
    sourcePath: string;
    destinationBucket: string;
    destinationPath: string;
  }) {
    const response: { status?: number; isFolder: boolean; error?: string } =
      await requestAPI(COPY_ENDPOINT, {
        method: 'POST',
        body: JSON.stringify({
          sourceBucket,
          sourcePath,
          destinationBucket,
          destinationPath
        })
      });

    return response;
  }

  /**
   * Thin wrapper around storage.object.download
   * @see https://cloud.google.com/storage/docs/downloading-objects#rest-download-object
   */
  static async downloadFile({
    bucket,
    path,
    name,
    format
  }: {
    bucket: string;
    path: string;
    name: string;
    format: 'text' | 'json' | 'base64';
  }): Promise<string> {
    const response = (await requestAPI(LOAD_FILE_ENDPOINT, {
      method: 'POST',
      body: JSON.stringify({
        bucket,
        path,
        name,
        format
      })
    })) as any;

    return response;
  }
}
