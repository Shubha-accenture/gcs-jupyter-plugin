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

import { Contents, ServerConnection } from '@jupyterlab/services';
import { ISignal, Signal } from '@lumino/signaling';
import { GcsService } from './gcsService';

// Template for an empty Directory IModel.
const DIRECTORY_IMODEL: Contents.IModel = {
  type: 'directory',
  path: '',
  name: '',
  format: null,
  content: null,
  created: '',
  writable: true,
  last_modified: '',
  mimetype: ''
};

export class GCSDrive implements Contents.IDrive {
  constructor() {
    // Not actually used, but the Contents.IDrive interface requires one.
    this.serverSettings = ServerConnection.makeSettings();
  }

  private _isDisposed = false;
  private _fileChanged = new Signal<this, Contents.IChangedArgs>(this);

  get fileChanged(): ISignal<this, Contents.IChangedArgs> {
    return this._fileChanged;
  }
  // private _currentPrefix = '';
  readonly serverSettings: ServerConnection.ISettings;

  get name() {
    return 'gs';
  }

  get isDisposed(): boolean {
    return this._isDisposed;
  }

  dispose(): void {
    if (this.isDisposed) {
      return;
    }
    this._isDisposed = true;
    Signal.clearData(this);
  }

  async get(
    localPath: string,
    options?: Contents.IFetchOptions
  ): Promise<Contents.IModel> {
    /**
     * Logic here is kind of complicated, we have 3 cases that
     * the IDrive interface uses this call for.
     * 1) If path is the root node, list the buckets
     * 2) If path is a directory in a bucket, list all of it's directory and files.
     * 3) If path is a file, return it's metadata and contents.
     */
    if (localPath.length === 0) {
      // Case 1: Return the buckets.
      return await this.getBuckets();
    }

    // Case 2: Return the directory contents.
    const directory = await this.getDirectory(localPath);
    const name = localPath.split('/').pop() ?? ""; // Gets the last part of the path
    const isFile = name.includes('.') && name.lastIndexOf('.') > 0;
    if (directory.content.length === 0 && isFile) {
      // Case 3?: Looks like there's no items with this prefix and path is a file, so
      //  maybe it's a file?  Try fetching the file.
      try {
        return await this.getFile(localPath, options);
      } catch (e) {
        // If it's a 404, maybe it was an (empty) directory after all.
        // fall out and return the directory IModel.
      }
    }
    return directory;
  }

  /**
   * @returns IModel directory containing all the GCS buckets for the current project.
   */
  private async getBuckets() {
    let paragraph: HTMLElement | null;
    let searchInput = document.getElementById('filter-buckets-objects');
    //@ts-ignore

    let searchValue = searchInput.value;
    const content = await GcsService.listBuckets({
      prefix: searchValue
    });

    if (content?.error) {
      if (document.getElementById('gcs-list-bucket-error')) {
        document.getElementById('gcs-list-bucket-error')?.remove();
      }
      const para = document.createElement('p');
      para.id = 'gcs-list-bucket-error';
      para.style.color = '#ff0000';
      para.style.maxWidth = '100%';
      para.style.whiteSpace = 'normal';
      para.textContent = content?.error;
      paragraph = document.getElementById('filter-buckets-objects');
      paragraph?.after(para);
    } else {
      if (document.getElementById('gcs-list-bucket-error')) {
        document.getElementById('gcs-list-bucket-error')?.remove();
      }
    }

    if (!content) {
      throw `Error Listing Buckets ${content}`;
    }
     return {
      ...DIRECTORY_IMODEL,
      content:
        content.map((bucket: { items: { name: string, updated: string } }) => ({
          ...DIRECTORY_IMODEL,
          path: bucket.items.name,
          name: bucket.items.name,
          last_modified: bucket.items.updated ?? ''
        })) ?? []
    };
  }

  /**
   * @returns IModel directory for the given local path.
   */
  private async getDirectory(localPath: string) {
    const path = GcsService.pathParser(localPath);
    let searchInput = document.getElementById('filter-buckets-objects');
    //@ts-ignore
    let searchValue = searchInput.value;
    const prefix = path.path.length > 0 ? `${path.path}/` : path.path;
    const content = await GcsService.listFiles({
        prefix: prefix + searchValue,
        bucket: path.bucket,
    });
    if (!content) {
        throw 'Error Listing Objects';
    }
    let directory_contents: Contents.IModel[] = [];

    if (content.prefixes && content.prefixes.length > 0) {
      directory_contents = directory_contents.concat(
        content.prefixes
        .map((item: { prefixes: { name: string } }) => {
          const pref = item.prefixes.name;
          const path = pref.split('/');
          const name = path.at(-2) ?? prefix;
          return {
            ...DIRECTORY_IMODEL,
            path: `${localPath}/${name}`,
            name: name
          };
        })
      ); 
    }

    if (content.files && content.files.length > 0) {
        directory_contents = directory_contents.concat(
          content.files.map((item: { items: { name: string; updated: string; size: number; content_type: string; timeCreated: string; } }) => {
            const itemName = item.items.name!;
            const pathParts = itemName.split('/');
            const name = pathParts.at(-1) ?? itemName;
            return {
                type: 'file',
                path: `${localPath}/${name}`,
                name: name,
                format: 'base64',
                content: null,
                created: item.items.timeCreated ?? '',
                writable: true,
                last_modified: item.items.updated ?? '',
                mimetype: item.items.content_type ?? '',
                size: item.items.size
            };
        }));
    }

    return {
        ...DIRECTORY_IMODEL,
        path: localPath,
        name: localPath.split('\\').at(-1) ?? '',
        content: directory_contents,
    };
  }

  /**
   * @returns IModel file for the given local path.
   */
  private async getFile(
    localPath: string,
    options?: Contents.IFetchOptions
  ): Promise<Contents.IModel> {
    const path = GcsService.pathParser(localPath);
    const content = await GcsService.loadFile({
      path: path.path,
      bucket: path.bucket,
      format: options?.format ?? 'text'
    });
    if (!content) {
      throw 'Error Listing Objects';
    }
    return {
      type: 'file',
      path: localPath,
      name: localPath.split('\\').at(-1) ?? '',
      format: options?.format ?? 'text',
      content: content,
      created: '',
      writable: true,
      last_modified: '',
      mimetype: ''
    };
  }

  getDownloadUrl(localPath: string): Promise<string> {
    throw new Error('Method not implemented.');
  }
  newUntitled(options?: Contents.ICreateOptions): Promise<Contents.IModel> {
    throw new Error('Method not implemented.');
  }
  delete(localPath: string): Promise<void> {
    throw new Error('Method not implemented.');
  }
  rename(oldLocalPath: string, newLocalPath: string): Promise<Contents.IModel> {
    throw new Error('Method not implemented.');
  }
  save(localPath: string, options?: Partial<Contents.IModel>): Promise<Contents.IModel> {
    throw new Error('Method not implemented.');
  }
  copy(localPath: string, toLocalDir: string): Promise<Contents.IModel> {
    throw new Error('Method not implemented.');
  }
  createCheckpoint(localPath: string): Promise<Contents.ICheckpointModel> {
    throw new Error('Method not implemented.');
  }
  listCheckpoints(localPath: string): Promise<Contents.ICheckpointModel[]> {
    throw new Error('Method not implemented.');
  }
  restoreCheckpoint(localPath: string, checkpointID: string): Promise<void> {
    throw new Error('Method not implemented.');
  }
  deleteCheckpoint(localPath: string, checkpointID: string): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
