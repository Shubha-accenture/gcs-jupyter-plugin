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

import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { showDialog, Dialog } from '@jupyterlab/apputils';
import mime from 'mime-types';

import {
  toastifyCustomStyle,
} from '../utils/utils';

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


let untitledFolderSuffix = '';
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

  async newUntitled(
    options?: Contents.ICreateOptions
  ): Promise<Contents.IModel> {

    // Validating parameters
    if (!options) { 
      console.error("No data provided for this operation. :", options);
        return Promise.reject('No data provided for this operation.');
    }
    else if(!options.path){ // Checkpoint for Bucket Level Object Creation
      if (options.type === 'directory'){
        console.error("Creating Folders at bucket level is not allowed. Note : Please use console to create new bucket :", options);
        await showDialog({
          title: 'Create Bucket Error',
          body: 'Folders cannot be created outside of a bucket.',
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
      else if (options.type === 'file'){
        console.error("Creating files at bucket level is not allowed :", options);
        await showDialog({
          title: 'Error Creating File',
          body: 'Files cannot be created outside of a bucket.',
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
      else if (options.type === 'notebook'){
        console.error("Creating notebooks at bucket level is not allowed :", options);
        await showDialog({
          title: 'Error Creating Notebook',
          body: 'Notebooks cannot be created outside of a bucket.',
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }else{
        console.error("Unsupported creation type :", options.type);
        await showDialog({
          title: 'Error',
          body: 'Unsupported creation type :' + options.type,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
    }

    // Extract the localPath from options
    let localPath = typeof options?.path == 'string' ? options?.path : '';

    // Check if the provided path is valid and not the root directory
    if (localPath === '/' || localPath === '') {
      console.error("Cannot create new objects in the root directory:", localPath);
      return Promise.reject('Cannot create new objects in the root directory.');
    }

    const parsedPath = GcsService.pathParser(localPath);

    if (options.type === 'directory') {
  
      const content = await GcsService.listFiles({
        prefix:
        parsedPath.path === ''
            ? parsedPath.path + 'UntitledFolder'
            : parsedPath.path + '/UntitledFolder',
        bucket: parsedPath.bucket
      });

      if (content.prefixes) {
        let maxSuffix  = 1;
        content.prefixes.forEach((data: { prefixes :{ name: string; updatedAt: string }}) => {
          const parts = data.prefixes.name.split('/');
          if (parts.length >= 2) {
            const potentialSuffix = parts[parts.length - 2];
            const suffixElement = potentialSuffix.match(/\d+$/);
            if (suffixElement !== null && parseInt(suffixElement[0]) >= maxSuffix) {
              maxSuffix = parseInt(suffixElement[0]) + 1;
            }
          }
          untitledFolderSuffix = maxSuffix.toString();
        });
      } else {
        untitledFolderSuffix = '';
      }
      let folderName = 'UntitledFolder' + untitledFolderSuffix;
  
      // Create the folder in your backend service
      const response = await GcsService.createFolder({
        bucket: parsedPath.bucket,
        path: parsedPath.path,
        folderName: folderName
      });
  
      // Handle the response from your backend service appropriately
      if (response) {
        // Folder created successfully, return the folder metadata
        return {
          type: 'directory',
          path: localPath + (localPath.endsWith('/') ? folderName : '/' + folderName),
          name: folderName,
          format: null,
          created: new Date().toISOString(),
          writable: true,
          last_modified: new Date().toISOString(),
          mimetype: '',
          content: null
        };
      } else {
        // Handle folder creation failure
        console.error("Failed to create folder.");
        await showDialog({
          title: 'Error Creating Folder',
          body: `Folder ${folderName} creation is failed.`,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
    }
    else if (options.type === 'file') {

      const content = await GcsService.listFiles({
        prefix:
        parsedPath.path === ''
            ? parsedPath.path + 'untitled'
            : parsedPath.path + '/untitled',
        bucket: parsedPath.bucket
      });
      
      let maxSuffix = 1;
      let baseFileName = 'untitled';
      let fileExtension = '.txt'; // Default extension

      if (content.files) {
        content.files.forEach((file: { items: { name: string } }) => {
          const nameParts = file.items.name.split('/');
          const fileName = nameParts.at(-1) ?? '';
          const baseNameMatch = fileName.match(/^untitled(\d*)(\..*)?$/);
          if (baseNameMatch) {
            const suffix = baseNameMatch[1];
            const ext = baseNameMatch[2] || '.txt';
            if (ext === fileExtension && suffix) {
              const num = parseInt(suffix);
              if (!isNaN(num) && num >= maxSuffix) {
                maxSuffix = num + 1;
              }
            } else if (ext === fileExtension && maxSuffix === 1 && fileName === 'untitled.txt') {
              maxSuffix = 2;
            }
          }
        });
      }

      const newFileName = maxSuffix > 1 ? `${baseFileName}${maxSuffix}${fileExtension}` : `${baseFileName}${fileExtension}`;
      //const newFilePath = parsedPath.path === '' ? newFileName : `${parsedPath.path}/${newFileName}`;

      // Logic for creating a new file with a specific name
      const filePathInGCS = parsedPath.path === '' ? newFileName : `${parsedPath.path}/${newFileName}`;
      
      const response = await GcsService.saveFile({
        bucket: parsedPath.bucket,
        path: filePathInGCS,
        contents: ''
      });
      

      if (response) {
        const parts = newFileName.split('.');
        const ext = parts.length > 1 ? `.${parts.slice(1).join('.')}` : '';
        const mimetype = ext === '.json' ? 'application/json' : 'text/plain'; // Basic MIME type detection

        return {
          type: 'file',
          path: `${localPath}/${newFileName}`,
          name: newFileName,
          format: 'text', // Default format
          content: '',
          created: new Date().toISOString(),
          writable: true,
          last_modified: new Date().toISOString(),
          mimetype: mimetype
        };
      } else {
        console.error("Failed to create file.");
        await showDialog({
          title: 'Error Creating File',
          body: `File ${newFileName} creation is failed.`,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
    } 
    else if (options.type === 'notebook') {
      const notebookExtension = '.ipynb';
      const baseNotebookName = 'Untitled';

      const content = await GcsService.listFiles({
        prefix:
          parsedPath.path === ''
            ? parsedPath.path + baseNotebookName
            : parsedPath.path + '/' + baseNotebookName,
        bucket: parsedPath.bucket
      });

      let maxSuffix = 1;

      if (content.files) {
        content.files.forEach((file: { items: { name: string } }) => {
          const nameParts = file.items.name.split('/');
          const fileName = nameParts.at(-1) ?? '';
          const baseNameMatch = fileName.match(/^Untitled(\d*)(\.ipynb)?$/);
          if (baseNameMatch) {
            const suffix = baseNameMatch[1];
            const ext = baseNameMatch[2];
            if (ext === notebookExtension && suffix) {
              const num = parseInt(suffix);
              if (!isNaN(num) && num >= maxSuffix) {
                maxSuffix = num + 1;
              }
            } else if (ext === notebookExtension && maxSuffix === 1 && fileName === 'Untitled.ipynb') {
              maxSuffix = 2;
            }
          }
        });
      }

      const newNotebookName = maxSuffix > 1 ? `${baseNotebookName}${maxSuffix}${notebookExtension}` : `${baseNotebookName}${notebookExtension}`;
      const filePathInGCS = parsedPath.path === '' ? newNotebookName : `${parsedPath.path}/${newNotebookName}`;

      const response = await GcsService.saveFile({
        bucket: parsedPath.bucket,
        path: filePathInGCS,
        contents: JSON.stringify({
          cells: [],
          metadata: {
            kernelspec: {
              display_name: 'Python 3', // Default kernel
              language: 'python',
              name: 'python3'
            },
            language_info: {
              codemirror_mode: {
                name: 'ipython',
                version: 3
              },
              file_extension: '.py',
              mimetype: 'text/x-python',
              name: 'python',
              nbconvert_exporter: 'python',
              pygments_lexer: 'ipython3',
              version: '3.x.x'
            }
          },
          nbformat: 4,
          nbformat_minor: 5
        })
      });

      if (response) {
        return {
          type: 'notebook',
          path: `${localPath}/${newNotebookName}`,
          name: newNotebookName,
          format: 'json', // Notebooks are JSON
          content: null, // Content will be fetched separately
          created: new Date().toISOString(),
          writable: true,
          last_modified: new Date().toISOString(),
          mimetype: 'application/x-ipynb+json'
        };
      } else {
        console.error("Failed to create notebook.");
        await showDialog({
          title: 'Error Creating Notebook',
          body: `Notebook ${newNotebookName} creation failed.`,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
    }
    else {
      console.warn(`Unsupported creation type: ${options.type}`);
      await showDialog({
        title: 'Unsupported Type Error',
        body: `Unsupported creation type: ${options.type}.`,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }

  }

  async save(
    localPath: string,
    options?: Partial<Contents.IModel>
  ): Promise<Contents.IModel> {
    const path = GcsService.pathParser(localPath);
    const content =
      options?.format == 'json'
        ? JSON.stringify(options.content)
        : options?.content;
    const resp = await GcsService.saveFile({
      bucket: path.bucket,
      path: path.path,
      contents: content
    });
    toast.success(
      `${path.name} saved successfully.`,
      toastifyCustomStyle
    );
    return {
      type: 'file',
      path: localPath,
      name: localPath.split('\\').at(-1) ?? '',
      format: 'text',
      created: '',
      content: '',
      writable: true,
      last_modified: (resp as { updated?: string }).updated ?? '',
      mimetype: '',
      ...options
    };
  }

  async delete(path: string): Promise<void> {
    const localPath = GcsService.pathParser(path);
    const response = await GcsService.deleteFile({
      bucket: localPath.bucket,
      path: localPath.path
    });

    const name = path.split('/').pop() ?? ""; // Gets the last part of the path
    const isFile = name.includes('.') && name.lastIndexOf('.') > 0;

    if(response.status === 200 || response.status ===204){
      if (isFile){
        toast.success(
          `File ${name} deleted successfully.`,
          toastifyCustomStyle
        );
      }else{
        toast.success(
          `Folder ${name} deleted successfully.`,
          toastifyCustomStyle
        );
      }

      this._fileChanged.emit({
        type: 'delete',
        oldValue: { path },
        newValue: null
      });
    }else{
      await showDialog({
        title: 'Deletion Error',
        body: response.error,
        buttons: [Dialog.okButton()]
      });
    }
  }

  async rename(
    path: string,
    newLocalPath: string,
    options?: Contents.IFetchOptions
  ): Promise<Contents.IModel> {
    const oldPath = GcsService.pathParser(path);
    const newPath = GcsService.pathParser(newLocalPath);

    const oldName = path.split('/').pop() ?? "";
    const isOldPathMeetsFilename = oldName.includes('.') && oldName.lastIndexOf('.') > 0;

    const newName = newLocalPath.split('/').pop() ?? "";
    const isNewPathMeetsFilename = newName.includes('.') && newName.lastIndexOf('.') > 0;


    if (
      newLocalPath.split('/')[newLocalPath.split('/').length - 1].length >= 1024
    ) {
      await showDialog({
        title: 'Rename Error',
        body: 'The maximum object length is 1024 characters.',
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }
    if (
      (!isOldPathMeetsFilename && oldPath.path === "")
    ) {
      await showDialog({
        title: 'Rename Error',
        body: 'Renaming Bucket is not allowed.',
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }else if(isOldPathMeetsFilename && !isNewPathMeetsFilename){
      // Old path has file name and New file name given dont have extension
      await showDialog({
        title: 'Rename Error',
        body: 'Invalid File Name Provided.',
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    } else {
      if (oldPath.path.includes('UntitledFolder' + untitledFolderSuffix)) {
        oldPath.path = oldPath.path + '/';
        newPath.path = newPath.path + '/';
        path = path + '/';
      }
      const response = await GcsService.renameFile({
        oldBucket: oldPath.bucket,
        oldPath: oldPath.path,
        newBucket: newPath.bucket,
        newPath: newPath.path
      });

      if (response.status === 200) {
        await GcsService.deleteFile({
          bucket: oldPath.bucket,
          path: oldPath.path
        });

        if (isOldPathMeetsFilename){
          toast.success(
            `File ${oldName} successfully renamed to ${newName}.`,
            toastifyCustomStyle
          );
          return {
            type: 'file',
            path: newLocalPath,
            name: newLocalPath.split('\\').at(-1) ?? '',
            format: options?.format ?? 'text',
            content: '',
            created: '',
            writable: true,
            last_modified: '',
            mimetype: ''
          };
        }else{
          toast.success(
            `Folder ${oldName} successfully renamed to ${newName}.`,
            toastifyCustomStyle
          );
          return {
            type: 'directory',
            path: newLocalPath + (newLocalPath.endsWith('/') ? newLocalPath : newLocalPath + '/'),
            name: newName,
            format: null,
            created: new Date().toISOString(),
            writable: true,
            last_modified: new Date().toISOString(),
            mimetype: '',
            content: null
          };
        }
      }else{
        await showDialog({
          title: 'Rename Error',
          body: response.error,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }
    }
  }

  async getDownloadUrl(
    localPath: string,
    options?: Contents.IFetchOptions
  ): Promise<string> {
    const path = GcsService.pathParser(localPath);
    const fileContent = await GcsService.downloadFile({
      path: path.path,
      bucket: path.bucket,
      name: path.name ? path.name : '',
      format: options?.format ?? 'text'
    });

    const fileName = localPath.split('/').pop() ?? "";

    // if mime not available, then taking default binary type
    const mimeType = typeof mime.lookup(fileName) == 'string' ? String(mime.lookup(fileName)) : "application/octet-stream";

    let blobData: BlobPart;
    if (fileName.endsWith('.ipynb')) {
      blobData = JSON.stringify(fileContent, null, 2); // Serialize the object to a JSON string (with indentation for readability, optional)
  } else {
      blobData = fileContent as BlobPart;
  }

    const blob = new Blob([blobData], { type: mimeType });
    const url = URL.createObjectURL(blob);

    // Create a temporary anchor element to trigger the download
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName; // Set the desired download filename
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a); // Clean up the temporary element
    URL.revokeObjectURL(url);
    
    return Promise.reject('Download initiated successfully through alternative approach.');
  }

  copy(localPath: string, toLocalDir: string): Promise<Contents.IModel> {
    throw new Error('Method not implemented.');
  }

  // Checkpoint APIs, not currently supported.
  async createCheckpoint(
    localPath: string
  ): Promise<Contents.ICheckpointModel> {
    return {
      id: '',
      last_modified: ''
    };
  }

  async listCheckpoints(
    localPath: string
  ): Promise<Contents.ICheckpointModel[]> {
    return [];
  }

  async restoreCheckpoint(
    localPath: string,
    checkpointID: string
  ): Promise<void> {}

  async deleteCheckpoint(
    localPath: string,
    checkpointID: string
  ): Promise<void> {}
}