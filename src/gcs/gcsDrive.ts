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

import 'react-toastify/dist/ReactToastify.css';
import { showDialog, Dialog, Spinner } from '@jupyterlab/apputils';
import mime from 'mime-types';

import { JupyterFrontEnd } from '@jupyterlab/application';
import { GcsBrowserWidget } from './gcsBrowserWidget';
import {
  DELETE_SIGNAL,
  DIRECTORY,
  FILE,
  GCS_PLUGIN_TITLE,
  NOTEBOOK,
  RENAME_SIGNAL,
  UNTITLED_DIRECTORY_NAME,
  UNTITLED_FILE_EXT,
  UNTITLED_FILE_NAME,
  UNTITLED_NOTEBOOK_EXT,
  UNTITLED_NOTEBOOK_NAME
} from '../utils/const';
import {
  COPY_ERROR_TITLE,
  BUCKET_LEVEL_FILE_CREATION_MESSAGE,
  BUCKET_LEVEL_FOLDER_CREATION_MESSAGE,
  BUCKET_LEVEL_NOTEBOOK_CREATION_MESSAGE,
  BUCKET_RENAME_ERROR,
  DELETION_ERROR_TITLE,
  FILE_CREATION_ERROR_TITLE,
  FOLDER_CREATION_ERROR_TITLE,
  INVALID_FILE_NAME_ERROR,
  NAME_EXCEEDS_MAX_LENGTH_ERROR,
  NO_DATA_PROVIDED_ERROR,
  NOTEBOOK_CREATION_ERROR_TITLE,
  NOTEBOOK_CREATION_GCS_CONTEXT_MESSAGE,
  OBJECT_CREATION_AT_ROOT_ERROR_MESSAGE,
  PASTE_BUCKET_ERROR_MESSAGE,
  PASTE_BUCKET_TITLE,
  RENAME_ERROR_TITLE,
  UNSUPPORTED_CREATE_ERROR,
  UNSUPPORTED_CREATE_TITLE,
  COPY_FILE_TO_SAME_LOCATION_ERROR
} from '../utils/message';

// Template for an empty Directory IModel.
const DIRECTORY_IMODEL: Contents.IModel = {
  type: DIRECTORY,
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
  // Instance members moved to the beginning
  readonly serverSettings: ServerConnection.ISettings;
  private _app: JupyterFrontEnd;
  private _browserWidget: GcsBrowserWidget | null = null;
  private _isDisposed = false;
  private _fileChanged = new Signal<this, Contents.IChangedArgs>(this);
  private _saveSpinner: Spinner | null = null;
  selected_panel: string | null = null;

  constructor(app: JupyterFrontEnd) {
    // Not actually used, but the Contents.IDrive interface requires one.
    this.serverSettings = ServerConnection.makeSettings();
    this._app = app;
  }

  public setBrowserWidget(widget: GcsBrowserWidget): void {
    this._browserWidget = widget;
  }

  get fileChanged(): ISignal<this, Contents.IChangedArgs> {
    return this._fileChanged;
  }

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
    if (this._saveSpinner) {
      this._saveSpinner.dispose();
      this._saveSpinner = null;
    }
    this._browserWidget = null;
  }

  private showSaveSpinner(): void {
    const activeWidget = this._app.shell.currentWidget;

    if (!activeWidget) {
      console.warn('No active widget found to show save spinner.');
      return;
    }

    if (!this._saveSpinner) {
      this._saveSpinner = new Spinner();
      this._saveSpinner.addClass('gcs-save-spinner-overlay');
    }

    this._saveSpinner.node.style.backgroundColor = 'transparent';

    activeWidget.node.appendChild(this._saveSpinner.node);
    this._saveSpinner.show();

    activeWidget.node.style.opacity = '0.5';
    activeWidget.node.style.pointerEvents = 'none';
  }

  private hideSaveSpinner(): void {
    if (this._saveSpinner) {
      this._saveSpinner.hide();

      if (this._saveSpinner.node.parentElement) {
        this._saveSpinner.node.parentElement.removeChild(
          this._saveSpinner.node
        );
      }
      this._saveSpinner.dispose();
      this._saveSpinner = null;

      const activeWidget = this._app.shell.currentWidget;
      if (activeWidget) {
        activeWidget.node.style.opacity = '';
        activeWidget.node.style.pointerEvents = '';
      }
    }
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

    const request: Contents.IFetchOptions = options || {};

    if (request.type === FILE || request.type === NOTEBOOK) {
      return await this.getFile(localPath, options);
    } else {
      return await this.getDirectory(localPath);
    }
  }

  /**
   * @returns IModel directory containing all the GCS buckets for the current project.
   */
  private async getBuckets() {
    const content = await GcsService.listBuckets();

    if (!content) {
      throw new Error(`Error Listing Buckets ${content}`);
    }
    return {
      ...DIRECTORY_IMODEL,
      content:
        content.map((bucket: { items: { name: string; updated: string } }) => ({
          ...DIRECTORY_IMODEL,
          path: bucket.items.name,
          name: bucket.items.name,
          last_modified: bucket.items.updated ?? new Date().toISOString()
        })) ?? []
    };
  }

  /**
   * @returns IModel directory for the given local path.
   */
  private async getDirectory(localPath: string) {
    const path = GcsService.pathParser(localPath);
    const prefix = path.path.length > 0 ? `${path.path}/` : path.path;
    const content = await GcsService.listFiles({
      prefix: prefix,
      bucket: path.bucket
    });
    if (!content) {
      throw 'Error Listing Objects';
    }
    let directory_contents: Contents.IModel[] = [];

    if (content.prefixes && content.prefixes.length > 0) {
      directory_contents = directory_contents.concat(
        content.prefixes.map((item: { prefixes: { name: string } }) => {
          const pref = item.prefixes.name;
          const path = pref.split('/');
          const name = path.at(-2) ?? prefix;
          return {
            ...DIRECTORY_IMODEL,
            path: `${localPath}/${name}`,
            name: name,
            created: new Date().toISOString(),
            last_modified: new Date().toISOString()
          };
        })
      );
    }

    if (content.files && content.files.length > 0) {
      directory_contents = directory_contents.concat(
        content.files.map(
          (item: {
            items: {
              name: string;
              updated: string;
              size: number;
              content_type: string;
              timeCreated: string;
            };
          }) => {
            const itemName = item.items.name!;
            const pathParts = itemName.split('/');
            const name = pathParts.at(-1) ?? itemName;
            return {
              type: FILE,
              path: `${localPath}/${name}`,
              name: name,
              format: 'base64',
              content: null,
              created: item.items.timeCreated ?? new Date().toISOString(),
              writable: true,
              last_modified: item.items.updated ?? new Date().toISOString(),
              mimetype: item.items.content_type ?? new Date().toISOString(),
              size: item.items.size
            };
          }
        )
      );
    }

    return {
      ...DIRECTORY_IMODEL,
      path: localPath,
      name: localPath.split('\\').at(-1) ?? '',
      content: directory_contents
    };
  }

  /**
   * @returns IModel file for the given local path.
   */
  private async getFile(
    localPath: string,
    options?: Contents.IFetchOptions
  ): Promise<Contents.IModel> {
    let fileContent: string | any;
    const format = options?.format ?? 'text';
    const path = GcsService.pathParser(localPath);
    const content = await GcsService.loadFile({
      path: path.path,
      bucket: path.bucket,
      format: format
    });
    if (content === null || typeof content === 'undefined') {
      throw 'File Loading Error';
    }

    if (format === 'text') {
      if (typeof content === 'object') {
        try {
          fileContent = JSON.stringify(content, null, 2);
        } catch (e) {
          console.error('Failed to stringify content as JSON:', e);
          fileContent = String(content);
        }
      } else {
        fileContent = String(content);
      }
    } else {
      fileContent = content;
    }
    
    return {
      type: FILE,
      path: localPath,
      name: localPath.split('\\').at(-1) ?? '',
      format: options?.format ?? 'text',
      content: fileContent,
      created: new Date().toISOString(),
      writable: true,
      last_modified: new Date().toISOString(),
      mimetype: ''
    };
  }

  async newUntitled(
    options?: Contents.ICreateOptions
  ): Promise<Contents.IModel> {
    if (this.selected_panel !== GCS_PLUGIN_TITLE) {
      return Promise.reject(new Error(NOTEBOOK_CREATION_GCS_CONTEXT_MESSAGE));
    }
    if (!options) {
      console.error(NO_DATA_PROVIDED_ERROR, options);
      return Promise.reject(new Error(NO_DATA_PROVIDED_ERROR));
    } else if (!options.path) {
      if (options.type === DIRECTORY) {
        await showDialog({
          title: FOLDER_CREATION_ERROR_TITLE,
          body: BUCKET_LEVEL_FOLDER_CREATION_MESSAGE,
          buttons: [Dialog.okButton()]
        });
        return Promise.reject();
      } else if (options.type === FILE) {
        await showDialog({
          title: FILE_CREATION_ERROR_TITLE,
          body: BUCKET_LEVEL_FILE_CREATION_MESSAGE,
          buttons: [Dialog.okButton()]
        });
        return Promise.reject();
      } else if (options.type === NOTEBOOK) {
        return Promise.reject(
          new Error(BUCKET_LEVEL_NOTEBOOK_CREATION_MESSAGE)
        );
      } else {
        await showDialog({
          title: UNSUPPORTED_CREATE_TITLE,
          body: UNSUPPORTED_CREATE_ERROR + options.type,
          buttons: [Dialog.okButton()]
        });
        return Promise.reject();
      }
    }

    const localPath = typeof options?.path === 'string' ? options?.path : '';

    if (localPath === '/' || localPath === '') {
      console.error(OBJECT_CREATION_AT_ROOT_ERROR_MESSAGE);
      return Promise.reject(new Error(OBJECT_CREATION_AT_ROOT_ERROR_MESSAGE));
    }

    const parsedPath = GcsService.pathParser(localPath);

    try {
      this._browserWidget?.showProgressBar();
      switch (options.type) {
        case DIRECTORY:
          return await this.createNewDirectory(localPath, parsedPath);
        case FILE:
          return await this.createNewFile(localPath, parsedPath);
        case NOTEBOOK:
          return await this.createNewNotebook(localPath, parsedPath);
        default:
          console.warn(`Unsupported creation type: ${options.type}`);
          await showDialog({
            title: UNSUPPORTED_CREATE_TITLE,
            body: UNSUPPORTED_CREATE_ERROR + options.type,
            buttons: [Dialog.okButton()]
          });
          return DIRECTORY_IMODEL;
      }
    } finally {
      this._browserWidget?.hideProgressBar();
    }
  }

  private async createNewDirectory(
    localPath: string,
    parsedPath: { bucket: string; path: string; name: string | undefined }
  ): Promise<Contents.IModel> {
    const content = await GcsService.listFiles({
      prefix:
        parsedPath.path === ''
          ? parsedPath.path + UNTITLED_DIRECTORY_NAME
          : parsedPath.path + '/' + UNTITLED_DIRECTORY_NAME,
      bucket: parsedPath.bucket
    });

    if (content.prefixes) {
      let maxSuffix = 1;
      content.prefixes.forEach(
        (data: { prefixes: { name: string; updatedAt: string } }) => {
          const parts = data.prefixes.name.split('/');
          if (parts.length >= 2) {
            const potentialSuffix = parts[parts.length - 2];
            const suffixElement = potentialSuffix.match(/\d+$/);
            if (
              suffixElement !== null &&
              parseInt(suffixElement[0]) >= maxSuffix
            ) {
              maxSuffix = parseInt(suffixElement[0]) + 1;
            }
          }
          untitledFolderSuffix = maxSuffix.toString();
        }
      );
    } else {
      untitledFolderSuffix = '';
    }
    const folderName = UNTITLED_DIRECTORY_NAME + untitledFolderSuffix;

    const response = await GcsService.createFolder({
      bucket: parsedPath.bucket,
      path: parsedPath.path,
      folderName: folderName
    });

    if (response) {
      const result = {
        type: DIRECTORY,
        path:
          localPath + (localPath.endsWith('/') ? folderName : '/' + folderName),
        name: folderName,
        format: null,
        created: new Date().toISOString(),
        writable: true,
        last_modified: new Date().toISOString(),
        mimetype: '',
        content: null
      };
      return result;
    } else {
      console.error('Failed to create folder.');
      await showDialog({
        title: FOLDER_CREATION_ERROR_TITLE,
        body: `Folder ${folderName} creation is failed.`,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }
  }

  private async createNewFile(
    localPath: string,
    parsedPath: { bucket: string; path: string; name: string | undefined }
  ): Promise<Contents.IModel> {
    const content = await GcsService.listFiles({
      prefix:
        parsedPath.path === ''
          ? parsedPath.path + UNTITLED_FILE_NAME
          : parsedPath.path + '/' + UNTITLED_FILE_NAME,
      bucket: parsedPath.bucket
    });

    let maxSuffix = 1;
    const baseFileName = UNTITLED_FILE_NAME;
    const fileExtension = UNTITLED_FILE_EXT;

    if (content.files) {
      content.files.forEach((file: { items: { name: string } }) => {
        const nameParts = file.items.name.split('/');
        const fileName = nameParts.at(-1) ?? '';
        const baseNameMatch = fileName.match(/^untitled(\d*)(\..*)?$/);
        if (baseNameMatch) {
          const suffix = baseNameMatch[1];
          const ext = baseNameMatch[2] || UNTITLED_FILE_EXT;
          if (ext === fileExtension && suffix) {
            const num = parseInt(suffix);
            if (!isNaN(num) && num >= maxSuffix) {
              maxSuffix = num + 1;
            }
          } else if (
            ext === fileExtension &&
            maxSuffix === 1 &&
            fileName === `${UNTITLED_FILE_NAME}${UNTITLED_FILE_EXT}`
          ) {
            maxSuffix = 2;
          }
        }
      });
    }

    const newFileName =
      maxSuffix > 1
        ? `${baseFileName}${maxSuffix}${fileExtension}`
        : `${baseFileName}${fileExtension}`;

    const filePathInGCS =
      parsedPath.path === ''
        ? newFileName
        : `${parsedPath.path}/${newFileName}`;

    const response = await GcsService.saveFile({
      bucket: parsedPath.bucket,
      path: filePathInGCS,
      contents: ''
    });

    if (response) {
      const parts = newFileName.split('.');
      const ext = parts.length > 1 ? `.${parts.slice(1).join('.')}` : '';
      const mimetype = ext === '.json' ? 'application/json' : 'text/plain';

      return {
        type: FILE,
        path: `${localPath}/${newFileName}`,
        name: newFileName,
        format: 'text',
        content: '',
        created: new Date().toISOString(),
        writable: true,
        last_modified: new Date().toISOString(),
        mimetype: mimetype
      };
    } else {
      console.error('Failed to create file.');
      await showDialog({
        title: FILE_CREATION_ERROR_TITLE,
        body: `File ${newFileName} creation is failed.`,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }
  }

  private async createNewNotebook(
    localPath: string,
    parsedPath: { bucket: string; path: string; name: string | undefined }
  ): Promise<Contents.IModel> {
    const notebookExtension = UNTITLED_NOTEBOOK_EXT;
    const baseNotebookName = UNTITLED_NOTEBOOK_NAME;

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
          } else if (
            ext === notebookExtension &&
            maxSuffix === 1 &&
            fileName === `${UNTITLED_NOTEBOOK_NAME}${UNTITLED_NOTEBOOK_EXT}`
          ) {
            maxSuffix = 2;
          }
        }
      });
    }

    const newNotebookName =
      maxSuffix > 1
        ? `${baseNotebookName}${maxSuffix}${notebookExtension}`
        : `${baseNotebookName}${notebookExtension}`;
    const filePathInGCS =
      parsedPath.path === ''
        ? newNotebookName
        : `${parsedPath.path}/${newNotebookName}`;

    const response = await GcsService.saveFile({
      bucket: parsedPath.bucket,
      path: filePathInGCS,
      contents: JSON.stringify({
        cells: [],
        metadata: {
          kernelspec: {
            display_name: 'Python 3',
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
        type: NOTEBOOK,
        path: `${localPath}/${newNotebookName}`,
        name: newNotebookName,
        format: 'json',
        content: null,
        created: new Date().toISOString(),
        writable: true,
        last_modified: new Date().toISOString(),
        mimetype: 'application/x-ipynb+json'
      };
    } else {
      console.error('Failed to create notebook.');
      await showDialog({
        title: NOTEBOOK_CREATION_ERROR_TITLE,
        body: `Notebook ${newNotebookName} creation failed.`,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }
  }

  async save(
    localPath: string,
    options?: Partial<Contents.IModel>
  ): Promise<Contents.IModel> {
    try {
      this.showSaveSpinner();
      const path = GcsService.pathParser(localPath);
      const content =
        options?.format === 'json'
          ? JSON.stringify(options.content)
          : options?.content;
      const resp = await GcsService.saveFile({
        bucket: path.bucket,
        path: path.path,
        contents: content
      });

      return {
        type: FILE,
        path: localPath,
        name: localPath.split('\\').at(-1) ?? '',
        format: 'text',
        created: new Date().toISOString(),
        content: '',
        writable: true,
        last_modified:
          (resp as { updated?: string }).updated ?? new Date().toISOString(),
        mimetype: '',
        ...options
      };
    } finally {
      this.hideSaveSpinner();
    }
  }

  async delete(path: string): Promise<void> {
    try {
      const localPath = GcsService.pathParser(path);

      this._browserWidget?.showProgressBar();

      const response = await GcsService.deleteFile({
        bucket: localPath.bucket,
        path: localPath.path
      });

      if (response.status === 200 || response.status === 204) {
        this._fileChanged.emit({
          type: DELETE_SIGNAL,
          oldValue: { path },
          newValue: null
        });
      } else {
        await showDialog({
          title: DELETION_ERROR_TITLE,
          body: response.error,
          buttons: [Dialog.okButton()]
        });
      }
    } finally {
      await this._browserWidget?.refreshContents();
      this._browserWidget?.hideProgressBar();
    }
  }

  private async _preventRootLevelPaste(): Promise<Contents.IModel> {
    await showDialog({
      title: PASTE_BUCKET_TITLE,
      body: PASTE_BUCKET_ERROR_MESSAGE,
      buttons: [Dialog.okButton()]
    });
    return DIRECTORY_IMODEL;
  }

  async rename(
    path: string,
    newLocalPath: string,
    options?: Contents.IFetchOptions
  ): Promise<Contents.IModel> {
    const oldPath = GcsService.pathParser(path);
    const newPath = GcsService.pathParser(newLocalPath);

    if (newPath.path === '') {
      // In the rename operation, it is not possible to get empty path from jupyter.
      // Only while user performs cut/paste operation, it is possible to get empty path.
      return this._preventRootLevelPaste();
    }

    const oldName = path.split('/').pop() ?? '';
    const isOldPathMeetsFilename =
      oldName.includes('.') && oldName.lastIndexOf('.') > 0;

    const newName = newLocalPath.split('/').pop() ?? '';
    const isNewPathMeetsFilename =
      newName.includes('.') && newName.lastIndexOf('.') > 0;

    if (
      newLocalPath.split('/')[newLocalPath.split('/').length - 1].length >= 1024
    ) {
      await showDialog({
        title: RENAME_ERROR_TITLE,
        body: NAME_EXCEEDS_MAX_LENGTH_ERROR,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    }
    if (!isOldPathMeetsFilename && oldPath.path === '') {
      await showDialog({
        title: RENAME_ERROR_TITLE,
        body: BUCKET_RENAME_ERROR,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    } else if (isOldPathMeetsFilename && !isNewPathMeetsFilename) {
      // Old path has file name and New file name given dont have extension
      await showDialog({
        title: RENAME_ERROR_TITLE,
        body: INVALID_FILE_NAME_ERROR,
        buttons: [Dialog.okButton()]
      });
      return DIRECTORY_IMODEL;
    } else {
      try {
        this._browserWidget?.showProgressBar();

        if (
          oldPath.path.includes(UNTITLED_DIRECTORY_NAME + untitledFolderSuffix)
        ) {
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

        if (response?.status === 200) {
          await GcsService.deleteFile({
            bucket: oldPath.bucket,
            path: oldPath.path
          });

          if (isOldPathMeetsFilename) {
            // Emitting the Signal ( If file is opened, JupyterLab updates name in the editor. )
            this._fileChanged.emit({
              type: RENAME_SIGNAL,
              // Creating Model Obj for Both Source and Destination (renamed)
              oldValue: this.ModelObject(path, isOldPathMeetsFilename),
              newValue: this.ModelObject(newLocalPath, isNewPathMeetsFilename)
            });

            return {
              type: FILE,
              path: newLocalPath,
              name: newLocalPath.split('\\').at(-1) ?? '',
              format: options?.format ?? 'text',
              content: '',
              created: new Date().toISOString(),
              writable: true,
              last_modified: new Date().toISOString(),
              mimetype: ''
            };
          } else {
            return {
              type: DIRECTORY,
              path:
                newLocalPath +
                (newLocalPath.endsWith('/')
                  ? newLocalPath
                  : newLocalPath + '/'),
              name: newName,
              format: null,
              created: new Date().toISOString(),
              writable: true,
              last_modified: new Date().toISOString(),
              mimetype: '',
              content: null
            };
          }
        } else {
          await showDialog({
            title: RENAME_ERROR_TITLE,
            body: response?.error,
            buttons: [Dialog.okButton()]
          });
          return DIRECTORY_IMODEL;
        }
      } finally {
        await this._browserWidget?.refreshContents();
        this._browserWidget?.hideProgressBar();
      }
    }
  }

  ModelObject(path: string, isPathMeetsFileName: boolean): Contents.IModel {
    const now = new Date().toISOString();
    const name = path.split('/').at(-1) || '';
    return {
      name: name,
      path: path,
      type: isPathMeetsFileName ? FILE : DIRECTORY,
      writable: true,
      created: now,
      last_modified: now,
      content: null,
      format: isPathMeetsFileName ? 'text' : null,
      mimetype: ''
    };
  }

  async getDownloadUrl(
    localPath: string,
    options?: Contents.IFetchOptions
  ): Promise<string> {
    const path = GcsService.pathParser(localPath);
    this._browserWidget?.showProgressBar();
    const fileContent = await GcsService.downloadFile({
      path: path.path,
      bucket: path.bucket,
      name: path.name ? path.name : '',
      format: options?.format ?? 'text'
    });

    const fileName = localPath.split('/').pop() ?? '';

    // if mime not available, then taking default binary type
    const mimeType =
      typeof mime.lookup(fileName) === 'string'
        ? String(mime.lookup(fileName))
        : 'application/octet-stream';

    let blobData: BlobPart;
    if (fileName.endsWith('.ipynb')) {
      blobData = JSON.stringify(fileContent, null, 2);
    } else {
      blobData = fileContent as BlobPart;
    }

    const blob = new Blob([blobData], { type: mimeType });
    const url = URL.createObjectURL(blob);

    // Create a temporary anchor element to trigger the download
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    this._browserWidget?.hideProgressBar();

    return Promise.reject(
      'Download initiated successfully through alternative approach.'
    );
  }

  async copy(localPath: string, toLocalDir: string): Promise<Contents.IModel> {
    this._browserWidget?.showProgressBar();
    try {
      if (toLocalDir === '') {
        // empty path means user is trying to paste at bucket level.
        return this._preventRootLevelPaste();
      }

      const parsedSource = GcsService.pathParser(localPath);
      const parsedDestinationDir = GcsService.pathParser(toLocalDir);

      const sourceName = localPath.split('/').pop() ?? '';

      const expectedDestinationPath = `${toLocalDir}/${sourceName}`;
      if (localPath === expectedDestinationPath) {
        await showDialog({
          title: COPY_ERROR_TITLE,
          body: COPY_FILE_TO_SAME_LOCATION_ERROR,
          buttons: [Dialog.okButton()]
        });
        return DIRECTORY_IMODEL;
      }

      let newFullPathInDestination: string;
      let copiedModel: Contents.IModel;

      let sourceGcsPath = parsedSource.path;

      newFullPathInDestination = `${parsedDestinationDir.bucket}/${parsedDestinationDir.path}/${sourceName}`;

      const response = await GcsService.copyFile({
        sourceBucket: parsedSource.bucket,
        sourcePath: sourceGcsPath,
        destinationBucket: parsedDestinationDir.bucket,
        destinationPath: `${parsedDestinationDir.path}/${sourceName}`
      });

      // Construct the IModel for the newly copied item
      copiedModel = this.ModelObject(
        newFullPathInDestination,
        response.isFolder
      );

      this._fileChanged.emit({
        type: 'new', // Indicate that a new item has been created
        oldValue: null,
        newValue: copiedModel
      });

      return copiedModel;
    } catch (error: any) {
      await showDialog({
        title: COPY_ERROR_TITLE,
        body: `${error.message || 'An unknown error occurred.'}`,
        buttons: [Dialog.okButton()]
      });
      // Return a default value to satisfy the return type
      return DIRECTORY_IMODEL;
    } finally {
      // Refresh the browser widget contents to show the newly copied item
      await this._browserWidget?.refreshContents();
      this._browserWidget?.hideProgressBar();
    }
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
