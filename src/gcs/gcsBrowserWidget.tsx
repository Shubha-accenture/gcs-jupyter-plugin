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

import { Widget, PanelLayout } from '@lumino/widgets';
import {
  Dialog,
  IThemeManager,
  ToolbarButton,
  showDialog
} from '@jupyterlab/apputils';
import { FileBrowser } from '@jupyterlab/filebrowser';
import { GcsService } from './gcsService';
import { GCSDrive } from './gcsDrive';
import { TitleWidget } from '../controls/SidePanelTitleWidget';
import { ProgressBarWidget } from './ProgressBarWidget';
import { authApi, login } from '../utils/utils';
import { Message } from '@lumino/messaging';

import {
  iconFileFilter,
  iconFileFilterDark,
  iconGCSNewFolder,
  iconGCSNewFolderDark,
  iconGCSRefresh,
  iconGCSRefreshDark,
  iconGCSUpload,
  iconGCSUploadDark,
  iconSigninGoogle
} from '../utils/icon';
import {
  GCS_PLUGIN_TITLE,
  NEW_FOLDER,
  FILE_UPLOAD,
  REFRESH,
  TOGGLE_FILE_FILTER
} from '../utils/const';
import {
  BUCKET_LEVEL_FOLDER_CREATION_MESSAGE,
  BUCKET_LEVEL_UPLOAD_MESSAGE,
  FILE_EXIST_TITLE,
  FILE_OVERWRITE_MESSAGE,
  FOLDER_CREATION_ERROR_TITLE,
  GCLOUD_CONFIG_ERROR,
  OVERWRITE_BUTTON_TEXT,
  UPLOAD_ERROR_TITLE
} from '../utils/message';

export class GcsBrowserWidget extends Widget {
  private readonly _themeManager: IThemeManager;
  private readonly fileInput!: HTMLInputElement;
  newFolder!: ToolbarButton;
  private gcsUpload!: ToolbarButton;
  private refreshButton!: ToolbarButton;
  private toggleFileFilter!: ToolbarButton;
  private readonly _progressBarWidget!: ProgressBarWidget;

  private readonly _browser: FileBrowser;

  private readonly _titleWidget!: TitleWidget;
  constructor(
    drive: GCSDrive,
    browser: FileBrowser,
    themeManager: IThemeManager
  ) {
    super();

    this._browser = browser;
    this._themeManager = themeManager;

    this._browser.showLastModifiedColumn = false;
    this._browser.showFileFilter = false;
    this._browser.showHiddenFiles = true;

    // Create an empty panel layout initially
    this.layout = new PanelLayout();
    this.node.style.height = '100%';
    this.node.style.display = 'flex';
    this.node.style.flexDirection = 'column';

    this._browser.node.style.overflowY = 'auto'; // Ensure vertical scrolling is enabled if needed
    this._browser.node.style.flexShrink = '1';
    this._browser.node.style.flexGrow = '1';

    // Title widget for the GCS Browser
    this._titleWidget = new TitleWidget(GCS_PLUGIN_TITLE, false);
    (this.layout as PanelLayout).addWidget(this._titleWidget);

    this._progressBarWidget = new ProgressBarWidget();
    (this.layout as PanelLayout).addWidget(this._progressBarWidget);

    // Listen for changes in the FileBrowser's path
    this._browser.model.pathChanged.connect(this.onPathChanged, this);

    const originalCd = this._browser.model.cd;
    this._browser.model.cd = async (path: string) => {
      this.showProgressBar();
      try {
        const result = await originalCd.call(this._browser.model, path);
        return result;
      } finally {
        this.hideProgressBar();
      }
    };

    this._browser.showFileCheckboxes = false;
    (this.layout as PanelLayout).addWidget(this._browser);
    this._browser.node.style.flexShrink = '1';
    this._browser.node.style.flexGrow = '1';

    // Create a file input element
    this.fileInput = document.createElement('input');
    this.fileInput.type = 'file';
    this.fileInput.multiple = true; // Enable multiple file selection
    this.fileInput.style.display = 'none';

    // Attach event listener for file selection
    this.fileInput.addEventListener('change', this.handleFileUpload);

    // Append the file input element to the widget's node
    this.node.appendChild(this.fileInput);

    this.newFolder = this.createNewFolderButton(true);
    this.gcsUpload = this.createUploadButton(true);
    this.refreshButton = this.createRefreshButton(true);
    this.toggleFileFilter = this.createToggleFileFilterButton(true);

    // Since the default location is root. disabling upload and new folder buttons
    this.newFolder.enabled = false;
    this.gcsUpload.enabled = false;

    this._browser.toolbar.addItem(NEW_FOLDER, this.newFolder);
    this._browser.toolbar.addItem(FILE_UPLOAD, this.gcsUpload);
    this._browser.toolbar.addItem(REFRESH, this.refreshButton);
    this._browser.toolbar.addItem(TOGGLE_FILE_FILTER, this.toggleFileFilter);

    this._themeManager.themeChanged.connect(this.onThemeChanged, this);
    this.onThemeChanged();
  }

  protected onAfterAttach(msg: Message): void {
    super.onAfterAttach(msg);
    // Call initialize asynchronously after widget is attached
    void this.initialize();
  }

  private createNewFolderButton(isLight: boolean): ToolbarButton {
    return new ToolbarButton({
      icon: isLight ? iconGCSNewFolder : iconGCSNewFolderDark,
      className: 'icon-white',
      onClick: this.handleFolderCreation,
      tooltip: NEW_FOLDER
    });
  }

  private createUploadButton(isLight: boolean): ToolbarButton {
    return new ToolbarButton({
      icon: isLight ? iconGCSUpload : iconGCSUploadDark,
      className: 'icon-white jp-UploadIcon',
      onClick: this.onUploadButtonClick,
      tooltip: FILE_UPLOAD
    });
  }

  private createRefreshButton(isLight: boolean): ToolbarButton {
    return new ToolbarButton({
      icon: isLight ? iconGCSRefresh : iconGCSRefreshDark,
      className: 'icon-white',
      onClick: () => {
        void this.onRefreshButtonClick();
      },
      tooltip: REFRESH
    });
  }

  private createToggleFileFilterButton(isLight: boolean): ToolbarButton {
    return new ToolbarButton({
      icon: isLight ? iconFileFilter : iconFileFilterDark,
      className: 'icon-white',
      onClick: () => {
        this._browser.showFileFilter = !this._browser.showFileFilter;
      },
      tooltip: TOGGLE_FILE_FILTER
    });
  }

  private readonly onThemeChanged = () => {
    const isLight = this._themeManager.theme
      ? this._themeManager.isLight(this._themeManager.theme)
      : true;

    const newFolderEnabled = this.newFolder.enabled;
    const gcsUploadEnabled = this.gcsUpload.enabled;
    const refreshButtonEnabled = this.refreshButton.enabled;
    const toggleFileFilterEnabled = this.toggleFileFilter.enabled;

    const browserToolbar: any = this._browser.toolbar;

    if (this.newFolder && !this.newFolder.isDisposed) {
      this.newFolder.dispose();
    }
    if (this.gcsUpload && !this.gcsUpload.isDisposed) {
      this.gcsUpload.dispose();
    }
    if (this.refreshButton && !this.refreshButton.isDisposed) {
      this.refreshButton.dispose();
    }
    if (this.toggleFileFilter && !this.toggleFileFilter.isDisposed) {
      this.toggleFileFilter.dispose();
    }

    // Re-create buttons with new icons
    this.newFolder = this.createNewFolderButton(isLight);
    this.gcsUpload = this.createUploadButton(isLight);
    this.refreshButton = this.createRefreshButton(isLight);
    this.toggleFileFilter = this.createToggleFileFilterButton(isLight);

    // Restore the enabled state
    this.newFolder.enabled = newFolderEnabled;
    this.gcsUpload.enabled = gcsUploadEnabled;
    this.refreshButton.enabled = refreshButtonEnabled;
    this.toggleFileFilter.enabled = toggleFileFilterEnabled;

    // Add the new buttons back to the toolbar using their original IDs
    if (typeof browserToolbar.addItem === 'function') {
      browserToolbar.addItem(NEW_FOLDER, this.newFolder);
      browserToolbar.addItem(FILE_UPLOAD, this.gcsUpload);
      browserToolbar.addItem(REFRESH, this.refreshButton);
      browserToolbar.addItem(TOGGLE_FILE_FILTER, this.toggleFileFilter);
    } else {
      console.error(
        'Toolbar addItem method not found at runtime. Cannot re-add buttons.'
      );
    }
  };

  // Function to trigger file input dialog when the upload button is clicked
  private readonly onUploadButtonClick = () => {
    if (this._browser.model.path.split(':')[1] !== '') {
      this.fileInput.click();
    } else {
      showDialog({
        title: UPLOAD_ERROR_TITLE,
        body: BUCKET_LEVEL_UPLOAD_MESSAGE,
        buttons: [Dialog.okButton()]
      });
    }
  };

  private readonly handleFolderCreation = () => {
    if (this._browser.model.path.split(':')[1] !== '') {
      this._browser.createNewDirectory();
    } else {
      showDialog({
        title: FOLDER_CREATION_ERROR_TITLE,
        body: BUCKET_LEVEL_FOLDER_CREATION_MESSAGE,
        buttons: [Dialog.okButton()]
      });
    }
  };

  // Function to handle file upload
  private readonly handleFileUpload = async (event: Event) => {
    const input = event.target as HTMLInputElement;
    const files = Array.from(input.files || []);

    // Clear the input element's value to force the 'change' event on subsequent selections
    input.value = '';

    if (files && files.length > 0) {
      files.forEach((fileData: any) => {
        const file = fileData;
        const reader = new FileReader();

        this.showProgressBar(); // Show spinner for file upload
        reader.onloadend = async () => {
          // Upload the file content to Google Cloud Storage
          const gcsPath = this._browser.model.path.split(':')[1];
          const path = GcsService.pathParser(gcsPath);
          let filePath;

          if (path.path === '') {
            filePath = file.name;
          } else {
            filePath = path.path + '/' + file.name;
          }

          const content = await GcsService.listFiles({
            prefix: filePath,
            bucket: path.bucket
          });

          if (content.files && content.files.length > 0) {
            const result = await showDialog({
              title: FILE_EXIST_TITLE,
              body: file.name + FILE_OVERWRITE_MESSAGE,
              buttons: [
                Dialog.cancelButton(),
                Dialog.okButton({ label: OVERWRITE_BUTTON_TEXT })
              ]
            });

            if (result.button.accept) {
              await GcsService.saveFile({
                bucket: path.bucket,
                path: filePath,
                contents: reader.result as string, // assuming contents is a string
                upload: false
              });
            }
          } else {
            await GcsService.saveFile({
              bucket: path.bucket,
              path: filePath,
              contents: reader.result as string, // assuming contents is a string
              upload: true
            });
          }

          await this._browser.model.refresh();
        };

        reader.readAsDataURL(file); // Read as Data URL for binary files
        
        this.hideProgressBar();
      });
    }
  };

  private readonly onRefreshButtonClick = async () => {
    this.showProgressBar(); // Show spinner for explicit refresh
    try {
      await this._browser.model.refresh();
    } finally {
      this.hideProgressBar(); // Hide after refresh completes
    }
  };

  private async initialize(): Promise<void> {
    try {
      const credentials = await authApi();
      if (credentials?.login_error || credentials?.config_error) {
        this._browser.hide();

        if (credentials) {
          if (credentials.config_error === 1) {
            // Config error
            const errorMessageNode =
              this.createErrorContainer(GCLOUD_CONFIG_ERROR);
            this.node.appendChild(errorMessageNode);
            return;
          }

          if (credentials.login_error === 1) {
            // Login error
            const loginContainer = this.createErrorContainer();
            const loginText = this._createLoginErrorTextElement();
            const loginButton = this._createLoginButton();

            loginButton.onclick = () => {
              // Assuming `login` is globally available
              login((value: boolean | ((prevState: boolean) => boolean)) => {
                if (typeof value === 'boolean' && !value) {
                  // Retry initialization after successful login
                  this.initialize();
                }
              });
            };

            loginButton.appendChild(this._createGoogleIconContainer());
            loginContainer.appendChild(loginText);
            loginContainer.appendChild(loginButton);
            this.node.appendChild(loginContainer);
            return;
          }
        }
      }
    } catch (error) {
      console.error('Error during initialization:', error);
    }
  }

  private createErrorContainer(text?: string): HTMLDivElement {
    const container = document.createElement('div');
    container.className = 'gcs-error-message'; // Can keep this class or make it more generic
    container.style.textAlign = 'center';
    container.style.marginTop = '20px';
    container.style.alignItems = 'center';
    container.style.justifyContent = 'center';
    container.style.display = 'flex';
    container.style.flexDirection = 'column';
    container.style.fontSize = '15px';
    container.style.fontWeight = '600';
    container.style.padding = '11px';

    if (text) {
      container.textContent = text;
    }

    return container;
  }

  private _createLoginErrorTextElement(): HTMLDivElement {
    const loginText = document.createElement('div');
    loginText.className = 'login-error';
    loginText.textContent = 'Please login to continue';
    return loginText;
  }

  private _createGoogleIconContainer(): HTMLDivElement {
    const googleIconContainer = document.createElement('div');
    googleIconContainer.style.marginTop = '20px';
    googleIconContainer.innerHTML = iconSigninGoogle.svgstr;
    return googleIconContainer;
  }

  private _createLoginButton(): HTMLDivElement {
    const loginButton = document.createElement('div');
    loginButton.className = 'signin-google-icon logo-alignment-style';
    loginButton.setAttribute('role', 'button');
    loginButton.style.cursor = 'pointer';
    return loginButton;
  }

  public async refreshContents() {
    await this._browser.model.refresh();
  }

  public showProgressBar(): void {
    if (this._progressBarWidget) {
      this._progressBarWidget.show();
    }
  }

  public hideProgressBar(): void {
    if (this._progressBarWidget) {
      this._progressBarWidget.hide();
    }
  }

  dispose() {
    this._browser.model.pathChanged.disconnect(this.onPathChanged, this);
    this._browser.dispose();
    this.fileInput.removeEventListener('change', this.handleFileUpload);
    this._themeManager.themeChanged.disconnect(this.onThemeChanged, this);
    this.hideProgressBar();
    super.dispose();
  }

  private readonly onPathChanged = () => {
    // The below 2 lines of code is added as a workaround for resetting the file filter
    if (this._browser.showFileFilter) {
      this._browser.showFileFilter = false;
      this._browser.showFileFilter = true;
    }

    const currentPath = this._browser.model.path.split(':')[1];
    // Check if the current path is the root (empty string or just '/')
    const isRootPath = currentPath === '' || currentPath === '/';

    // Freeze upload button if path is root
    if (this.gcsUpload) {
      this.gcsUpload.enabled = !isRootPath;
    }

    // Freeze new folder button if path is root
    if (this.newFolder) {
      this.newFolder.enabled = !isRootPath;
    }
  };

  public get browser(): FileBrowser {
    return this._browser;
  }
}
