import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { GCSDrive } from './gcs/gcsDrive';
import { Panel } from '@lumino/widgets';
import { DataprocLoggingService, LOG_LEVEL } from './utils/loggingService';
import { GcsBrowserWidget } from './gcs/gcsBrowserWidget';
import { IDocumentManager } from '@jupyterlab/docmanager';
import { IFileBrowserFactory } from '@jupyterlab/filebrowser';
import { IThemeManager } from '@jupyterlab/apputils';
import { LabIcon } from '@jupyterlab/ui-components';
import storageIcon from '../style/icons/storage_icon.svg';
import storageIconDark from '../style/icons/Storage-icon-dark.svg';
/**
 * Initialization data for the gcs-jupyter-plugin extension.
 */

const iconStorage = new LabIcon({
  name: 'launcher:storage-icon',
  svgstr: storageIcon
});
const iconStorageDark = new LabIcon({
  name: 'launcher:storage-icon-dark',
  svgstr: storageIconDark
});
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'gcs-jupyter-plugin:plugin',
  description: 'A JupyterLab extension.',
  autoStart: true,
  optional: [    
    IFileBrowserFactory,
    IThemeManager,
    IDocumentManager],
  activate: (
    app: JupyterFrontEnd,
    factory: IFileBrowserFactory,
    themeManager: IThemeManager,
    documentManager: IDocumentManager
  ) => {

    console.log('JupyterLab extension gcs-jupyter-plugin is activated!');

    const onThemeChanged = () => {
      const isLightTheme = themeManager.theme
        ? themeManager.isLight(themeManager.theme)
        : true;
      if (isLightTheme) {
        if ( panelGcs) {
          panelGcs.title.icon = iconStorage;
        }
      } else {

        if ( panelGcs) {
          panelGcs.title.icon = iconStorageDark;
        }
      }
    };
    // themeManager.themeChanged.connect(onThemeChanged);
  

    let panelGcs: Panel | undefined;
    let gcsDrive: GCSDrive | undefined;
    panelGcs?.dispose();
    gcsDrive?.dispose();
    panelGcs = undefined;
    gcsDrive = undefined;
    panelGcs = new Panel();
    panelGcs.id = 'GCS-bucket-tab';
    panelGcs.title.caption = 'Google Cloud Storage';
    panelGcs.title.className = 'panel-icons-custom-style';
    gcsDrive = new GCSDrive();
    documentManager.services.contents.addDrive(gcsDrive);
    panelGcs.addWidget(
      new GcsBrowserWidget(gcsDrive, factory as IFileBrowserFactory)
    );
    onThemeChanged();
    app.shell.add(panelGcs, 'left', { rank: 1002 });
    DataprocLoggingService.log(
      'Cloud storage is enabled',
      LOG_LEVEL.INFO
    );
  }
};

export default plugin;
