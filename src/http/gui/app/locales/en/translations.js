/**
 * English translation of GUI strings.
 * Translations dictionary is organized as in routes dir.
 *
 * @module locales/en/translations
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/
export default {
  common: {
    modal: {
      ok: 'OK',
      cancel: 'Cancel'
    }
  },
  components: {
    topBar: {
      logout: 'Log out',
      manageProviders: 'Manage providers'
    },
    mainMenu: {
      data: 'data',
      links: 'links',
      recent: 'recent',
      collection: 'collection',
      trash: 'trash',
      spaces: 'spaces',
      groups: 'groups',
      token: 'tokens',
      providers: 'providers'
    },
    spacesMenu: {
      title: 'spaces',
    },
    spacesSubmenu: {
      users: 'users',
      groups: 'groups',
      providers: 'providers'
    },
    permissionsTable: {
      viewSpace: 'view space',
      modifySpace: 'modify space',
      removeSpace: 'remove space',
      inviteUser: 'invite user',
      removeUser: 'remove user',
      inviteGroup: 'invite group',
      removeGroup: 'remove group',
      inviteProvider: 'invite provider',
      removeProvider: 'remove provider',
    },
    // data
    dataFilesTree: {
      rootDirectory: 'Root directory'
    },
    dataFilesList: {
      files: 'files',
      size: 'size',
      modification: 'modification',
      permissions: 'permissions'
    },
    dataFilesListToolbar: {
      tooltip: {
        createDir: 'Create directory',
        createFile: 'Create file',
        shareFile: 'Share element',
        uploadFile: 'Upload file',
        rename: 'Rename element',
        permissions: 'Change element permissions',
        copy: 'Copy element',
        cut: 'Cut element',
        remove: 'Remove element'
      },
      renameFileModal: {
        title: 'Rename file or directory',
        enterName: 'Rename the item "{{currentName}}" to:'
      },
      createDirModal: {
        title: 'New directory',
        enterName: 'Enter new directory name:'
      },
      createFileModal: {
        title: 'New file',
        enterName: 'Enter new file name:'
      },
      removeFilesModal: {
        title: 'Remove files',
        text: 'Do you want to remove selected files?'
      },
      editPermissionsModal: {
        title: 'Edit permissions',
        text: 'Enter new file permissions code:'
      },
    },
    fileUpload: {
      titleUpload: 'Uploading {{count}} file(s)',
      cancelAll: 'Cancel all',
    }
  },
  spaces: {
    show: {
      users: {
        tableTitle: 'users'
      },
      groups: {
        tableTitle: 'groups'
      },
    }
  },
  data: {
    rootDirectory: 'Root directory'
  }
};
