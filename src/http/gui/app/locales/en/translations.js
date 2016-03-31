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
      cancel: 'Cancel',
      yes: 'Yes',
      no: 'No',
      fetchingToken: 'Fetching token...'
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
      create: 'Create',
      join: 'Join',
      drop: {
        setHome: 'Set as home',
        moveUp: 'Move up',
        moveDown: 'Move down',
        leave: 'Leave space',
        rename: 'Rename',
        remove: 'Remove',
        inviteGroup: 'Invite group',
        inviteUser: 'Invite user',
        getSupport: 'Get support'
      },
      createModal: {
        title: 'Create a new space',
        enterName: 'Enter new space name:'
      },
      joinModal: {
        title: 'Join a space',
        label: 'Enter a token of a space to join:'
      },
      renameModal: {
        title: 'Rename a space',
        label: 'Enter new space name:'
      },
      leaveModal: {
        title: 'Leave a space',
        label: 'Are you sure you want to leave space "{{spaceName}}"?'
      },
      removeModal: {
        title: 'Remove a space',
        label: 'Are you sure you want to remove a space "{{spaceName}}"?'
      },
      notify: {
        spaceRemoved: 'Space "{{spaceName}}" has been removed',
        spaceRemoveFailed: 'Failed to remove space "{{spaceName}}"',
        joinedToSpace: 'Successfully joined to space "{{spaceName}}"'
      }
    },
    spacesSubmenu: {
      users: 'users',
      groups: 'groups',
      providers: 'providers'
    },
    permissionsTable: {
      inviteButton: 'Invite {{type}}',
      viewSpace: 'view space',
      modifySpace: 'modify space',
      setPrivileges: 'set privileges',
      removeSpace: 'remove space',
      inviteUser: 'invite user',
      removeUser: 'remove user',
      inviteGroup: 'invite group',
      removeGroup: 'remove group',
      inviteProvider: 'invite provider',
      removeProvider: 'remove provider',
      inviteModal: {
        title: 'Invite {{type}} to space',
        label: 'Pass the below token to the {{type}} you want to invite'
      }
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
