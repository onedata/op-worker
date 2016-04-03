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
      close: 'Close',
      fetchingToken: 'Fetching token...',
      fetchingTokenError: 'Fetching token failed!',
    },
    notify: {
      clipboardSuccess: 'The text copied to clipboard',
      clipboardFailue: 'The text cannot be copied to clipboard - please copy it manually'
    },
    featureNotSupportedShort: 'Feature not supported',
    featureNotSupportedLong: 'Sorry, this feature is not supported yet.',
  },
  components: {
    topBar: {
      logout: 'Log out',
      manageProviders: 'Manage account'
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
        label: 'Are you sure you want to remove the "{{spaceName}}" space?'
      },
      notify: {
        leaveSuccess: 'Space "{{spaceName}}" left successfully',
        leaveFailed: 'Cannot leave space "{{spaceName}}" due to an error',
        removeSuccess: 'Space "{{spaceName}}" has been removed',
        removeFailed: 'Failed to remove space "{{spaceName}}"',
        joinSuccess: 'Successfully joined to space "{{spaceName}}"'
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
        remove: 'Remove element',
        chunks: 'Show file distribution'
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
      fileChunksModal: {
        title: 'File distribution',
        text: 'Distribution of file blocks among providers for file',
        providerName: 'Provider',
        dataDitribution: 'File blocks'
      },
    },
    fileUpload: {
      titleUpload: 'Uploading {{count}} file(s)',
      cancelAll: 'Cancel all',
    },
    tokenModals: {
      user: {
        title: 'Invite user to the space',
        label: 'Pass the below token to the user you want to invite'
      },
      group: {
        title: 'Invite group to the space',
        label: 'Pass the below token to the group you want to invite'
      },
      support: {
        title: 'Get support for the space',
        label: 'Pass the below token to the provider you want to request support from'
      }
    },
  },
  groups: {
    title: 'Groups'
  },
  collection: {
    title: 'Collection'
  },
  trash: {
    title: 'Trash'
  },
  recent: {
    title: 'Recent'
  },
  links: {
    title: 'Links'
  },
  spaces: {
    title: 'Spaces',
    show: {
      title: 'Space settings',
      users: {
        title: 'Users permissions',
        tableTitle: 'users'
      },
      groups: {
        title: 'Groups permissions',
        tableTitle: 'groups'
      },
    }
  },
  data: {
    title: 'Data',
    rootDirectory: 'Root directory',
    dataSpace: {
      title: 'Space "{{spaceName}}"',
      nullDir: 'This space is not supported by any providers or cannot be synchronized',
      dir: {
        title: 'File browser',
        file: {
          title: 'File details'
        }
      }
    }
  }
};
