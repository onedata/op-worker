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
      token: 'token'
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
    dataFilesListToolbar: {
      renameFileModal: {
        title: 'Rename file or directory',
        enterName: 'Rename the item "{{currentName}}" to:'
      }
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
