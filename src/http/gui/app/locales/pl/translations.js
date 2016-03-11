/**
 * Polish translation of GUI strings.
 * Translations dictionary is organized as in routes dir.
 *
 * @module locales/pl/translations
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/
export default {
  components: {
    // layout
    topBar: {
      logout: 'Wyloguj',
      manageProviders: 'Zarządzaj dostawcami'
    },
    mainMenu: {
      data: 'dane',
      links: 'linki',
      recent: 'ostatnie',
      collection: 'kolekcja',
      trash: 'kosz',
      spaces: 'miejsca',
      groups: 'grupy',
      token: 'token'
    },
    // spaces
    spacesMenu: {
      title: 'miejsca',
    },
    spacesSubmenu: {
      users: 'użytkownicy',
      groups: 'grupy',
      providers: 'dostawcy'
    },
    permissionsTable: {
      viewSpace: 'przegląd miejsca',
      modifySpace: 'zmiana miejsca',
      removeSpace: 'usuwanie miejsca',
      inviteUser: 'zapraszanie użytkowników',
      removeUser: 'usuwanie użytkowników',
      inviteGroup: 'zapraszanie grup',
      removeGroup: 'usuwanie grup',
      inviteProvider: 'zapraszanie dostawców',
      removeProvider: 'usuwanie dostawców'
    },
    // data
    dataFilesTree: {
      rootDirectory: 'Katalog główny'
    }
  },
  spaces: {
    show: {
      users: {
        tableTitle: 'użytkownicy'
      },
      groups: {
        tableTitle: 'grupy'
      }
    }
  },
};
