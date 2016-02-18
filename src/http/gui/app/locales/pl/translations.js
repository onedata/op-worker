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
    topBar: {
      logout: 'Wyloguj'
    },
    mainMenu: {
      data: 'dane',
      links: 'odnośniki',
      recent: 'najnowsze',
      collection: 'kolekcja',
      trash: 'kosz',
      spaces: 'przestrzenie',
      groups: 'groupy',
      token: 'token'
    },
    spacesMenu: {
      title: 'miejsca',
    },
    spacesSubmenu: {
      users: 'użytkownicy',
      groups: 'grupy',
      providers: 'dostawcy'
    },
    permissionsTable: {
      viewSpace: 'przeglądanie przestrzeni',
      modifySpace: 'zmiana przestrzeni',
      removeSpace: 'usuwanie przestrzeni',
      inviteUser: 'zapraszanie użytkowników',
      removeUser: 'usuwanie użytkowników',
      inviteGroup: 'zapraszanie grup',
      removeGroup: 'usuwanie grup',
      inviteProvider: 'zapraszanie dostawców',
      removeProvider: 'usuwanie dostawców',
    }
  },
  spaces: {
    show: {
      users: {
        tableTitle: 'użytkownicy'
      },
      groups: {
        tableTitle: 'grupy'
      },
    }
  }
};
