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
  common: {
    modal: {
      ok: 'OK',
      cancel: 'Anuluj'
    }
  },
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
      token: 'tokeny',
      providers: 'dostawcy'
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
    },
    dataFilesList: {
      files: 'pliki',
      size: 'rozmiar',
      modification: 'modyfikacja',
      permissions: 'uprawnienia'
    },
    dataFilesListToolbar: {
      tooltip: {
        createDir: 'Utwórz katalog',
        createFile: 'Utwórz plik',
        shareFile: 'Udostępnij element',
        uploadFile: 'Załaduj plik',
        rename: 'Zmień nazwę elementu',
        permissions: 'Zmień uprawnienia elementu',
        copy: 'Kopiuj element',
        cut: 'Wytnij element',
        remove: 'Usuń element'
      },
      renameFileModal: {
        title: 'Zmiana nazwy pliku lub katalogu',
        enterName: 'Zmień nazwę elementu "{{currentName}}" na:'
      },
      createDirModal: {
        title: 'Nowy katalog',
        enterName: 'Wprowadź nazwę nowego katalogu:'
      },
      createFileModal: {
        title: 'Nowy plik',
        enterName: 'Wprowadź nazwę nowego pliku:'
      },
      removeFilesModal: {
        title: 'Usuwanie plików',
        text: 'Czy chcesz usunąć wybrane elementy?'
      },
      editPermissionsModal: {
        title: 'Zmiana uprawnień',
        text: 'Wprowadź nowy kod uprawnień:'
      },
    },
    fileUpload: {
      titleUpload: 'Wysyłanie {{count}} pliku',
      cancelAll: 'Anuluj wszystkie',
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
