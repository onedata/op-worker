/**
 * Creates some hard-coded records with random relations in store.
 *
 * Works onlu on models:
 * - space
 * - spaceUser
 * - spaceGroup
 * - spaceUserPermission
 * - spaceGroupPermission
 *
 * Firstly, removes records (all of given models) then creates new.
 * Other models are leaved untouched.
 *
 * @module initializers/fixtures
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

// TODO causes problems, currently always invokes thenCallback
let ifNoSpacesInStore = function (store, thenCallback) {
  console.warn('Will generate test spaces data!');
  thenCallback();
  // TODO: a code for invoking thenCallback only if there are no records
  // store.findAll('space').then((spaces) => {
  //   let spacesCount = spaces.get('length');
  //   if (spacesCount <= 0) {
  //     console.debug('No Spaces in store - will generate some');
  //     return thenCallback && thenCallback();
  //   } else {
  //     console.debug(`There are ${spacesCount} spaces already in store`);
  //   }
  // });
};

function clearStore(store, successCallback) {
  store.findAll('space').then((spaces) => {
    let destroyPromises = spaces.toArray().map((s) => s.destroyRecord());

    Ember.RSVP.all(destroyPromises).then(() => {
      return (successCallback && successCallback());
    });
  });
}

export function initialize(container /*, application */) {
  // let store = container.lookup('service:store');
  //
  // ifNoSpacesInStore(store, () => {
  //   clearStore(store, () => {
  //     let spaces = ['My files', 'Experiments', 'Publication results'];
  //     spaces = spaces.map((name, i) => store.createRecord('space', {id: i+1, name: name}));
  //     spaces.forEach(s => s.save());
  //     let users = [
  //       'Łukasz Opioła', 'Ewa Salawa', 'Michał Orzechowski',
  //       'Jakub Liput', 'Michał Wrzeszcz', 'Bartosz Kryza',
  //       'Darin Nikolow', 'Jakub Kudzia', 'Krzysztof Trzepla',
  //       'Konrad Zemek', 'Łukasz Dutka', 'Mateusz Paciorek',
  //       'Michał Wrona', 'Michał Żmuda', 'Rafał Słota', 'Tomasz Lichoń'
  //     ];
  //     users = users.map((name, i) => store.createRecord('spaceUser', {id: i+1, name: name}));
  //     users.forEach((u) => u.save());
  //
  //     let groups = [
  //       'Users', 'Developers', 'Sciencists'
  //     ];
  //     groups = groups.map((name, i) => store.createRecord('spaceGroup', {id: i+1, name: name}));
  //     groups.forEach((g) => g.save());
  //
  //     // for each user, create permission record
  //     let userPerms = users.map((u) => {
  //       return store.createRecord('spaceUserPermission', {
  //         permViewSpace: true,
  //         permModifySpace: true,
  //         permRemoveSpace: true,
  //         permInviteUser: true,
  //         permRemoveUser: true,
  //         permInviteProvider: true,
  //         permRemoveProvider: true,
  //         user: u
  //       });
  //     });
  //
  //     let groupPerms = groups.map((g) => {
  //       return store.createRecord('spaceGroupPermission', {
  //         permViewSpace: true,
  //         permModifySpace: true,
  //         permRemoveSpace: true,
  //         permInviteUser: true,
  //         permRemoveUser: true,
  //         permInviteProvider: true,
  //         permRemoveProvider: true,
  //         group: g
  //       });
  //     });
  //
  //     let perms = userPerms.concat(groupPerms);
  //     // distribute permissions randomly on spaces
  //     perms.forEach((p) => {
  //       p.set('space', spaces[Math.trunc(Math.random()*spaces.length)]);
  //       p.save();
  //     });
  //   });
  // });
}

export default {
  name: 'fixtures',
  after: 'store',
  initialize
};
