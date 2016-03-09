import Ember from 'ember';

/**
 * Load model for space - to be able to browse it's root dir.
 *
 * @module routes/data
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default Ember.Route.extend({
  // TODO: enable this
  // model(params) {
  //   return this.store.peekRecord('dataSpace', params.data_space_id);
  // },

  model(params) {
    // TODO: use dataSpaceId to fetch root files for DataSpace
    // TODO: use modelFor('data')
    let dataSpaceId = params.data_space_id;
    // TODO: real store
    let rootDir = Ember.Object.create({
      id: 0,
      name: `${params.dir_id}-root`,
      type: 'dir',
      isDir: true,
      parent: null,
      children: [
        Ember.Object.create({
          id: 1,
          name: 'lol',
          type: 'file',
          parent: null,
          children: null,
        }),
        Ember.Object.create({
          id: 2,
          name: 'lol2',
          type: 'file',
          parent: 0,
          children: null,
        }),
        Ember.Object.create({
          id: 3,
          name: 'lol',
          type: 'dir',
          isDir: true,
          parent: 0,
          children: [
            Ember.Object.create({
              id: 4,
              name: 'rotfl1',
              type: 'dir',
              isDir: true,
              parent: 3,
              children: null,
            }),
            Ember.Object.create({
              id: 5,
              name: 'lolcat',
              type: 'file',
              parent: 3,
              children: null,
            }),
          ],
        }),
        Ember.Object.create({
          id: 6,
          name: 'octocat',
          type: 'file',
          parent: 0,
          children: null,
        }),
      ],
    });

    return {
      id: dataSpaceId,
      name: `data-space-${dataSpaceId}`,
      rootDir: rootDir
    };
  },

  renderTemplate() {
    this.render({outlet: 'data-space'});
  },

  actions: {
    showDirInBrowser(fileId) {
      this.transitionTo('data.data-space.dir', fileId);
    }
  }
});
