import Ember from 'ember';

export default Ember.Route.extend({
  dataFilesTree: Ember.inject.service('dataFilesTree'),

  model(params) {
    // TODO: use params.dir_id to get
    // params.dir_id
    let rootDir = Ember.Object.create({
      id: 0,
      name: `${params.dir_id}-root`,
      type: 'dir',
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
          type: 'dir',
          parent: 0,
          children: null,
        }),
        Ember.Object.create({
          id: 3,
          name: 'lol',
          type: 'dir',
          parent: 0,
          children: [
            Ember.Object.create({
              id: 4,
              name: 'rotfl1',
              type: 'dir',
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
          type: 'dir',
          parent: 0,
          children: null,
        }),
      ],
    });

    // this.get('dataFilesTree').trigger('setRootDir', rootDir);
    return rootDir;
  },

  renderTemplate() {
    this.render({
      into: 'data',
      outlet: 'dir'
    });
  }
});
