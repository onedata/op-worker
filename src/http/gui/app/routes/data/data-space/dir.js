import Ember from 'ember';

// TODO jsdoc
export default Ember.Route.extend({
  dataFilesTree: Ember.inject.service('dataFilesTree'),

  model(params) {
    // TODO: check if loaded dir belongs to loaded space (data/data-space model)?
    return this.store.findRecord('file', params.dir_id);
  },

  /**
    This is a directory browers. It can show only one directory contents at a time.
    Render it in "data" template, because it's a master view of a data browser.
  */
  renderTemplate() {
    this.render({
      into: 'data',
      outlet: 'dir'
    });
  }
});
