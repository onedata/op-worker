import Ember from 'ember';

// TODO jsdoc
export default Ember.Route.extend({
  fileSystemTree: Ember.inject.service('file-system-tree'),
  dataFilesTree: Ember.inject.service('dataFilesTree'),

  model(params) {
    // TODO: check if loaded dir belongs to loaded space (data/data-space model)?
    return this.store.findRecord('file', params.dir_id);
  },

  afterModel(file, transition) {
    console.log('dir afterModel');
    if (!file.get('isDir')) {
      console.error('Loaded file is not a directory - it cannot be viewed in browser');
      transition.abort();
    }

    let loadedDirSpaceId = this.get('fileSystemTree').getSpaceIdForFile(file);
    if (loadedDirSpaceId !== this.modelFor('data.data-space').get('id')) {
      console.error('Space of loaded dir (file) is not a space loaded in data-space route');
      transition.abort();
    }

    Ember.run.scheduleOnce('afterRender', this, function() {
      this.get('fileSystemTree').expandDir(file).then(() => {
        let elementId = `#tree-dir-${file.id}`;
        $('.dir-item.active').removeClass('active');
        $(elementId).addClass('active');
      });
    });
  },

  /**
    This is a directory browser. It can show only one directory contents at a time.
    Render it in "data" template, because it's a master view of a data browser.
  */
  renderTemplate() {
    this.render({
      into: 'data',
      outlet: 'dir'
    });
  }
});
