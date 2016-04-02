import Ember from 'ember';

// TODO: jsdoc
export default Ember.Component.extend({
  store: Ember.inject.service('store'),
  fileSystemTree: Ember.inject.service('file-system-tree'),
  errorNotifier: Ember.inject.service('error-notifier'),
  fileBrowser: Ember.inject.service('file-browser'),
  notify: Ember.inject.service('notify'),
  fileUpload: Ember.inject.service('file-upload'),

  // TODO: doc
  dir: null,

  // TODO: enable sorting in GUI
  filesSorting: ['type:asc', 'name:asc'],
  filesSorted: Ember.computed.sort('dir.children', 'filesSorting'),

  bindFileUpload() {
     this.get('fileUpload').assignDrop(this.$().find('.table'));
    //this.get('fileUpload').assignDrop($('#content-scroll'));
  },

  didInsertElement() {
    this.resumableJsChange();
  },

  resumable: function() {
    return this.get('fileUpload.fileUploadComponent.resumable');
  }.property('fileUpload', 'fileUploadComponent', 'fileUpload.fileUploadComponent.resumable'),

  resumableJsChange: function() {
    let resumable = this.get('resumable');
    console.debug(`Browser resumable changed dir id: ${this.get('fileUpload.fileUploadComponent.dir.id')}`);
    if (resumable) {
      this.bindFileUpload();
    }
  }.observes('resumable'),

  actions: {
    openFile(file) {
      if (file.get('isDir')) {
        this.sendAction('openDirInBrowser', file.get('id'));
      } else {
        window.open(`/download/${file.get('id')}`, '_blank');
      }
    },

    // TODO: show modal
    createFile(type) {
      this.get('dir').createFile(type, this.get('createFileName'));
    },

    // TODO: multiple select only with ctrl
    selectFile(file) {
      file.set('isSelected', !file.get('isSelected'));
    },

    // TODO: renameFileName will be probably in modal
    renameSelectedFile() {
      let file = this.get('dir.singleSelectedFile');
      if (file) {
        file.set('name', this.get('renameFileName') || '');
        file.save();
      } else {
        console.error('No file selected to rename or multiple selected');
      }
    },

    // TODO: error handling
    removeSelectedFiles() {
      this.get('dir').removeSelectedFiles();
    }
  }

});
