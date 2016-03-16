import Ember from 'ember';

// TODO: jsdoc
export default Ember.Component.extend({
  store: Ember.inject.service('store'),
  fileSystemTree: Ember.inject.service('file-system-tree'),
  errorNotifier: Ember.inject.service('error-notifier'),
  fileBrowser: Ember.inject.service('file-browser'),

  // TODO: doc
  dir: null,
  
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
