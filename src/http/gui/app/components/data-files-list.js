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

  bindFileUpload() {
    var r = this.get('fileUpload.resumable');
    // Resumable.js isn't supported, fall back on a different method
    if (!r.support) {
      // TODO: other message
      window.alert('resumable js is not supported!');
      $('.resumable-error').show();
    } else {
      let container = this.$().find('.table');

      r.assignDrop(container[0]);

      // Dragging
      // TODO: styles
      container.on('dragenter', function() {
        $(this).addClass('file-drag');
      });
      container.on('dragend', function() {
        $(this).removeClass('file-drag');
      });
      container.on('drop', function() {
        $(this).removeClass('file-drag');
      });

      // Resumable events

      r.on('fileAdded', (file) => {
        this.get('notify').info('Starting file upload: ' + file.fileName);
        r.upload();
      });

      // TODO: handle pause

      r.on('complete', () => {
        this.get('notify').info('File upload completed');
      });

      r.on('fileSuccess', (file/*, message*/) => {
        this.get('notify').info(`File "${file.fileName}" uploaded successfully!`);
      });

      r.on('fileError', (file, message) => {
        this.get('notify').error(`File "${file.fileName}" upload failed: ${message}`);
      });

      // TODO: handle file progress

      r.on('uploadStart', () => {
        this.get('notify').info('Starting file upload');
      });
    }
  },

  didInsertElement() {
    this.bindFileUpload();
  },

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
