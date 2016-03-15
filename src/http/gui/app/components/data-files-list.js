import Ember from 'ember';

// TODO: jsdoc
export default Ember.Component.extend({
  store: Ember.inject.service('store'),
  fileSystemTree: Ember.inject.service('file-system-tree'),
  errorNotifier: Ember.inject.service('error-notifier'),

  // TODO: doc
  dir: null,

  selectedFiles: function() {
    return this.get('dir.children').filter((file) => file.get('isSelected'));
  }.property('dir.children.@each.isSelected'),

  // TODO: only for single-selected file
  singleSelectedFile: function() {
    let selected = this.get('selectedFiles');
    return selected.length === 1 ? selected[0] : null;
  }.property('selectedFiles'),


  // selectedFileChanged: function() {
  //   let selectedFile = this.get('selectedFile');
  //   this.$().find('.file-entry.selected-file').removeClass('selected-file');
  //   this.$().find('#file-' + selectedFile.get('id')).addClass('selected-file');
  // }.observes('selectedFile'),

  filesInfo: function() {
    return this.get('dir.children').map((file) => {
      let fileSystemTree = this.get('fileSystemTree');
      // TODO: get space id will be removed - only for space resolve demonstration
      return {
        file: file,
        label: `id: "${file.get('id')}" "${file.get('name')}" (${file.get('type')}) <- space "${fileSystemTree.getSpaceIdForFile(file)}", `,
        path: fileSystemTree.dirsPath(file).map((i) => `${i.get('name')}`).join('/')
      };
    });
  }.property('dir.children.@each.name', 'dir.children.@each.type'),

  actions: {
    openFile(dirFile) {
      if (dirFile.get('isDir')) {
        this.sendAction('openDirInBrowser', dirFile.get('id'));
      } else {
        console.error(`Clicked file is no a dir, so nothing will happen: ${dirFile.get('id')}`);
      }
    },

    createFile(type) {
      let fileName = this.get('createFileName');
      let record = this.get('store').createRecord('file', {
        name: fileName,
        parent: this.get('dir'),
        type: type
      });
      record.save().then(() => {}, (failMessage) => {
        this.get('errorNotifier').handle(failMessage);
      });
    },

    // TODO: multiple select only with ctrl
    selectFile(file) {
      file.set('isSelected', !file.get('isSelected'));
    },

    renameSelectedFile() {
      let file = this.get('singleSelectedFile');
      if (file) {
        file.set('name', this.get('renameFileName') || '');
        file.save();
      } else {
        console.error('No file selected to rename or multiple selected');
      }
    },

    // TODO: error handling
    removeSelectedFiles() {
      this.get('selectedFiles').forEach((file) => {
        file.destroyRecursive();
      });
    }
  },

});
