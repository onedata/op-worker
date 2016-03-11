import Ember from 'ember';

// TODO: jsdoc
export default Ember.Component.extend({
  fileSystemTree: Ember.inject.service('file-system-tree'),

  // TODO: doc
  dir: null,

  filesInfo: function() {
    return this.get('dir.children').map((file) => {
      let fileSystemTree = this.get('fileSystemTree');
      // TODO: get space id will be removed - only for space resolve demonstration
      return {
        file: file,
        label: `id: "${file.get('id')}" "${file.get('name')}" (${file.get('type')}) <- space "${fileSystemTree.getSpaceIdForFile(file)}"`
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
    }
  }
});
