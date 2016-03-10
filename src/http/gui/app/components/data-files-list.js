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
      return `"${file.get('name')}" (${file.get('type')}) <- space "${fileSystemTree.getSpaceIdForFile(file)}"`;
    });
  }.property('dir.children')
});
