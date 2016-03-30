import Ember from 'ember';

export default Ember.Component.extend({
  nextLevel: function() {
    return this.get('level') + 1;
  }.property('level'),

  levelClass: function() {
    let level = this.get('level');
    return level ? `level-${level}` : '';
  }.property('level'),

  subdirsSorting: ['name:asc'],
  subdirs: Ember.computed.filterBy('rootDir.children', 'isDir', true),
  subdirsSorted: Ember.computed.sort('subdirs', 'subdirsSorting'),

  actions: {
    toggleDir(dirFile) {
      dirFile.set('isExpanded', !dirFile.get('isExpanded'));
    },

    browseDir(dirFile) {
      if (dirFile.get('isDir')) {
        this.sendAction('openDirInBrowser', dirFile.get('id'));
      } else {
        console.error(`Tried to browse a file in file brower (should be dir): ${dirFile.get('id')}`);
      }
    },

    /** Pass the action up */
    openDirInBrowser(fileId) {
      this.sendAction('openDirInBrowser', fileId);
    }
  }
});
