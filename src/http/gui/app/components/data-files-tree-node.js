import Ember from 'ember';

export default Ember.Component.extend({
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
