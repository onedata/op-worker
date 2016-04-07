import Ember from 'ember';
// import fileToTreeNode from '../utils/file-to-tree-node';

export default Ember.Component.extend({
  fileSystemTreeService: Ember.inject.service('fileSystemTree'),

  classNames: ['data-files-tree'],

  /**
    Reference to File - root of the filesystem showed in tree.
    Note, that only chilren of this File will be showed in tree (root will be hidden).
  */
  rootDir: null,

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['setRootDir'],

  /** Listen on mainMenuService's events */
  listen: function() {
    let fileSystemTreeService = this.get('fileSystemTreeService');
    this.SERVICE_API.forEach(name => fileSystemTreeService.on(name, this, name));
  }.on('init'),

  /** Deregister event listener from main-menu service */
  cleanup: function() {
    let fileSystemTreeService = this.get('fileSystemTreeService');
    this.SERVICE_API.forEach(name => fileSystemTreeService.off(name, this, name));
  }.on('willDestroyElement'),

  /*** Service API ***/

  actions: {
    openDirInBrowser(fileId) {
      this.sendAction('openDirInBrowser', fileId);
    }
  }
});
