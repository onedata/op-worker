import Ember from 'ember';
// import fileToTreeNode from '../utils/file-to-tree-node';

export default Ember.Component.extend({
  dataFilesTreeService: Ember.inject.service('dataFilesTree'),

  /**
    Reference to File - root of the filesystem showed in tree.
    Note, that only chilren of this File will be showed in tree (root will be hidden).
  */
  rootDir: null,

  /*** Bind with main-menu service, TODO: mixin or something? ***/
  SERVICE_API: ['setRootDir'],

  /** Listen on mainMenuService's events */
  listen: function() {
    let dataFilesTreeService = this.get('dataFilesTreeService');
    this.SERVICE_API.forEach(name => dataFilesTreeService.on(name, this, name));
  }.on('init'),

  /** Deregister event listener from main-menu service */
  cleanup: function() {
    let dataFilesTreeService = this.get('dataFilesTreeService');
    this.SERVICE_API.forEach(name => dataFilesTreeService.off(name, this, name));
  }.on('willDestroyElement'),

  // treeData: function() {
  //   let rootDir = this.get('rootDir');
  //   return rootDir ? fileToTreeNode(rootDir) : [];
  //   // TODO: root dir, not its childs - probably to delete
  //   // return rootDir.get('children').map((child) => fileToTreeNode(child));
  // }.property('rootDir', 'rootDir.children', 'rootDir.children.@each.name'),

  // /** Use treeData to update treeview of Bootstrap tree */
  // updateTree: function() {
  //   let tree = this.$().find('#tree');
  //   tree.treeview({
  //     data: this.get('treeData'),
  //     enableLinks: true,
  //   });
  //   tree.on('nodeExpanded', (event, node) => {
  //     console.debug(`Node expanded: ${node} ${event}`);
  //   });
  //   tree.on('nodeSelected', (event, node) => {
  //     console.debug(`File node selected: ${node.fileId} ${event}`);
  //     this.sendAction('onDirSelected', node.fileId);
  //   });
  // }.observes('treeData'),

  // didInsertElement() {
  //   this.updateTree();
  // },

  /*** Service API ***/
  setRootDir(rootDir) {
    this.set('rootDir', rootDir);
  },
});
