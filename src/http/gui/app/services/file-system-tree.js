import Ember from 'ember';

// TODO: jsdoc
export default Ember.Service.extend(Ember.Evented, {
  store: Ember.inject.service('store'),

  dataSpaces: null,

  rootSpaces: function() {
    let rootSpaces = {};
    this.get('dataSpaces').forEach((s) => {
      rootSpaces[s.get('rootDir.id')] = s.get('id');
    });
    return rootSpaces;
  }.property('dataSpaces.@each.rootDir.id'),

  getSpaceIdForFile(file) {
    if (file) {
      let parent = file.get('parent');
      if (parent.get('id')) {
        return this.getSpaceIdForFile(file.get('parent'));
      } else {
        return this.get('rootSpaces')[file.get('id')];
      }
    } else {
      return null;
    }
  },

  // TODO: cache of tree
  /**
   * Returns array with file parents, including the file.
   * The array is ordered from root dir to given file (from parents to children).
   *
   * @param file - a leaf file of path to find
   * @returns {Array} array of Files
   */
  dirsPath(file) {
    if (file) {
      let path = [file];
      let parent = file.get('parent');
      while (parent && parent.get('id')) {
        path.unshift(parent);
        parent = parent.get('parent');
      }
      console.debug(`Computed path for file ${file.get('id')}: ${JSON.stringify(path)}`);
      return path;
    } else {
      return [];
    }
  },

  expandDir(file) {
    return new Ember.RSVP.Promise((resolve) => {
      let path = this.dirsPath(file);
      // TODO: this.rootDir should be the same as first element of path
      // TODO: check if dir to expand is child of previous dir?
      // TODO: should last dir in path be expanded?
      let parentsLength = path.length - 1;
      for (let i=0; i<parentsLength; ++i) {
        path[i].set('isExpanded', true);
      }

      resolve();
    });

  }
});
