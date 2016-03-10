import Ember from 'ember';

// TODO: jsdoc
export default Ember.Service.extend({
  store: Ember.inject.service('store'),

  rootSpaces: {},

  updateDataSpaces(dataSpaces) {
    let rootSpaces = {};
    dataSpaces.forEach((s) => {
      rootSpaces[s.get('rootDir.id')] = s.get('id');
    });
    this.set('rootSpaces', rootSpaces);
  },

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
  }
});
