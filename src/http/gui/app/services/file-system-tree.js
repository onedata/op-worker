import Ember from 'ember';

// TODO: jsdoc
export default Ember.Service.extend({
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
  }
});
