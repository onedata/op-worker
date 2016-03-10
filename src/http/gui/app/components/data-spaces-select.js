import Ember from 'ember';

// TODO: doc
export default Ember.Component.extend({
  /** List of DataSpace records */
  spaces: null,

  /** Space currently selected */
  selectedSpaceId: null,

  selectedSpace: function() {
    return this.get('spaces').find((s) => s.id === this.get('selectedSpaceId'));
  }.property('selectedSpaceId'),

  selectedSpaceDidChange: function() {
    this.sendAction('goToDataSpace', this.get('selectedSpaceId'));
  }.observes('selectedSpaceId'),

  // something: function() {
  //   let a = this.get('spaces').toArray()[0].get('rootDir').get('children').toArray();
  //   return a.length > 0 && a[0].get('name');
  // }.property('spaces.@each.rootDir.@each.children.@each.name')
});
