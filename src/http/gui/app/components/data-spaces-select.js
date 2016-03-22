import Ember from 'ember';
/* globals Select */

// TODO: doc
export default Ember.Component.extend({
  classNames: ['data-spaces-select'],

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

  didInsertElement() {
    let selectInstance = new Select({
      el: document.querySelector('select'),
      className: 'select-theme-onedata'
    });
  }

  // something: function() {
  //   let a = this.get('spaces').toArray()[0].get('rootDir').get('children').toArray();
  //   return a.length > 0 && a[0].get('name');
  // }.property('spaces.@each.rootDir.@each.children.@each.name')
});
