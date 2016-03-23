import Ember from 'ember';

// TODO: doc
export default Ember.Component.extend({
  classNames: ['data-spaces-select'],

  /** List of DataSpace records */
  spaces: null,

  /** Space currently selected */
  selectedSpace: null,

  // selectedSpace: function() {
  //   return this.get('spaces').find((s) => s.id === this.get('selectedSpaceId'));
  // }.property('selectedSpaceId'),

  prevSelectedSpace: null,

  spacesChanged: function() {
    console.warn(`Spaces changed: ${this.get('spaces.length')}, prev: ${this.get('prevSelectedSpace')}`);
    if (!this.get('prevSelectedSpace') && this.get('spaces.length') > 0) {
      let defaultSpace = this.get('spaces').find((s) => s.get('isDefault'));
      this.set('selectedSpace', defaultSpace);
    }
  }.observes('spaces', 'spaces.length', 'spaces.@each.isDefault'),

  selectedSpaceDidChange: function() {
    if (this.get('selectedSpace')) {
      this.sendAction('goToDataSpace', this.get('selectedSpace.id'));
    }
  }.observes('selectedSpace'),

  didInsertElement() {
    // let selectInstance = new Select({
    //   el: document.querySelector('select'),
    //   className: 'select-theme-onedata'
    // });

    console.warn('did insert spaces');
    this.spacesChanged();
  },

  actions: {
    setSelectedSpace(space) {
      this.set('prevSelectedSpace', this.get('selectedSpace'));
      this.set('selectedSpace', space);
    }
  }

  // something: function() {
  //   let a = this.get('spaces').toArray()[0].get('rootDir').get('children').toArray();
  //   return a.length > 0 && a[0].get('name');
  // }.property('spaces.@each.rootDir.@each.children.@each.name')
});
