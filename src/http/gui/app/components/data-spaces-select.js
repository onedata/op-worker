import Ember from 'ember';

// TODO: doc
export default Ember.Component.extend({
  classNames: ['data-spaces-select'],

  /** List of DataSpace records */
  spaces: null,

  /** Space currently selected */
  selectedSpace: null,

  prevSelectedSpace: null,

  spacesChanged: function() {
    console.debug(`Spaces changed: len ${this.get('spaces.length')}, prev: ${this.get('prevSelectedSpace')}`);
    if (!this.get('prevSelectedSpace') && this.get('spaces.length') > 0) {
      let defaultSpace = this.get('spaces').find((s) => s.get('isDefault'));
      console.debug('spaces: ' + this.get('spaces').map((s) => s.get('isDefault')));
      if (defaultSpace) {
        console.debug(`Will set new selectedSpace: ${defaultSpace.get('name')}`);
      } else {
        console.debug('DataSpacesSelect: no selectedSpace!');
      }

      this.set('prevSelectedSpace', this.get('selectedSpace'));
      this.set('selectedSpace', defaultSpace);
    }
  }.observes('spaces', 'spaces.length', 'spaces.@each.isDefault'),

  selectedSpaceDidChange: function() {
    if (this.get('selectedSpace')) {
      this.sendAction('goToDataSpace', this.get('selectedSpace.id'));
    }
  }.observes('selectedSpace'),

  didInsertElement() {
    console.warn('did insert spaces');
    this.spacesChanged();
  },

  actions: {
    setSelectedSpace(space) {
      this.set('prevSelectedSpace', this.get('selectedSpace'));
      this.set('selectedSpace', space);
    }
  }
});
