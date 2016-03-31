import Ember from 'ember';

export default Ember.Component.extend({
  commonModals: Ember.inject.service(),

  /**
    Before opening modal, additional params may be required
    which can be used in specific modals
  */
  modalParams: {},

  registerInService: function() {
    this.set('commonModals.component', this);
  }.on('init'),

});
