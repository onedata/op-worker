import Ember from 'ember';

export default Ember.Component.extend({
  serverService: Ember.inject.service('server'),
  userName: null,
  manageProvidersURL: null,

  initSessionDetails: function() {
    this.get('serverService').callServer('userName', (value) => this.set('userName', value));
    this.get('serverService').callServer('manageProvidersURL', (value) => this.set('manageProvidersURL', value));
  }.on('init')
});
