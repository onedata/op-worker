import Ember from 'ember';

export default Ember.Component.extend({
  serverService: Ember.inject.service('server'),
  userName: null,

  initUsername: function() {
    this.get('serverService').callServer('userName', (value) => this.set('userName', value));
  }.on('init')
});
