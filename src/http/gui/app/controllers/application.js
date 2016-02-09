import Ember from 'ember';

export default Ember.Controller.extend({
  serverService: Ember.inject.service('server'),

  mains: [
    {name: 'data', icon: 'data'},
    {name: 'links', icon: 'links'},
    {name: 'recent', icon: 'recent'},
    {name: 'collection', icon: 'collection'},
    {name: 'trash', icon: 'trash'},
    {name: 'spaces', icon: 'spaces'},
    // TODO: bad names of icons
    {name: 'groups', icon: 'group2'},
    {name: 'token', icon: 'group'},
  ],

  activate() {
    this.initializeValue('userName');
  },

  /**
   * Initialize a value by sending a callback to the server. Value can be
   * e.g. user name which has to be checked once and then is cached.
   */
  initializeValue: function (key) {
    // TODO VFS-1508: probably use of (returnedValue) => {...}
    var controller = this;
    this.serverService.callServer(key, function (returnedValue) {
      controller.set(key, returnedValue);
    });
  },

});
