import Ember from 'ember';

export default Ember.Controller.extend({
  // Static values that are available globally
  userName: null,

  actions: {
    createNewFile: function(name, type, parentID) {
      var file = this.get('store').createRecord('file', {
        name: name,
        type: type,
        parentId: parentID
      });
      file.save();
    }
  },

  // Initialize a value by sending a callback to the server. Value can be
  // e.g. user name which has to be checked once and then is cached
  initializeValue: function (key) {
    var controller = this;
    this.callServer(key, function (returnedValue) {
      controller.set(key, returnedValue);
    });
  },

  // Sends a callback to the server. thenFun is evaluated on response from
  // the server.
  callServer: function (key, thenFun) {
    this.get('store').adapterFor('application').callback('global', key).then(thenFun);
  },

  // Controller init, called automatically by Ember.
  init: function () {
    this.initializeValue('userName');
  }
});
