import Ember from 'ember';

export default Ember.Service.extend({
  /**
   * Sends a callback to the server. thenFun is evaluated on response from
   * the server.
   */
  callServer: function (key, thenFun) {
    this.get('store').adapterFor('application').callback('global', key).then(thenFun);
  }
});
