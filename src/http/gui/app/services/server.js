/**
 * Provides communication with Server.
 * @module services/server
 * @author Łukasz Opioła
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Service.extend({
  store: Ember.inject.service('store'),

  /**
   * Sends a callback to the server. thenFun is evaluated on response from
   * the server.
   */
  callServer: function (key, thenFun) {
    this.get('store').adapterFor('application').callback('global', key).then(thenFun);
  }
});
