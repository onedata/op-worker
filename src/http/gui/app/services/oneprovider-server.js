/**
 * Provides API for communication with Oneprovider Server.
 * @module services/oneprovider-server
 * @author Lukasz Opiola
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Service.extend({
  server: Ember.inject.service('server'),

  /** TODO: should resolve space name on success */
  joinSpace(token) {
    return this.get('server').privateRPC('joinSpace', {token: token});
  },

  leaveSpace(space) {
    return this.get('server').privateRPC('leaveSpace', {
      spaceId: space.get('id')
    });
  },

  /** Allowed types: user, group, support */
  getToken(type, spaceId) {
    return this.get('server').privateRPC(`${type}Token`, {
      spaceId: spaceId
    });
  }
});
