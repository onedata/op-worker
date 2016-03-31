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

  joinSpace() {
    return this.get('server').privateRPC('joinSpace', {});
  },

  leaveSpace(space) {
    return this.get('server').privateRPC('leaveSpace', {
      spaceId: space.get('id')
    });
  },

  inviteGroup(space) {
    return this.get('server').privateRPC('inviteGroup', {
      spaceId: space.get('id')
    });
  },

  inviteUser(space) {
    return this.get('server').privateRPC('inviteUser', {
      spaceId: space.get('id')
    });
  },

  getSupport(space) {
    return this.get('server').privateRPC('getSupport', {
      spaceId: space.get('id')
    });
  },
});
