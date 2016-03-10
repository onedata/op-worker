/**
 * Provides communication with Server.
 * @module services/server
 * @author Lukasz Opiola
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

// This file should be linked to app/services/server.js

import Ember from 'ember';

export default Ember.Service.extend({
  server: Ember.inject.service('server')
});
